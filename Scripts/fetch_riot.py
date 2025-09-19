# add this tiny safety so scripts can import siblings
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__)))

from imports import (
    os, sys, io, time, json, gzip, random, asyncio,
    Path, quote, datetime, timezone,
    requests, aiohttp, boto3, load_dotenv,
    ClientError, NoCredentialsError
)
from utils import (
    API_KEY, AWS_REGION, S3_BUCKET, DDB_TABLE, HEADERS,
    s3, ddb,
    s3_exists, put_json_gz,
    PLATFORM_BY_REGION, regional_from_platform,
    riot_get_sync, get_puuid_from_riot_id, list_match_ids_year,
    fetch_match_detail, fetch_timeline
)

# =======================
# CONFIG
# =======================
# Tune this based on how aggressive you want to be (8–12 is usually safe).
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))

# If True, we’ll also skip network fetch when a local JSON cache exists
USE_LOCAL_CACHE = True

# =======================
# HELPERS
# =======================
def gzip_bytes(obj: dict) -> bytes:
    b = io.BytesIO()
    with gzip.GzipFile(fileobj=b, mode="wb") as gz:
        gz.write(json.dumps(obj).encode("utf-8"))
    return b.getvalue()

def s3_exists(key: str) -> bool:
    try:
        s3.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except NoCredentialsError:
        print("❌ No AWS credentials found. Run `aws configure` or set a profile.", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey") or status == 404:
            return False
        if status in (301, 400):
            print("❌ S3 region or bucket mismatch. Verify AWS_REGION and bucket region.", file=sys.stderr)
            raise
        raise

def put_json_gz(key: str, obj: dict):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=gzip_bytes(obj),
        ContentType="application/json",
        ContentEncoding="gzip",
    )

# =======================
# MAIN
# =======================
def main():
    riot_id = input("Enter Riot ID (GameName#TAG): ").strip()
    region  = input("Enter Region (na, euw, eune, kr, jp, oce, br, lan, las, sg, th, tw, vn, ph): ").strip().lower()
    year_in = input(f"Year (default {datetime.now().year}): ").strip()
    year    = int(year_in) if year_in else datetime.now().year

    upload_matches   = input("Upload MATCH details to S3? (y/N): ").strip().lower() == "y"
    upload_timelines = input("Upload TIMELINES to S3? (y/N): ").strip().lower() == "y"
    write_ddb_index  = False
    if DDB_TABLE:
        write_ddb_index = input(f"Write DynamoDB index items to '{DDB_TABLE}'? (y/N): ").strip().lower() == "y"

    platform = PLATFORM_BY_REGION.get(region)
    if not platform:
        print(f"❌ Unknown region '{region}'.", file=sys.stderr); return
    regional = regional_from_platform(platform)

    # Resolve PUUID (sync ok)
    puuid = get_puuid_from_riot_id(regional, riot_id)
    print(f"✅ PUUID: {puuid}")

    # List IDs (sync ok)
    print(f"⏳ Listing match IDs for {year}…")
    match_ids = list_match_ids_year(regional, puuid, year)
    total = len(match_ids)
    print(f"✅ Found {total} matches in {year}")

    # Save local IDs file
    out_dir = Path("data") / "raw" / puuid / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "match_ids.json").write_text(json.dumps(match_ids, indent=2))
    print(f"📂 Saved IDs → {out_dir/'match_ids.json'}")

    # Optional limit for testing
    limit_in = input("Limit to first N matches (blank = all): ").strip()
    if limit_in:
        try:
            n = int(limit_in)
            match_ids = match_ids[:n]
            print(f"⚡ Limiting to first {n} matches for this run")
        except ValueError:
            print("⚠️ Invalid number entered, using all matches")

    if not (upload_matches or upload_timelines):
        print("ℹ️ Skipping uploads (you chose No). Done.")
        return

    # Local cache paths (used only if USE_LOCAL_CACHE=True)
    def local_match_path(mid): return out_dir / f"{mid}.json"
    def local_tl_path(mid):    return out_dir / f"{mid}.timeline.json"

    async def run_async():
        sem = asyncio.Semaphore(CONCURRENCY)
        async with aiohttp.ClientSession() as session:

            # 1) FETCH MATCH DETAILS (only for those we need)
            need_details = []
            for mid in match_ids:
                if not upload_matches and not write_ddb_index:
                    continue
                s3_key_match = f"matches/{puuid}/{year}/{mid}.json.gz"
                if s3_exists(s3_key_match):
                    continue
                if USE_LOCAL_CACHE and local_match_path(mid).exists():
                    continue
                need_details.append(mid)

            print(f"🧩 Need {len(need_details)} new match detail fetches")

            match_detail_by_id = {}
            # For DDB indexing we may still require details even if matches already exist in S3.
            # Let's also track details we can load from local cache to avoid extra network.
            for mid in match_ids:
                if (USE_LOCAL_CACHE and local_match_path(mid).exists()) and (upload_matches or write_ddb_index):
                    try:
                        mj = json.loads(local_match_path(mid).read_text())
                        match_detail_by_id[mid] = mj
                    except Exception:
                        pass

            tasks = [fetch_match_detail(session, regional, mid, sem) for mid in need_details]
            done = 0
            for coro in asyncio.as_completed(tasks):
                try:
                    mj = await coro
                    mid = mj.get("metadata", {}).get("matchId")
                    if mid:
                        match_detail_by_id[mid] = mj
                        if USE_LOCAL_CACHE:
                            local_match_path(mid).write_text(json.dumps(mj))
                    done += 1
                    if done and done % 25 == 0:
                        print(f"   details fetched: {done}/{len(need_details)}")
                except Exception as e:
                    print("detail skip:", e)

            # 2) UPLOAD MATCH DETAILS (S3)
            if upload_matches:
                uploaded = 0
                for mid in match_ids:
                    mj = match_detail_by_id.get(mid)
                    if not mj:
                        # Could be already on S3; if yes, skip
                        if s3_exists(f"matches/{puuid}/{year}/{mid}.json.gz"):
                            continue
                        # If neither local nor fetched, skip
                        continue
                    key = f"matches/{puuid}/{year}/{mid}.json.gz"
                    if not s3_exists(key):
                        put_json_gz(key, mj)
                        uploaded += 1
                print(f"☁️  Uploaded {uploaded} match detail objects")

            # 3) FETCH & UPLOAD TIMELINES (only if requested and not in cache/S3)
            if upload_timelines:
                need_tl = []
                for mid in match_ids:
                    key = f"timelines/{puuid}/{year}/{mid}.json.gz"
                    if s3_exists(key):
                        continue
                    if USE_LOCAL_CACHE and local_tl_path(mid).exists():
                        continue
                    need_tl.append(mid)
                print(f"🧭 Need {len(need_tl)} new timelines")

                tl_tasks = [fetch_timeline(session, regional, mid, sem) for mid in need_tl]
                done = 0
                for coro in asyncio.as_completed(tl_tasks):
                    try:
                        tl = await coro
                        mid = tl.get("metadata", {}).get("matchId")
                        if not mid:
                            continue
                        if USE_LOCAL_CACHE:
                            local_tl_path(mid).write_text(json.dumps(tl))
                        key = f"timelines/{puuid}/{year}/{mid}.json.gz"
                        if not s3_exists(key):
                            put_json_gz(key, tl)
                        done += 1
                        if done and done % 20 == 0:
                            print(f"   timelines processed: {done}/{len(need_tl)}")
                    except Exception as e:
                        print("timeline skip:", e)

            # 4) DDB INDEX (optional) — only for matches we have a match JSON for
            if write_ddb_index:
                indexed = 0
                for mid in match_ids:
                    key_m  = f"matches/{puuid}/{year}/{mid}.json.gz" if upload_matches else None
                    key_tl = f"timelines/{puuid}/{year}/{mid}.json.gz" if upload_timelines else None
                    mj = match_detail_by_id.get(mid)
                    if not mj:
                        # If we didn't fetch this run, try to use local cache
                        if USE_LOCAL_CACHE and local_match_path(mid).exists():
                            try:
                                mj = json.loads(local_match_path(mid).read_text())
                            except Exception:
                                pass
                    if mj:
                        try:
                            put_index_ddb(puuid, year, mj, key_m, key_tl)
                            indexed += 1
                        except Exception as e:
                            print("ddb skip:", e)
                print(f"🗂️  Wrote {indexed} index items to DynamoDB")

    asyncio.run(run_async())

    print("🎉 Done.")
    if upload_matches:
        print(f"   → s3://{S3_BUCKET}/matches/{puuid}/{year}/...")
    if upload_timelines:
        print(f"   → s3://{S3_BUCKET}/timelines/{puuid}/{year}/...")

if __name__ == "__main__":
    main()
