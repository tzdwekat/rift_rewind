# scripts/utils.py

# ----- Imports reused across scripts -----
from imports import (
    os, sys, io, time, json, gzip, random, asyncio,
    Path, quote, datetime, timezone,
    requests, aiohttp, boto3, load_dotenv,
    ClientError, NoCredentialsError
)

# ----- Env / AWS -----
load_dotenv("secrets/.env")
API_KEY    = os.getenv("RIOT_API_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET  = os.getenv("S3_BUCKET")
DDB_TABLE  = os.getenv("DDB_TABLE")  # optional

HEADERS = {"X-Riot-Token": API_KEY}
s3  = boto3.client("s3", region_name=AWS_REGION)
ddb = boto3.client("dynamodb", region_name=AWS_REGION) if DDB_TABLE else None

# ===== S3 helpers =====
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
        print("❌ No AWS credentials found. Run `aws configure`.", file=sys.stderr)
        sys.exit(1)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if code in ("404", "NoSuchKey") or status == 404:
            return False
        if status in (301, 400):
            print("❌ S3 region or bucket mismatch. Verify AWS_REGION & bucket.", file=sys.stderr)
            raise
        raise

def put_json_gz(key: str, obj: dict):
    s3.put_object(
        Bucket=S3_BUCKET, Key=key,
        Body=gzip_bytes(obj),
        ContentType="application/json",
        ContentEncoding="gzip",
    )

# ===== Region routing =====
PLATFORM_BY_REGION = {
    "na":"na1","na1":"na1","br":"br1","br1":"br1","lan":"la1","la1":"la1","las":"la2","la2":"la2",
    "euw":"euw1","euw1":"euw1","eune":"eun1","eun1":"eun1","tr":"tr1","tr1":"tr1","ru":"ru",
    "kr":"kr","jp":"jp1","jp1":"jp1",
    "oce":"oc1","oc1":"oc1","ph":"ph2","ph2":"ph2","sg":"sg2","sg2":"sg2","th":"th2","th2":"th2","tw":"tw2","tw2":"tw2","vn":"vn2","vn2":"vn2",
}

def regional_from_platform(platform: str) -> str:
    if platform in {"na1","br1","la1","la2"}: return "americas"
    if platform in {"euw1","eun1","tr1","ru"}: return "europe"
    if platform in {"kr","jp1"}: return "asia"
    if platform in {"oc1","ph2","sg2","th2","tw2","vn2"}: return "sea"
    return "americas"

# ===== Sync Riot calls =====
def riot_get_sync(url: str, params=None, max_retries=5):
    for attempt in range(max_retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2") or "2")
            time.sleep(wait); continue
        if 500 <= r.status_code < 600:
            time.sleep(1 + attempt); continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

def get_puuid_from_riot_id(regional_host: str, riot_id: str) -> str:
    if "#" not in riot_id:
        raise ValueError("Riot ID must be GameName#TAG")
    game, tag = riot_id.split("#", 1)
    url = f"https://{regional_host}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{quote(game)}/{quote(tag)}"
    return riot_get_sync(url)["puuid"]

def list_match_ids_year(regional_host: str, puuid: str, year: int) -> list[str]:
    start_dt = datetime(year, 1, 1, tzinfo=timezone.utc)
    end_dt   = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    start_ts = int(start_dt.timestamp()); end_ts = int(end_dt.timestamp())
    all_ids, start = [], 0
    while True:
        params = {"start": start, "count": 100, "startTime": start_ts, "endTime": end_ts}
        url = f"https://{regional_host}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        batch = riot_get_sync(url, params=params)
        if not batch: break
        all_ids.extend(batch)
        start += len(batch)
        if len(batch) < 100: break
        time.sleep(0.03)
    seen, out = set(), []
    for mid in all_ids:
        if mid not in seen:
            seen.add(mid); out.append(mid)
    return out

# ===== Async Riot calls =====
async def get_json(session: aiohttp.ClientSession, url: str, params=None, retries=5):
    for attempt in range(retries):
        try:
            async with session.get(url, headers=HEADERS, params=params, timeout=aiohttp.ClientTimeout(total=40)) as r:
                if r.status == 429:
                    wait = int(r.headers.get("Retry-After", "2"))
                    await asyncio.sleep(wait); continue
                if 500 <= r.status < 600:
                    await asyncio.sleep(1 + attempt + random.random()); continue
                r.raise_for_status()
                return await r.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            await asyncio.sleep(0.5 + attempt)
    raise RuntimeError(f"Failed after retries: {url}")

async def fetch_match_detail(session, regional, match_id, sem):
    async with sem:
        url = f"https://{regional}.api.riotgames.com/lol/match/v5/matches/{match_id}"
        return await get_json(session, url)

async def fetch_timeline(session, regional, match_id, sem):
    async with sem:
        url = f"https://{regional}.api.riotgames.com/lol/match/v5/matches/{match_id}/timeline"
        return await get_json(session, url)

# ===== DDB index (optional) =====
def put_index_ddb(puuid: str, year: int, match_json: dict, s3_key_match: str | None, s3_key_timeline: str | None):
    if not ddb: return
    info = match_json.get("info", {})
    parts = info.get("participants", [])
    me = next((p for p in parts if p.get("puuid")==puuid), {}) if parts else {}
    mid = match_json.get("metadata", {}).get("matchId", "")
    item = {
        "pk": {"S": puuid},
        "sk": {"S": f"{year}#{mid}"},
        "region": {"S": info.get("platformId","unknown")},
        "patch": {"S": info.get("gameVersion","")},
        "queueId": {"N": str(info.get("queueId", 0))},
        "gameCreation": {"N": str(info.get("gameCreation", 0))},
        "durationSec": {"N": str(info.get("gameDuration", me.get("timePlayed", 0)))},
        "champion": {"S": me.get("championName","")},
        "role": {"S": (me.get("teamPosition") or me.get("role") or "")},
    }
    if s3_key_match:
        item["s3_key_match"] = {"S": s3_key_match}
    if s3_key_timeline:
        item["s3_key_timeline"] = {"S": s3_key_timeline}
    ddb.put_item(TableName=DDB_TABLE, Item=item)
