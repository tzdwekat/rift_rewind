# scripts/analyze_with_bedrock.py
import os, sys, json, argparse, boto3
from dotenv import load_dotenv

# -------------------------------
# ENV / AWS clients
# -------------------------------
load_dotenv("secrets/.env")

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET  = os.getenv("S3_BUCKET")
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
# Claude 3.5 via Inference Profile ARN (REQUIRED for many accounts)
BEDROCK_INFERENCE_PROFILE_ARN = os.getenv("BEDROCK_INFERENCE_PROFILE_ARN")

s3 = boto3.client("s3", region_name=AWS_REGION)
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

# -------------------------------
# Riot routing (for optional --riot-id resolve)
# -------------------------------
PLATFORM_TO_CLUSTER = {
    "na":"americas","na1":"americas","br":"americas","br1":"americas","lan":"americas","la1":"americas","las":"americas","la2":"americas",
    "euw":"europe","euw1":"europe","eune":"europe","eun1":"europe","tr":"europe","tr1":"europe","ru":"europe",
    "kr":"asia","jp":"asia","jp1":"asia",
    "oce":"sea","oc1":"sea","ph":"sea","ph2":"sea","sg":"sea","sg2":"sea","th":"sea","th2":"sea","tw":"sea","tw2":"sea","vn":"sea","vn2":"sea",
}

def resolve_puuid(riot_id: str, region: str) -> str:
    assert RIOT_API_KEY, "Missing RIOT_API_KEY in secrets/.env"
    if "#" not in riot_id:
        raise ValueError("Riot ID must be GameName#TAG")
    game, tag = riot_id.split("#", 1)
    cluster = PLATFORM_TO_CLUSTER.get(region.lower(), "americas")
    url = f"https://{cluster}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{game}/{tag}"
    import requests
    r = requests.get(url, headers={"X-Riot-Token": RIOT_API_KEY}, timeout=20)
    r.raise_for_status()
    return r.json()["puuid"]

# -------------------------------
# S3 helpers
# -------------------------------
def load_kpis_from_s3(puuid: str, year: str) -> dict:
    key = f"kpis/{puuid}/{year}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        prefix = f"kpis/{puuid}/"
        print(f"❌ Not found: s3://{S3_BUCKET}/{key}")
        print("🔎 Listing available KPI files for this PUUID:")
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        for it in resp.get("Contents", []):
            print(" -", it["Key"])
        raise
    except Exception as e:
        print(f"S3 error: {e}")
        raise

# -------------------------------
# Compaction to stay under token limits
# -------------------------------
def _top_n(lst, key=lambda x: x, n=10):
    try:
        return sorted(lst, key=key, reverse=True)[:n]
    except Exception:
        return lst[:n]

def compact_kpis(kpis_doc: dict,
                 keep_champs=12,
                 keep_items=6,
                 keep_duos=True,
                 drop_zero_objectives=False) -> dict:
    """
    Slim KPI dict to avoid hitting model input caps.
    Keeps headline aggregates and top-N of noisy arrays.
    Accepts either {puuid,year,kpis:{...}} or a bare KPI dict.
    """
    k = kpis_doc.get("kpis") or kpis_doc
    out = {}

    # Headline aggregates
    for fld in [
        "games","winrate","avg_game_time_min",
        "kill_participation_mean","damage_share_mean","cs_per_min_mean",
        "vision_pm_mean","gold_per_min_mean","dmg_per_min_mean",
        "objective_contrib_mean","objective_damage_pm_mean",
        "first_blood_rate_self","favorite_damage_type"
    ]:
        if fld in k: out[fld] = k[fld]

    # Objectives
    for fld in ["turrets_killed_total","dragons_killed_total","barons_killed_total",
                "heralds_killed_total","grubs_killed_total","objective_damage_total"]:
        if fld in k:
            if drop_zero_objectives and not k[fld]:
                continue
            out[fld] = k[fld]

    # Conditional winrates (team firsts)
    for fld in ["when_team_first_blood","when_first_tower","when_first_dragon",
                "when_first_baron","when_first_herald"]:
        if fld in k and isinstance(k[fld], dict):
            out[fld] = {kk: k[fld].get(kk) for kk in ("games","winrate")}

    # Top champs (by games)
    if "top_champions" in k:
        out["top_champions"] = _top_n(k["top_champions"], key=lambda x: x.get("games",0), n=min(keep_champs, 12))

    # Role distribution
    if "role_distribution" in k:
        out["role_distribution"] = k["role_distribution"]

    # Champion winrates (trim)
    if "champion_winrates" in k:
        out["champion_winrates"] = _top_n(k["champion_winrates"], key=lambda x: x.get("games",0), n=min(keep_champs, 12))

    # Items (trim)
    for fld in ["favorite_items","best_items","worst_items"]:
        if fld in k and isinstance(k[fld], list):
            out[fld] = _top_n(k[fld], key=lambda x: x.get("games",0), n=min(keep_items, 6))

    # Duos
    if keep_duos:
        for fld in ["duo_most_played","duo_best","duo_worst"]:
            if fld in k: out[fld] = k[fld]

    # Queue splits
    for fld in ["split_ranked_solo_duo","split_ranked_flex","split_normals"]:
        if fld in k: out[fld] = k[fld]

    return out

# -------------------------------
# Bedrock (Claude 3.5 via Converse + Inference Profile)
# -------------------------------
def analyze_with_bedrock(kpis_doc: dict) -> str:
    if not BEDROCK_INFERENCE_PROFILE_ARN:
        raise RuntimeError("BEDROCK_INFERENCE_PROFILE_ARN is required for Claude 3.5. Set it in secrets/.env")

    # Compact a bit to reduce costs and avoid hitting length
    slim = compact_kpis(kpis_doc, keep_champs=12, keep_items=6)

    prompt = (
        "You are a League of Legends coach.\n"
        "The info you output should be in a fun playful way. Banter if possible but don't be mean.\n"
        "Write ONLY the final report in clean Markdown. No JSON, no prefaces.\n\n"
        "## Summary\n"
        "- 3–5 bullets (games, winrate, avg game length)\n\n"
        "## Playstyle\n"
        "- kill participation, damage share, cs/min, vision/min, gold/min, dmg/min\n\n"
        "## Objectives\n"
        "- totals + conditional winrates when first objective secured\n\n"
        "## Mains & Roles\n"
        "- top champions with winrates; role distribution\n\n"
        "## Items & Duos\n"
        "- favorite items; best/worst items by winrate; most-played/best/worst duo\n\n"
        "## Coaching Insights\n"
        "- 6–10 specific, actionable suggestions tied to the numbers\n\n"
        f"KPI JSON (slice):\n{json.dumps(slim, separators=(',',':'))}\n"
    )

    # Mirror the AWS Playground style: pass the inference profile ARN as modelId
    resp = bedrock.converse(
        modelId=BEDROCK_INFERENCE_PROFILE_ARN,
        messages=[{"role": "user", "content": [{"text": prompt}]}],
        inferenceConfig={
            "maxTokens": 2000,
            "stopSequences": ["\n\nHuman:"],
            "temperature": 0.5,
            "topP": 0.999
        },
        additionalModelRequestFields={
            "top_k": 250
        },
        performanceConfig={
            "latency": "standard"
        }
    )

    # Parse Converse output
    try:
        return resp["output"]["message"]["content"][0]["text"].strip()
    except Exception:
        # If the structure changes, return the raw response for debugging
        return json.dumps(resp, indent=2)

# -------------------------------
# CLI
# -------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--puuid", help="Player PUUID (reads kpis/{puuid}/{year}.json from S3)")
    ap.add_argument("--year", help="Year like 2025")
    ap.add_argument("--file", help="Read KPIs from local JSON file instead of S3")
    ap.add_argument("--riot-id", help="Alternative: resolve PUUID from Riot ID Name#TAG")
    ap.add_argument("--region", help="Region for Riot ID resolve (e.g., na, euw, kr)")
    args = ap.parse_args()

    # Load KPI JSON
    if args.file:
        with open(args.file, "r") as f:
            kpis_doc = json.load(f)
    else:
        if not S3_BUCKET:
            print("❌ Set S3_BUCKET in secrets/.env or use --file")
            sys.exit(1)
        puuid = args.puuid
        if not puuid:
            if args.riot_id and args.region:
                puuid = resolve_puuid(args.riot_id, args.region)
                print("PUUID:", puuid)
            else:
                print("❌ Provide --puuid + --year OR --file OR --riot-id + --region + --year")
                sys.exit(1)
        if not args.year:
            print("❌ Provide --year")
            sys.exit(1)
        kpis_doc = load_kpis_from_s3(puuid, str(args.year))

    # Ask Claude 3.5 for the coaching report
    summary = analyze_with_bedrock(kpis_doc)
    print("\n===== COACH REPORT =====\n")
    print(summary)

if __name__ == "__main__":
    main()
