# scripts/kpis_basic.py
import sys, os
sys.path.append(os.path.join(os.path.dirname(__file__)))

from imports import os, json, io, gzip, boto3
from utils import S3_BUCKET, DDB_TABLE

s3  = boto3.client("s3")
ddb = boto3.client("dynamodb")


ROLE_MAP = {
    "TOP": "TOP",
    "JUNGLE": "JUNGLE",
    "MIDDLE": "MIDDLE",
    "BOTTOM": "BOTTOM",
    "UTILITY": "SUPPORT",   # normalize here
    "": "UNKNOWN"
}

def _gunzip_to_json(body_bytes: bytes):
    import gzip, io, json
    with gzip.GzipFile(fileobj=io.BytesIO(body_bytes), mode="rb") as gz:
        return json.loads(gz.read())

def _query_matches(puuid: str, year: str):
    resp = ddb.query(
        TableName=DDB_TABLE,
        KeyConditionExpression="pk = :pk AND begins_with(sk, :yr)",
        ExpressionAttributeValues={":pk": {"S": puuid}, ":yr": {"S": f"{year}#"}}
    )
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = ddb.query(
            TableName=DDB_TABLE,
            KeyConditionExpression="pk = :pk AND begins_with(sk, :yr)",
            ExpressionAttributeValues={":pk": {"S": puuid}, ":yr": {"S": f"{year}#"}},
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))
    out = []
    for it in items:
        sk = it["sk"]["S"]
        mid = sk.split("#", 1)[1]
        key = it.get("s3_key_match", {}).get("S") or f"matches/{puuid}/{year}/{mid}.json.gz"
        out.append((mid, key))
    return out

def _safe_div(n, d): return (n / d) if d else 0.0

def _features_from_match(match_json: dict, puuid: str):
    info = match_json.get("info", {})
    parts = info.get("participants", [])
    me = next((p for p in parts if p.get("puuid") == puuid), None)
    if not me: return None
    mins = (info.get("gameDuration") or me.get("timePlayed", 0)) / 60.0 or 0.0001
    team_id = me.get("teamId")
    team = [p for p in parts if p.get("teamId") == team_id]
    team_kills = sum(p.get("kills", 0) for p in team)
    team_dmg   = sum(p.get("totalDamageDealtToChampions", 0) for p in team)
    ch = me.get("challenges") or {}
    my_obj = int(ch.get("dragonTakedowns", 0)) + int(ch.get("baronTakedowns", 0)) + int(ch.get("riftHeraldTakedowns", 0))
    team_drag = max(int(p.get("challenges", {}).get("dragonTakedowns", 0)) for p in team) if team else 0
    team_baron= max(int(p.get("challenges", {}).get("baronTakedowns", 0)) for p in team) if team else 0
    team_her  = max(int(p.get("challenges", {}).get("riftHeraldTakedowns", 0)) for p in team) if team else 0
    team_obj_total = team_drag + team_baron + team_her
    cs = me.get("totalMinionsKilled", 0) + me.get("neutralMinionsKilled", 0)
    raw_role = (me.get("teamPosition") or me.get("role") or "")
    norm_role = ROLE_MAP.get(raw_role, "UNKNOWN")

    return {
        "match_id": match_json.get("metadata", {}).get("matchId"),
        "win": 1 if me.get("win") else 0,
        "champion": me.get("championName"),
        "role": norm_role,
        "kill_participation": _safe_div(me.get("kills",0)+me.get("assists",0), team_kills),
        "damage_share":       _safe_div(me.get("totalDamageDealtToChampions",0), team_dmg),
        "cs_per_min":         _safe_div(cs, mins),
        "vision_pm":          _safe_div(me.get("visionScore",0), mins),
        "objective_contrib":  _safe_div(my_obj, team_obj_total),
    }



def _aggregate(rows):
    from collections import Counter
    if not rows: return {}
    n = len(rows)
    mean = lambda k: _safe_div(sum(r[k] for r in rows), n)
    wr = _safe_div(sum(r["win"] for r in rows), n)
    champs = Counter(r["champion"] for r in rows if r["champion"])
    roles  = Counter(ROLE_MAP.get(r.get("role",""), "UNKNOWN") for r in rows if r.get("role") is not None)
    top_champs = [{"name": c, "games": g} for c, g in champs.most_common(5)]
    top_roles  = [{"role": r, "games": g} for r, g in roles.most_common(5)]
    return {
        "games": n,
        "winrate": wr,
        "kill_participation_mean": mean("kill_participation"),
        "damage_share_mean":       mean("damage_share"),
        "cs_per_min_mean":         mean("cs_per_min"),
        "vision_pm_mean":          mean("vision_pm"),
        "objective_contrib_mean":  mean("objective_contrib"),
        "top_champions": top_champs,
        "role_distribution": top_roles,
    }

def write_kpis_to_s3(puuid: str, year: str, kpis: dict) -> str:
    key = f"kpis/{puuid}/{year}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(kpis).encode("utf-8"), ContentType="application/json")
    return key

if __name__ == "__main__":
    # CLI usage: python scripts/kpis_basic.py <PUUID> <YEAR> [LIMIT]
    import sys
    if len(sys.argv) < 3:
        print("usage: python scripts/kpis_basic.py <PUUID> <YEAR> [LIMIT]")
        sys.exit(1)
    puuid = sys.argv[1]
    year  = sys.argv[2]
    limit = int(sys.argv[3]) if len(sys.argv) > 3 else 0

    match_refs = _query_matches(puuid, year)
    if limit: match_refs = match_refs[:limit]

    rows = []
    for mid, key in match_refs:
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
            mj  = _gunzip_to_json(obj["Body"].read())
            fr  = _features_from_match(mj, puuid)
            if fr: rows.append(fr)
        except Exception as e:
            print("skip:", e)

    kpis = _aggregate(rows)
    s3_key = write_kpis_to_s3(puuid, year, {"puuid": puuid, "year": year, "kpis": kpis})
    print(f"✅ KPIs computed for {len(rows)} matches.\n📦 Saved JSON → s3://{S3_BUCKET}/{s3_key}")
    print(json.dumps(kpis, indent=2))
