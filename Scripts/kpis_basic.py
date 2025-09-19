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

# --- queue buckets for split stats ---
QUEUE_BUCKET = {
    "RANKED_SOLO_DUO": {420},          # SoloQ
    "RANKED_FLEX":     {440},          # Flex
    "NORMALS":         {400, 430, 490} # Blind, Draft, Normal Draft (adjust as you like)
    # ARAM=450 not included in splits by default
}

# Treat these item ids as "ignore" when counting favorites (trinkets/boots/consumables).
# You can refine using Data Dragon later; for now we skip trinkets and common consumables.
TRINKETS = {3340, 3363, 3364}
CONSUMABLES = {2003, 2010, 2031, 2033, 2138, 2139, 2140}
BOOTS = {1001, 3006, 3009, 3020, 3047, 3111, 3117}
IGNORE_ITEMS = TRINKETS | CONSUMABLES  # (we will keep boots in favorites by default)


def infer_role(me: dict, info: dict) -> str:
    """
    Prefer teamPosition. If blank, infer from lane/role.
    Special-case ARAM (queueId=450).
    """
    q = info.get("queueId")
    if q == 450:
        return "ARAM"  # label ARAM separately if you want

    # 1) Use teamPosition if present
    tp = (me.get("teamPosition") or "").upper()
    if tp:
        return ROLE_MAP.get(tp, "UNKNOWN")

    # 2) Try older lane/role fields
    lane = (me.get("lane") or "").upper()
    role = (me.get("role") or "").upper()

    # Map obvious lanes first
    if lane in ("TOP", "MIDDLE", "MID", "JUNGLE", "BOTTOM", "BOT", "ADC"):
        if lane == "MID": lane = "MIDDLE"
        if lane in ("BOT", "ADC"): lane = "BOTTOM"

        if lane == "BOTTOM":
            # Disambiguate ADC vs Support by 'role'
            if role in ("DUO_SUPPORT", "SUPPORT"):
                return "SUPPORT"
            elif role in ("DUO_CARRY", "CARRY", "SOLO", "DUO", ""):
                return "BOTTOM"
            else:
                return "BOTTOM"
        if lane == "MIDDLE":
            return "MIDDLE"
        return lane

    # 3) If lane is unknown but role hints support/adc
    if role in ("DUO_SUPPORT", "SUPPORT"):
        return "SUPPORT"
    if role in ("DUO_CARRY", "CARRY"):
        return "BOTTOM"

    return "UNKNOWN"

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
    game_creation_ms = info.get("gameCreation")  # for dating a match later

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

    # smarter role inference (you already have infer_role defined earlier)
    norm_role = infer_role(me, info)

    # team objectives "first" flags (for conditional winrates)
    team_obj = next((t for t in info.get("teams", []) if t.get("teamId") == team_id), {})
    obj = (team_obj.get("objectives") or {})
    team_first_blood  = bool(((obj.get("champion")   or {}).get("first", False)))
    team_first_tower  = bool(((obj.get("tower")      or {}).get("first", False)))
    team_first_dragon = bool(((obj.get("dragon")     or {}).get("first", False)))
    team_first_baron  = bool(((obj.get("baron")      or {}).get("first", False)))
    team_first_herald = bool(((obj.get("riftHerald") or {}).get("first", False)))

    fb_self = bool(me.get("firstBloodKill") or me.get("firstBloodAssist"))

    # teammate PUUIDs (same team) to compute DUO stats
    teammates = [p.get("puuid") for p in team if p.get("puuid") and p.get("puuid") != puuid]
    teammate_names = {p.get("puuid"): (p.get("riotIdGameName") or p.get("summonerName") or "Unknown")
                      for p in team if p.get("puuid")}

    # end-of-game items (we'll count favorites/best/worst based on final build)
    items = [me.get(f"item{i}", 0) for i in range(7)]  # item6 is trinket
    items = [i for i in items if i and i not in IGNORE_ITEMS]  # keep boots by default

    # spells & casts
    spell1, spell2 = me.get("spell1Id"), me.get("spell2Id")
    s1_casts, s2_casts = me.get("summoner1Casts", 0), me.get("summoner2Casts", 0)
    flash_casts = 0
    if spell1 == 4: flash_casts += s1_casts
    if spell2 == 4: flash_casts += s2_casts

    # objective kills credited to player
    p_dragons = me.get("dragonKills", 0)
    p_barons  = me.get("baronKills", 0)
    p_heralds = me.get("riftHeraldKills", 0)
    # "void grubs" field is not stable; try a few likely keys
    p_grubs   = ch.get("voidgrubKills", 0) or ch.get("voidGrubKills", 0) or ch.get("voidMonstersKilled", 0)

    return {
        "match_id": match_json.get("metadata", {}).get("matchId"),
        "queueId": info.get("queueId"),
        "time_played_sec": me.get("timePlayed", info.get("gameDuration", 0)),
        "win": 1 if me.get("win") else 0,
        "game_creation_ms": game_creation_ms,
        "game_version": info.get("gameVersion"),
        "champion": me.get("championName"),
        "role": norm_role,

        # core KPIs
        "kill_participation": _safe_div(me.get("kills",0)+me.get("assists",0), team_kills),
        "damage_share":       _safe_div(me.get("totalDamageDealtToChampions",0), team_dmg),
        "cs_per_min":         _safe_div(cs, mins),
        "vision_pm":          _safe_div(me.get("visionScore",0), mins),
        "objective_contrib":  _safe_div(my_obj, team_obj_total),

        # extended lines
        "kills": me.get("kills", 0),
        "deaths": me.get("deaths", 0),
        "assists": me.get("assists", 0),
        "kda": _safe_div(me.get("kills",0)+me.get("assists",0), me.get("deaths",1)),
        "gold_per_min": _safe_div(me.get("goldEarned",0), mins),
        "dmg_per_min": _safe_div(me.get("totalDamageDealtToChampions",0), mins),
        "dmg_taken_pm": _safe_div(me.get("totalDamageTaken",0), mins),
        "heal_shield_pm": _safe_div(
            me.get("totalHealsOnTeammates",0) + me.get("totalDamageShieldedOnTeammates",0),
            mins
        ),
        "vision_wards": me.get("visionWardsBoughtInGame", 0),
        "wards_killed": me.get("wardsKilled", 0),
        "solo_kills": ch.get("soloKills", 0),
        "double_kills": me.get("doubleKills", 0),
        "triple_kills": me.get("tripleKills", 0),
        "quadra_kills": me.get("quadraKills", 0),
        "penta_kills": me.get("pentaKills", 0),

        # damage types (to champs)
        "phys_to_champs": me.get("physicalDamageDealtToChampions", 0),
        "magic_to_champs": me.get("magicDamageDealtToChampions", 0),
        "true_to_champs": me.get("trueDamageDealtToChampions", 0),

        # objectives & objective damage
        "turret_kills": me.get("turretKills", 0),
        "dragons_killed": p_dragons,
        "barons_killed":  p_barons,
        "heralds_killed": p_heralds,
        "grubs_killed":   p_grubs,
        "objective_damage": me.get("damageDealtToObjectives", 0),

        # first blood
        "fb_involved": 1 if fb_self else 0,
        "team_first_blood":  team_first_blood,
        "team_first_tower":  team_first_tower,
        "team_first_dragon": team_first_dragon,
        "team_first_baron":  team_first_baron,
        "team_first_herald": team_first_herald,

        # items & spells for favorites/best/worst
        "items_final": items,
        "spell1": spell1, "spell2": spell2,
        "flash_casts": flash_casts,

        # duos
        "teammates": teammates,
        "teammate_names": teammate_names
    }


def _aggregate(rows):
    from collections import Counter, defaultdict
    if not rows: return {}
    n = len(rows)
    mean = lambda k: _safe_div(sum(r.get(k,0) for r in rows), n)

    agg = {
        "games": n,
        "winrate": _safe_div(sum(r["win"] for r in rows), n),

        # existing means
        "kill_participation_mean": mean("kill_participation"),
        "damage_share_mean":       mean("damage_share"),
        "cs_per_min_mean":         mean("cs_per_min"),
        "vision_pm_mean":          mean("vision_pm"),
        "objective_contrib_mean":  mean("objective_contrib"),

        # requested means
        "gold_per_min_mean": mean("gold_per_min"),
        "dmg_per_min_mean":  mean("dmg_per_min"),
        "dmg_taken_pm_mean": mean("dmg_taken_pm"),

        # objectives (totals and per-game)
        "turrets_killed_total": sum(r.get("turret_kills",0) for r in rows),
        "dragons_killed_total": sum(r.get("dragons_killed",0) for r in rows),
        "barons_killed_total":  sum(r.get("barons_killed",0)  for r in rows),
        "heralds_killed_total": sum(r.get("heralds_killed",0) for r in rows),
        "grubs_killed_total":   sum(r.get("grubs_killed",0)   for r in rows),
        "objective_damage_total": sum(r.get("objective_damage",0) for r in rows),
        "objective_damage_pm_mean": mean("objective_damage") / max(mean("time_played_sec")/60.0, 1e-9),

        # first blood rate (self)
        "first_blood_rate_self": mean("fb_involved"),

        # avg game time (minutes)
        "avg_game_time_min": mean("time_played_sec") / 60.0
    }

    # --- damage type preference ---
    phys = sum(r.get("phys_to_champs",0) for r in rows)
    mag  = sum(r.get("magic_to_champs",0) for r in rows)
    tru  = sum(r.get("true_to_champs",0)  for r in rows)
    fav_type = max([("PHYSICAL", phys), ("MAGIC", mag), ("TRUE", tru)], key=lambda x: x[1])[0] if (phys+mag+tru)>0 else None
    agg["favorite_damage_type"] = fav_type

    # --- role & champs ---
    champs = Counter(r.get("champion") for r in rows if r.get("champion"))
    roles  = Counter(r.get("role","UNKNOWN") for r in rows)
    agg["top_champions"] = [{"name": c, "games": g} for c, g in champs.most_common(5)]
    agg["role_distribution"] = [{"role": r, "games": g} for r, g in roles.most_common()]

    # --- items: favorite / best / worst (by winrate) ---
    item_counts = Counter()
    item_result = defaultdict(lambda: {"games":0,"wins":0})
    for r in rows:
        for it in r.get("items_final", []):
            item_counts[it] += 1
            item_result[it]["games"] += 1
            item_result[it]["wins"]  += r["win"]

    def _best_worst_items(min_games=10):
        ranked = []
        for it, res in item_result.items():
            g, w = res["games"], res["wins"]
            if g >= min_games:
                ranked.append({"itemId": it, "games": g, "wins": w, "winrate": _safe_div(w, g)})
        ranked.sort(key=lambda x: x["winrate"], reverse=True)
        best = ranked[:5]
        worst = ranked[-5:] if ranked else []
        return best, worst

    agg["favorite_items"] = [{"itemId": it, "games": cnt} for it, cnt in item_counts.most_common(10)]
    best, worst = _best_worst_items(min_games=10)
    agg["best_items"]  = best
    agg["worst_items"] = worst

    # --- summoner spells & flash usage ---
    spell_counts = Counter()
    flash_total = 0
    for r in rows:
        s1, s2 = r.get("spell1"), r.get("spell2")
        if s1: spell_counts[s1] += 1
        if s2: spell_counts[s2] += 1
        flash_total += r.get("flash_casts", 0)
    agg["favorite_summoner_spell"] = (spell_counts.most_common(1)[0][0] if spell_counts else None)
    agg["flash_casts_total"] = flash_total
    agg["flash_casts_per_game"] = _safe_div(flash_total, n)

    # --- DUO winrates (favorite/best/worst) ---
    duo = defaultdict(lambda: {"games":0,"wins":0,"name":None})
    for r in rows:
        for mate in r.get("teammates", []):
            duo[mate]["games"] += 1
            duo[mate]["wins"]  += r["win"]
            name = r.get("teammate_names", {}).get(mate)
            if name: duo[mate]["name"] = name

    duo_stats = []
    for mate, v in duo.items():
        duo_stats.append({
            "mate_puuid": mate,
            "name": v["name"] or "Unknown",
            "games": v["games"],
            "wins": v["wins"],
            "winrate": _safe_div(v["wins"], v["games"])
        })
    duo_stats.sort(key=lambda x: x["games"], reverse=True)
    agg["duo_most_played"] = duo_stats[0] if duo_stats else None

    MIN_DUO_GAMES = 5
    eligible = [d for d in duo_stats if d["games"] >= MIN_DUO_GAMES]
    agg["duo_best"]  = (max(eligible, key=lambda x: x["winrate"]) if eligible else None)
    agg["duo_worst"] = (min(eligible, key=lambda x: x["winrate"]) if eligible else None)

    # --- Queue splits (normals, flex, solo/duo) ---
    def _split_wr(qset):
        subset = [r for r in rows if r.get("queueId") in qset]
        return {
            "games": len(subset),
            "winrate": _safe_div(sum(r["win"] for r in subset), len(subset)) if subset else None
        }
    agg["split_ranked_solo_duo"] = _split_wr(QUEUE_BUCKET["RANKED_SOLO_DUO"])
    agg["split_ranked_flex"]     = _split_wr(QUEUE_BUCKET["RANKED_FLEX"])
    agg["split_normals"]         = _split_wr(QUEUE_BUCKET["NORMALS"])

    # =========================
    # Per-champion winrates (+ FB conditionals)
    # =========================
    by_champ = defaultdict(lambda: {
        "games":0, "wins":0,
        "games_fb_self":0, "wins_fb_self":0,
        "games_team_fb":0, "wins_team_fb":0
    })
    for r in rows:
        c = r.get("champion") or "Unknown"
        bc = by_champ[c]
        bc["games"] += 1
        bc["wins"]  += r["win"]
        if r.get("fb_self"):
            bc["games_fb_self"] += 1
            bc["wins_fb_self"]  += r["win"]
        if r.get("team_first_blood"):
            bc["games_team_fb"] += 1
            bc["wins_team_fb"]  += r["win"]

    champ_wr = []
    for c, v in by_champ.items():
        champ_wr.append({
            "name": c,
            "games": v["games"],
            "wins": v["wins"],
            "winrate": _safe_div(v["wins"], v["games"]),
            "winrate_when_fb_self": _safe_div(v["wins_fb_self"], v["games_fb_self"]) if v["games_fb_self"] else None,
            "winrate_when_team_fb": _safe_div(v["wins_team_fb"], v["games_team_fb"]) if v["games_team_fb"] else None,
        })
    champ_wr.sort(key=lambda x: x["games"], reverse=True)
    agg["champion_winrates"] = champ_wr[:min(20, len(champ_wr))]

    # =========================
    # Global conditional winrates
    # =========================
    def _cond_wr(pred):
        subset = [r for r in rows if pred(r)]
        return {
            "games": len(subset),
            "winrate": _safe_div(sum(r["win"] for r in subset), len(subset)) if subset else None
        }
    agg["when_fb_self"]         = _cond_wr(lambda r: r.get("fb_self"))
    agg["when_team_first_blood"]= _cond_wr(lambda r: r.get("team_first_blood"))
    agg["when_first_tower"]     = _cond_wr(lambda r: r.get("team_first_tower"))
    agg["when_first_dragon"]    = _cond_wr(lambda r: r.get("team_first_dragon"))
    agg["when_first_baron"]     = _cond_wr(lambda r: r.get("team_first_baron"))
    agg["when_first_herald"]    = _cond_wr(lambda r: r.get("team_first_herald"))

    # =========================
    # Largest gold lead/deficit (REQUIRES TIMELINE)
    # We will fill these in timeline pass. For now, include fields as null.
    # =========================
    agg["largest_gold_lead"] = None     # {"match_id": "...", "date_iso": "...", "gold": 0}
    agg["largest_gold_deficit"] = None  # {"match_id": "...", "date_iso": "...", "gold": 0}

    return agg

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
