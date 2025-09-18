#!/usr/bin/env python3
import argparse, os, time, sys, json
from urllib.parse import quote
import requests
from dotenv import load_dotenv
load_dotenv("secrets/.env")

API_KEY = os.getenv("RIOT_API_KEY")
if not API_KEY:
    print("Missing RIOT_API_KEY env var.", file=sys.stderr)
    sys.exit(1)

# ---- Routing helpers ----
# Platform -> used by /lol/summoner/v4 (and other platform-scoped APIs)
PLATFORM_BY_REGION = {
    # Americas
    "na": "na1", "na1": "na1",
    "br": "br1", "br1": "br1",
    "lan": "la1", "la1": "la1",
    "las": "la2", "la2": "la2",
    # Europe
    "euw": "euw1", "euw1": "euw1",
    "eune": "eun1", "eun1": "eun1",
    "tr": "tr1", "tr1": "tr1",
    "ru": "ru",   # legacy label
    # Asia
    "kr": "kr", "jp": "jp1", "jp1": "jp1",
    # SEA & OCE
    "oce": "oc1", "oc1": "oc1",
    "ph": "ph2", "ph2": "ph2",
    "sg": "sg2", "sg2": "sg2",
    "th": "th2", "th2": "th2",
    "tw": "tw2", "tw2": "tw2",
    "vn": "vn2", "vn2": "vn2",
}

# Regional clusters -> used by /lol/match/v5 and /riot/account/v1
def regional_from_platform(platform: str) -> str:
    if platform in {"na1", "br1", "la1", "la2"}:
        return "americas"
    if platform in {"euw1", "eun1", "tr1", "ru"}:
        return "europe"
    if platform in {"kr", "jp1"}:
        return "asia"
    if platform in {"oc1", "ph2", "sg2", "th2", "tw2", "vn2"}:
        return "sea"
    # Fallback: default to americas
    return "americas"

# ---- HTTP with polite retries ----
def riot_get(url: str, params=None, max_retries=5):
    headers = {"X-Riot-Token": API_KEY}
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=15)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2"))
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600:
            time.sleep(1 + attempt)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()

# ---- Lookups ----
def get_puuid_from_riot_id(regional_host: str, riot_id: str) -> str:
    # riot_id format: GameName#TAG
    if "#" not in riot_id:
        raise ValueError("Riot ID must look like GameName#TAG (or use --use-summoner-name).")
    game, tag = riot_id.split("#", 1)
    url = f"https://{regional_host}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{quote(game)}/{quote(tag)}"
    data = riot_get(url)
    return data["puuid"]

def get_puuid_from_summoner_name(platform_host: str, summoner_name: str) -> str:
    # Deprecated for player-facing, but still usable to locate PUUID
    url = f"https://{platform_host}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{quote(summoner_name)}"
    data = riot_get(url)
    return data["puuid"]

def get_match_ids(regional_host: str, puuid: str, count=20, start=0, queue=None):
    params = {"start": start, "count": count}
    if queue is not None:
        params["queue"] = queue
    url = f"https://{regional_host}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
    return riot_get(url, params=params)

def get_match(regional_host: str, match_id: str):
    url = f"https://{regional_host}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return riot_get(url)

# ---- CLI ----
def main():
    p = argparse.ArgumentParser(description="Fetch Riot match history by Riot ID or Summoner Name.")
    p.add_argument("--region", required=True, help="e.g., na, euw, eune, kr, jp, oce, sg, ph, th, tw, vn")
    p.add_argument("--name", required=True, help="Riot ID 'GameName#TAG' (preferred) or Summoner Name")
    p.add_argument("--use-summoner-name", action="store_true", help="Interpret --name as legacy Summoner Name")
    p.add_argument("--count", type=int, default=10, help="How many match IDs to fetch (max 100 per call)")
    p.add_argument("--dump", action="store_true", help="Also download full match JSONs")
    args = p.parse_args()

    platform = PLATFORM_BY_REGION.get(args.region.lower())
    if not platform:
        raise SystemExit(f"Unknown region '{args.region}'. Try one of: {', '.join(sorted(set(PLATFORM_BY_REGION.keys())))}")
    regional = regional_from_platform(platform)

    if args.use_summoner_name or "#" not in args.name:
        puuid = get_puuid_from_summoner_name(platform, args.name)
    else:
        puuid = get_puuid_from_riot_id(regional, args.name)

    print(f"Resolved PUUID: {puuid}")

    match_ids = get_match_ids(regional, puuid, count=args.count)
    print(f"Got {len(match_ids)} match IDs. Example: {match_ids[:3]}")

    # Save locally
    out_dir = os.path.join("data", "raw", puuid)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir, "match_ids.json"), "w") as f:
        json.dump(match_ids, f, indent=2)

    if args.dump:
        for mid in match_ids:
            mpath = os.path.join(out_dir, f"{mid}.json")
            if os.path.exists(mpath):
                continue
            m = get_match(regional, mid)
            with open(mpath, "w") as f:
                json.dump(m, f)
        print(f"Saved {len(match_ids)} matches to {out_dir}")

if __name__ == "__main__":
    main()
