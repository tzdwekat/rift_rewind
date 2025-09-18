#!/usr/bin/env python3
import argparse, asyncio, aiohttp, os, time, sys, json
from urllib.parse import quote
import requests
from dotenv import load_dotenv
from datetime import datetime
load_dotenv("Secrets/.env")

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
    riot_id = input("Enter Riot ID (format: SummonerName#TAG): ").strip()
    region = input("Enter Region(na, euw, eune, kr, jp, oce, br, lan, las, sg, th, tw, vn, ph): ").strip().lower()
    year = (datetime.now()).year

    platform = PLATFORM_BY_REGION.get(region)
    if not platform:
        print(f"❌ Unknown region '{region}'.", file=sys.stderr)
        return
    regional = regional_from_platform(platform)

    puuid = get_puuid_from_riot_id(regional, riot_id)
    print(f"✅ PUUID resolved: {puuid}")

    match_ids = get_match_ids(regional, puuid, count=5)
    print(f"✅ Found {len(match_ids)} matches:")
    for mid in match_ids:
        print(" -", mid)

    # Save results
    out_dir = os.path.join("data","raw",puuid)
    os.makedirs(out_dir, exist_ok=True)
    with open(os.path.join(out_dir,"match_ids.json"),"w") as f:
        json.dump(match_ids,f,indent=2)
    print(f"📂 Saved to {out_dir}/match_ids.json")

if __name__ == "__main__":
    main()
