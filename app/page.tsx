'use client';

import React, { useState } from 'react';

type KpisResponse = {
  puuid: string;
  year: string;
  kpis: {
    games: number;
    winrate: number;
    kill_participation_mean: number;
    damage_share_mean: number;
    cs_per_min_mean: number;
    vision_pm_mean: number;
    objective_contrib_mean: number;

    gold_per_min_mean?: number;
    dmg_per_min_mean?: number;
    dmg_taken_pm_mean?: number;

    turrets_killed_total?: number;
    dragons_killed_total?: number;
    barons_killed_total?: number;
    heralds_killed_total?: number;
    grubs_killed_total?: number;
    objective_damage_total?: number;
    objective_damage_pm_mean?: number;

    first_blood_rate_self?: number;
    avg_game_time_min?: number;

    favorite_damage_type?: 'PHYSICAL' | 'MAGIC' | 'TRUE' | null;

    top_champions: { name: string; games: number }[];
    role_distribution: { role: string; games: number }[];

    favorite_items?: { itemId: number; games: number }[];
    best_items?: { itemId: number; games: number; wins: number; winrate: number }[];
    worst_items?: { itemId: number; games: number; wins: number; winrate: number }[];

    favorite_summoner_spell?: number | null;
    flash_casts_total?: number;
    flash_casts_per_game?: number;

    duo_most_played?: { mate_puuid: string; name: string; games: number; wins: number; winrate: number } | null;
    duo_best?: { mate_puuid: string; name: string; games: number; wins: number; winrate: number } | null;
    duo_worst?: { mate_puuid: string; name: string; games: number; wins: number; winrate: number } | null;

    split_ranked_solo_duo?: { games: number; winrate: number | null };
    split_ranked_flex?: { games: number; winrate: number | null };
    split_normals?: { games: number; winrate: number | null };

    champion_winrates?: {
      name: string;
      games: number;
      wins: number;
      winrate: number;
      winrate_when_fb_self: number | null;
      winrate_when_team_fb: number | null;
    }[];

    when_fb_self?: { games: number; winrate: number | null };
    when_team_first_blood?: { games: number; winrate: number | null };
    when_first_tower?: { games: number; winrate: number | null };
    when_first_dragon?: { games: number; winrate: number | null };
    when_first_baron?: { games: number; winrate: number | null };
    when_first_herald?: { games: number; winrate: number | null };

    largest_gold_lead?: { match_id: string; date_iso: string; gold: number } | null;
    largest_gold_deficit?: { match_id: string; date_iso: string; gold: number } | null;
  };
};

function pct(n?: number | null) {
  if (n === undefined || n === null) return '—';
  return `${(n * 100).toFixed(1)}%`;
}

function num(n?: number | null, digits = 2) {
  if (n === undefined || n === null) return '—';
  return Number(n).toFixed(digits);
}

export default function Home() {
  const [riotId, setRiotId] = useState('riq#8008');
  const [region, setRegion] = useState('na');
  const [year, setYear] = useState(new Date().getFullYear().toString());
  const [loading, setLoading] = useState(false);
  const [data, setData] = useState<KpisResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setData(null);
    try {
      const res = await fetch('/api/kpis', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ riotId, region, year }),
      });
      if (!res.ok) {
        const t = await res.text();
        throw new Error(t || `HTTP ${res.status}`);
      }
      const json = (await res.json()) as KpisResponse;
      setData(json);
    } catch (err: any) {
      setError(err.message || 'Failed to fetch KPIs');
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="min-h-screen bg-slate-950 text-slate-100">
      <div className="mx-auto max-w-5xl p-6">
        <h1 className="text-3xl md:text-4xl font-semibold tracking-tight">
          Rift Rewind — KPI Explorer
        </h1>
        <p className="mt-2 text-slate-400">
          Enter a Riot ID, region, and year. We’ll resolve the player, pull your precomputed KPIs from S3, and render a quick recap.
        </p>

        <form onSubmit={onSubmit} className="mt-6 grid grid-cols-1 md:grid-cols-5 gap-3 bg-slate-900/50 p-4 rounded-2xl">
          <input
            className="col-span-2 rounded-xl bg-slate-900 px-3 py-2 outline-none ring-1 ring-slate-700 focus:ring-2 focus:ring-indigo-500"
            placeholder="Riot ID (GameName#TAG)"
            value={riotId}
            onChange={(e) => setRiotId(e.target.value)}
            required
          />
          <select
            className="rounded-xl bg-slate-900 px-3 py-2 outline-none ring-1 ring-slate-700 focus:ring-2 focus:ring-indigo-500"
            value={region}
            onChange={(e) => setRegion(e.target.value)}
          >
            {['na','euw','eune','kr','jp','oce','br','lan','las','sg','th','tw','vn','ph'].map(r => (
              <option key={r} value={r}>{r.toUpperCase()}</option>
            ))}
          </select>
          <input
            className="rounded-xl bg-slate-900 px-3 py-2 outline-none ring-1 ring-slate-700 focus:ring-2 focus:ring-indigo-500"
            placeholder="Year"
            value={year}
            onChange={(e) => setYear(e.target.value)}
            pattern="\d{4}"
            required
          />
          <button
            type="submit"
            className="rounded-xl bg-indigo-600 hover:bg-indigo-500 transition px-4 py-2 font-semibold disabled:opacity-60"
            disabled={loading}
          >
            {loading ? 'Fetching…' : 'Get KPIs'}
          </button>
        </form>

        {error && (
          <div className="mt-4 rounded-xl bg-red-900/40 border border-red-700 px-4 py-3">
            <p className="font-medium text-red-200">Error</p>
            <p className="text-red-300 text-sm">{error}</p>
          </div>
        )}

        {data && (
          <section className="mt-8 space-y-6">
            <header className="flex items-center justify-between">
              <div>
                <h2 className="text-2xl font-semibold">Year {data.year}</h2>
                <p className="text-slate-400 text-sm break-all">PUUID: {data.puuid}</p>
              </div>
              <div className="text-right">
                <p className="text-3xl font-bold">{pct(data.kpis.winrate)} winrate</p>
                <p className="text-slate-400">{data.kpis.games} games</p>
              </div>
            </header>

            {/* Stat grid */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card title="Kill Participation" value={pct(data.kpis.kill_participation_mean)} />
              <Card title="Damage Share" value={pct(data.kpis.damage_share_mean)} />
              <Card title="CS per Min" value={num(data.kpis.cs_per_min_mean)} />
              <Card title="Vision / Min" value={num(data.kpis.vision_pm_mean)} />
              <Card title="Gold / Min" value={num(data.kpis.gold_per_min_mean)} />
              <Card title="Damage / Min" value={num(data.kpis.dmg_per_min_mean)} />
              <Card title="Obj Dmg / Min" value={num(data.kpis.objective_damage_pm_mean)} />
              <Card title="First Blood Rate" value={pct(data.kpis.first_blood_rate_self)} />
              <Card title="Avg Game Time" value={`${num(data.kpis.avg_game_time_min)} min`} />
            </div>

            {/* Distributions */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <ListCard title="Top Champions" rows={data.kpis.top_champions.map(c => ({
                label: c.name, right: `${c.games} games`
              }))} />
              <ListCard title="Role Distribution" rows={data.kpis.role_distribution.map(r => ({
                label: r.role, right: `${r.games} games`
              }))} />
            </div>

            {/* Items / Duos */}
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <ListCard title="Favorite Items (by count)" rows={(data.kpis.favorite_items || []).map(i => ({
                label: `Item ${i.itemId}`, right: `${i.games} games`
              }))} />
              <div className="grid grid-cols-1 gap-4">
                <ListCard title="Best Items (by winrate, ≥10 games)" rows={(data.kpis.best_items || []).map(i => ({
                  label: `Item ${i.itemId}`, right: `${pct(i.winrate)} · ${i.games}g`
                }))} />
                <ListCard title="Worst Items (by winrate, ≥10 games)" rows={(data.kpis.worst_items || []).map(i => ({
                  label: `Item ${i.itemId}`, right: `${pct(i.winrate)} · ${i.games}g`
                }))} />
              </div>
            </div>

            {/* Duos */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <DuoCard title="Most Played Duo" duo={data.kpis.duo_most_played} />
              <DuoCard title="Best Duo (≥5g)" duo={data.kpis.duo_best} />
              <DuoCard title="Worst Duo (≥5g)" duo={data.kpis.duo_worst} />
            </div>

            {/* Queue splits */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <Card title="Solo/Duo" value={`${pct(data.kpis.split_ranked_solo_duo?.winrate)} · ${data.kpis.split_ranked_solo_duo?.games || 0}g`} />
              <Card title="Flex" value={`${pct(data.kpis.split_ranked_flex?.winrate)} · ${data.kpis.split_ranked_flex?.games || 0}g`} />
              <Card title="Normals" value={`${pct(data.kpis.split_normals?.winrate)} · ${data.kpis.split_normals?.games || 0}g`} />
            </div>

            {/* Champion winrates */}
            {data.kpis.champion_winrates && (
              <ListCard
                title="Champion Winrates (top 20)"
                rows={data.kpis.champion_winrates.map(c => ({
                  label: `${c.name} — ${pct(c.winrate)} (${c.wins}/${c.games})`,
                  right: c.winrate_when_fb_self != null
                    ? `FB-self: ${pct(c.winrate_when_fb_self)}`
                    : ''
                }))}
              />
            )}
          </section>
        )}
      </div>
    </main>
  );
}

function Card({ title, value }: { title: string; value: string }) {
  return (
    <div className="rounded-2xl bg-slate-900/60 p-4 ring-1 ring-slate-800">
      <p className="text-slate-400 text-sm">{title}</p>
      <p className="mt-1 text-2xl font-semibold">{value}</p>
    </div>
  );
}

function ListCard({ title, rows }: { title: string; rows: { label: string; right?: string }[] }) {
  return (
    <div className="rounded-2xl bg-slate-900/60 p-4 ring-1 ring-slate-800">
      <p className="text-slate-400 text-sm mb-2">{title}</p>
      <ul className="divide-y divide-slate-800">
        {rows.map((r, i) => (
          <li key={i} className="flex items-center justify-between py-2">
            <span className="truncate pr-4">{r.label}</span>
            {r.right && <span className="text-slate-400 text-sm">{r.right}</span>}
          </li>
        ))}
      </ul>
    </div>
  );
}

function DuoCard({ title, duo }: { title: string; duo: any }) {
  return (
    <div className="rounded-2xl bg-slate-900/60 p-4 ring-1 ring-slate-800">
      <p className="text-slate-400 text-sm">{title}</p>
      {duo ? (
        <div className="mt-2">
          <p className="font-semibold">{duo.name}</p>
          <p className="text-slate-400 text-sm">{duo.games} games · {Math.round((duo.winrate || 0) * 100)}% WR</p>
        </div>
      ) : (
        <p className="mt-2 text-slate-400 text-sm">Not enough data</p>
      )}
    </div>
  );
}
