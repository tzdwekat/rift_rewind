import { NextResponse } from 'next/server';
import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { spawn } from 'child_process';

const PLATFORM_TO_CLUSTER: Record<string, string> = {
  na: 'americas', na1: 'americas', br: 'americas', br1: 'americas', lan: 'americas', la1: 'americas', las: 'americas', la2: 'americas',
  euw: 'europe', euw1: 'europe', eune: 'europe', eun1: 'europe', tr: 'europe', tr1: 'europe', ru: 'europe',
  kr: 'asia', jp: 'asia', jp1: 'asia',
  oce: 'sea', oc1: 'sea', ph: 'sea', ph2: 'sea', sg: 'sea', sg2: 'sea', th: 'sea', th2: 'sea', tw: 'sea', tw2: 'sea', vn: 'sea', vn2: 'sea'
};

const s3 = new S3Client({ region: process.env.AWS_REGION });

function resolveCluster(region: string) {
  return PLATFORM_TO_CLUSTER[region.toLowerCase()] || 'americas';
}

async function resolvePuuid(riotId: string, region: string): Promise<string> {
  if (!process.env.RIOT_API_KEY) throw new Error('RIOT_API_KEY missing');
  if (!riotId.includes('#')) throw new Error('Riot ID must be GameName#TAG');
  const [game, tag] = riotId.split('#');
  const cluster = resolveCluster(region);
  const url = `https://${cluster}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/${encodeURIComponent(game)}/${encodeURIComponent(tag)}`;
  const r = await fetch(url, { headers: { 'X-Riot-Token': process.env.RIOT_API_KEY! } });
  if (!r.ok) throw new Error(`Riot resolve failed: ${r.status}`);
  const j = await r.json();
  return j.puuid;
}

function runPython(args: string[], cwd?: string) {
  return new Promise<{ stdout: string; stderr: string }>((resolve, reject) => {
    const py = spawn('python', args, { cwd, env: process.env });
    let stdout = '', stderr = '';
    py.stdout.on('data', (d) => (stdout += d.toString()));
    py.stderr.on('data', (d) => (stderr += d.toString()));
    py.on('close', (code) => {
      if (code === 0) resolve({ stdout, stderr });
      else reject(new Error(stderr || `python exited with ${code}`));
    });
  });
}

async function getKpisFromS3(puuid: string, year: string) {
  const bucket = process.env.AWS_S3_BUCKET!;
  const key = `kpis/${puuid}/${year}.json`;
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key });
  const out = await s3.send(cmd);
  const body = await out.Body?.transformToString();
  if (!body) throw new Error('Empty S3 body');
  return JSON.parse(body);
}

export async function POST(req: Request) {
  try {
    const { riotId, region, year, limit } = await req.json();

    if (!riotId || !region || !year) {
      return NextResponse.json({ error: 'riotId, region, year required' }, { status: 400 });
    }

    const puuid = await resolvePuuid(riotId, region);

    // 1) Run fetcher (headless) — upload matches (and optionally timelines)
    const fetchArgs = [
      'scripts/fetch_riot.py',
      '--riot-id', riotId,
      '--region', region,
      '--year', String(year),
      '--upload-matches',
      // Uncomment if you want timelines as part of this call:
      // '--upload-timelines',
    ];
    if (limit && Number(limit) > 0) {
      fetchArgs.push('--limit', String(limit));
    }
    const fetched = await runPython(fetchArgs, process.cwd());
    // (optional) parse out PUUID=... from stdout if you need to sanity check

    // 2) Run KPI job
    const kpiArgs = ['scripts/kpis_basic.py', puuid, String(year)];
    if (limit && Number(limit) > 0) kpiArgs.push(String(limit));
    const kpiRun = await runPython(kpiArgs, process.cwd());

    // 3) Read KPIs from S3
    const kpisDoc = await getKpisFromS3(puuid, String(year));
    return NextResponse.json({ puuid, year: String(year), kpis: kpisDoc.kpis }, { status: 200 });

  } catch (e: any) {
    return new NextResponse(e?.message || 'Failed', { status: 500 });
  }
}
