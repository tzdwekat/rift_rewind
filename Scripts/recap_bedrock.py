# scripts/recap_bedrock.py
import sys, os, json, boto3

# allow importing sibling files if needed
sys.path.append(os.path.join(os.path.dirname(__file__)))

from dotenv import load_dotenv
load_dotenv("secrets/.env")

S3_BUCKET      = os.getenv("S3_BUCKET")
BEDROCK_REGION = os.getenv("BEDROCK_REGION", "us-east-1")
MODEL_ID       = os.getenv("MODEL_ID", "anthropic.claude-3-5-sonnet-20240620")

s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))
br = boto3.client("bedrock-runtime", region_name=BEDROCK_REGION)

SYSTEM_PROMPT = (
    "You are a concise League analyst. Output STRICT JSON with keys: "
    "title (string), summary (<=80 words), strengths (array of strings), "
    "improvements (array of strings), awards (array of objects {name, reason}). "
    "Use only provided stats; be specific to role/champ context."
)

def get_kpis_from_s3(puuid: str, year: str) -> dict:
    key = f"kpis/{puuid}/{year}.json"
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())

def put_recap_to_s3(puuid: str, year: str, recap: dict) -> str:
    key = f"recaps/{puuid}/{year}.json"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json.dumps(recap).encode("utf-8"),
                  ContentType="application/json")
    return key

def call_bedrock(kpis_doc: dict) -> dict:
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": SYSTEM_PROMPT,
        "max_tokens": 600,
        "temperature": 0.2,
        "messages": [
            {"role": "user", "content": [
                {"type": "text",
                 "text": "Given this KPIs JSON, produce the recap JSON.\n\n" + json.dumps(kpis_doc)}
            ]}
        ]
    }
    resp = br.invoke_model(
        modelId=MODEL_ID,
        accept="application/json",
        contentType="application/json",
        body=json.dumps(body),
    )
    payload = json.loads(resp["body"].read())
    # Claude-style response: extract text and parse JSON
    txt = "".join(c.get("text","") for c in payload.get("content", []) if c.get("type")=="text")
    try:
        return json.loads(txt)
    except Exception:
        # fallback: wrap raw text
        return {"title": "Your Year in LoL", "summary": txt, "strengths": [], "improvements": [], "awards": []}

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("usage: python scripts/recap_bedrock.py <PUUID> <YEAR>")
        sys.exit(1)

    puuid, year = sys.argv[1], sys.argv[2]
    kpis_doc = get_kpis_from_s3(puuid, year)   # expects you already ran kpis_basic to write this
    recap = call_bedrock(kpis_doc)
    out_key = put_recap_to_s3(puuid, year, recap)
    print(f"✅ Recap saved → s3://{S3_BUCKET}/{out_key}")
    print(json.dumps(recap, indent=2))
