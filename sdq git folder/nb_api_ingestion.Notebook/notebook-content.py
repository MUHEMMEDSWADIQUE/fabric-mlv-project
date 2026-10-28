# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b02fd652-ec5b-4597-88c7-25b6779922a6",
# META       "default_lakehouse_name": "lh_github",
# META       "default_lakehouse_workspace_id": "66e95359-350d-4abd-9b10-110ec6cfd52b",
# META       "known_lakehouses": [
# META         {
# META           "id": "b02fd652-ec5b-4597-88c7-25b6779922a6"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import requests,time,json

BASE = "https://api.github.com"
TOKEN = ""#you have to provide one pat token here, if we give a pat token physically here , some issues coming while commiting so i removed the token from here

headers = {
    "Accept": "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "Authorization": f"Bearer {TOKEN}",
}

r = requests.get(f"{BASE}/repos/apache/airflow/issues",headers=headers,params={"state":"all","per_page":5},timeout=(5,30))

print(r.status_code)
print("Link",r.headers.get("Retry-After"))
print("Link",r.links.get("next"))
print("Link",r.links)
print({k:v for k,v in r.headers.items() if k.lower().startswith("x-ratelimit")})
print(json.dumps(r.json()[0],indent=2)[:1500])


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def paginate(session,url,params=None,max_pages=10_000):
    seen , page = set() ,0
    while url and page < max_pages:
        r = get_json(session,url,params)
        params = None
        yield page , r.json()
        page += 1
        url = r.links.get("next",{}).get("url")
        if url:
            if url in seen :
                raise RuntimeError(f"pagination loop ditected at {url}")
            seen.add(url)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import random

def make_session():
    s=requests.session()
    s.headers.update(headers)
    return s

def _backoff(attempt):
    time.sleep(min(60,2**attempt) + random(0,1))


def _rate_limit_wait(r):
    if "Retry-After" in r.headers:
        return int(r.headers["Retry-After"])
    if r.headers.get("X-RateLimit-Remaining") == "0":
        return max(int(r.headers["X-RateLimit-Reset"]) - time.time(),0)
    return None



def get_json(s,url,params=None,extra_headers=None,max_retries=6,max_wait=900):
    for attempt in range(max_retries+1):
        try:
            r=s.get(url,headers=extra_headers,timeout=(5,60))
        except (requests.ConnectionError,requests.Timeout):
            if attempt == max_retries: raise
            _backoff(attempt);continue
        
        sc = r.status_code

        if sc in (200,304):
            if int(r.headers.get("X-RateLimit-Remaining",99)) <=2:
                time.sleep(min(_rate_limit_wait(r),max_wait)+1)
            return r
        if sc in (403,429):
            wait=_rate_limit_wait(r)
            if wait is None : r.raise_for_status()
            if wait > max_wait : raise RuntimeError(f"Rate Limit Wait {wait}s too long , aborting")
            time.sleep(wait+1);continue
        if sc > 500:
            if attempt == max_retries: r.raise_for_status()
            _backoff(attempt);continue
        r.raise_for_status()             

    raise RuntimeError("Retries Exhuasted")    





# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# import requests
# s = make_session()
# for u in ["https://httpbin.org/status/503", "https://httpbin.org/status/404",
#           "https://httpbin.org/delay/10"]:
#     try: get_json(s, u, max_retries=2)
#     except Exception as e: print(u, "→", type(e).__name__, e)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os, gzip, uuid
from datetime import datetime, timedelta, timezone

RAW_ROOT = "/lakehouse/default/Files/raw/github"

def iso(d):   return d.strftime("%Y-%m-%dT%H:%M:%SZ")
def parse(t): return datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)

def ingest_issues(repo, since, run_id):
    s = make_session()
    out_dir = f"{RAW_ROOT}/issues/repo={repo.replace('/', '__')}/run_id={run_id}"
    os.makedirs(out_dir, exist_ok=True)
    params = {"state": "all", "sort": "updated", "direction": "asc",
              "since": since, "per_page": 100}
    n, max_ts = 0, since
    for i, page in paginate(s, f"{BASE}/repos/{repo}/issues", params):
        if not page: break
        with gzip.open(f"{out_dir}/page_{i:05d}.json.gz", "wt", encoding="utf-8") as f:
            json.dump(page, f)
        n += len(page)
        max_ts = max(max_ts, max(x["updated_at"] for x in page))   # ISO strings sort correctly
    return n, max_ts

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

def merge_silver(run_id):
    raw = (spark.read.option("multiLine", True)
           .json(f"Files/raw/github/issues/repo=*/run_id={run_id}/*.json.gz"))

    df = (raw.withColumn("_src", F.input_file_name())
        .select(
            F.col("id").cast("long").alias("issue_id"),
            "number", "title", "state",
            F.col("user.login").alias("author"),
            F.col("comments").alias("comment_count"),
            F.col("pull_request").isNotNull().alias("is_pr"),
            F.to_timestamp("created_at").alias("created_at"),
            F.to_timestamp("updated_at").alias("updated_at"),
            F.to_timestamp("closed_at").alias("closed_at"),
            F.regexp_extract("_src", r"repo=([^/]+)/", 1).alias("repo"),
            F.current_timestamp().alias("_ingested_at")))

    # dedupe inside the batch (lookback windows re-pull rows)
    w = Window.partitionBy("issue_id").orderBy(F.col("updated_at").desc())
    df = df.withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn")

    if not spark.catalog.tableExists("silver_issues"):
        df.write.format("delta").saveAsTable("silver_issues")
        return
    (DeltaTable.forName(spark, "silver_issues").alias("t")
        .merge(df.alias("s"), "t.issue_id = s.issue_id")
        .whenMatchedUpdateAll(condition="s.updated_at > t.updated_at")
        .whenNotMatchedInsertAll()
        .execute())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""CREATE TABLE IF NOT EXISTS etl_watermark
             (source STRING, entity STRING, watermark STRING) USING DELTA""")

def get_wm(entity):
    rows = spark.sql(f"SELECT watermark FROM etl_watermark WHERE entity = '{entity}'").collect()
    return rows[0][0] if rows else None

def set_wm(entity, value):
    spark.sql(f"""MERGE INTO etl_watermark t
        USING (SELECT 'github' AS source, '{entity}' AS entity, '{value}' AS watermark) s
        ON t.entity = s.entity
        WHEN MATCHED THEN UPDATE SET t.watermark = s.watermark
        WHEN NOT MATCHED THEN INSERT *""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- parameters (mark this cell as a Parameters cell, see Phase 8) ---
REPOS = ["apache/airflow", "apache/spark"]
LOOKBACK_HOURS = 2
DEFAULT_START = "2026-10-01T00:00:00Z"   # keep the first load small
FULL_REFRESH = False

from concurrent.futures import ThreadPoolExecutor
run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:6]

def start_for(repo):
    wm = None if FULL_REFRESH else get_wm(f"issues:{repo}")
    return iso(parse(wm) - timedelta(hours=LOOKBACK_HOURS)) if wm else DEFAULT_START

starts = {r: start_for(r) for r in REPOS}

with ThreadPoolExecutor(max_workers=3) as ex:      # keep this small: secondary limits
    results = dict(zip(REPOS, ex.map(lambda r: ingest_issues(r, starts[r], run_id), REPOS)))

if any(n for n, _ in results.values()):
    merge_silver(run_id)                            # 1) data first
    for repo, (n, max_ts) in results.items():       # 2) watermark only after success
        if n: set_wm(f"issues:{repo}", max_ts)

print(results)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

s = make_session()
r1 = get_json(s, f"{BASE}/repos/apache/airflow")
etag = r1.headers["ETag"]
r2 = get_json(s, f"{BASE}/repos/apache/airflow", extra_headers={"If-None-Match": etag})
print(r2.status_code, len(r2.content))   # expect 304 and an empty body

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
