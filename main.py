import os
import re
import json
from typing import Any, Dict, List, Tuple
from decimal import Decimal
from datetime import date, datetime

import httpx
import psycopg
from psycopg.rows import dict_row
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field

# -----------------------------
# Config (env)
# -----------------------------
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:120b-cloud")

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB = os.getenv("PG_DB", "Cars")
PG_USER = os.getenv("PG_USER", "demirai_ro")
PG_PASSWORD = os.getenv("PG_PASSWORD", "Demirkan")

STATEMENT_TIMEOUT_MS = int(os.getenv("STATEMENT_TIMEOUT_MS", "3000"))
MAX_ROWS = int(os.getenv("MAX_ROWS", "2000"))

# -----------------------------
# DemirAI System Prompt (SQL gen)
# -----------------------------
SQL_SYSTEM_PROMPT = f"""
You are DemirAI, a data analysis assistant.

LANGUAGE
- Reply ONLY in Turkish.
- Be concise, analytical, professional.

DATABASE ACCESS RULES (ABSOLUTE)
- You can ONLY query tables under schema: mart.
- Allowed tables:
  - mart.fact_listings
  - mart.dim_vehicle
  - mart.dim_seller
  - mart.dim_time
- You can ONLY generate a SINGLE SQL SELECT statement.
- NEVER use INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE.
- NEVER access raw or stg schemas.
- ALWAYS include a LIMIT (max {MAX_ROWS}).

SCHEMA OVERVIEW
mart.fact_listings:
- listing_id (pk)
- vehicle_id (fk)
- seller_id (fk)
- time_id (fk)
- price
- price_drop
- mileage
- mpg
- driver_rating
- driver_reviews_num
- accidents_or_damage (boolean)
- one_owner (boolean)
- personal_use_only (boolean)

mart.dim_vehicle:
- vehicle_id (pk)
- manufacturer
- model
- year
- engine
- transmission
- drivetrain
- fuel_type
- exterior_color
- interior_color
- transmission_norm   -- canonical: automatic | manual | other
- fuel_type_norm      -- canonical: gasoline | diesel | hybrid | electric | other

mart.dim_time:
- time_id (pk)
- year

mart.dim_seller:
- seller_id (pk)
- seller_name
- seller_rating

JOIN RULES
- fact_listings.vehicle_id = dim_vehicle.vehicle_id
- fact_listings.time_id = dim_time.time_id
- fact_listings.seller_id = dim_seller.seller_id

IMPORTANT FILTERING RULES
- When filtering by transmission or fuel, ALWAYS use:
  - dim_vehicle.transmission_norm
  - dim_vehicle.fuel_type_norm
- NEVER compare dim_vehicle.transmission or dim_vehicle.fuel_type directly to literals like 'automatic' or 'gasoline'.

OUTPUT (STRICT)
Return ONLY a JSON object with keys:
- "sql": the SQL string
No extra text.
""".strip()

# -----------------------------
# DemirAI System Prompt (Insight)
# -----------------------------
INSIGHT_SYSTEM_PROMPT = """
You are DemirAI. You will be given:
- user question
- executed SQL
- query result preview (rows + columns) and basic stats

TASK
- Explain the result in Turkish, concise and insightful.
- Do NOT invent data not present in the result.
- If result is empty, explain possible reasons and propose next query.

OUTPUT (STRICT)
Return ONLY a JSON object with keys:
- "tldr": string (2-3 sentences)
- "findings": array of strings (3-8 bullets)
- "recommendations": array of strings (3 bullets)
No extra text.
""".strip()

# -----------------------------
# FastAPI
# -----------------------------
app = FastAPI(title="DemirAI v2", version="2.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
STATIC_DIR = os.path.join(BASE_DIR, "static")

os.makedirs(TEMPLATES_DIR, exist_ok=True)
os.makedirs(STATIC_DIR, exist_ok=True)

templates = Jinja2Templates(directory=TEMPLATES_DIR)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


class AskRequest(BaseModel):
    question: str = Field(..., min_length=3)


class AskResponse(BaseModel):
    sql: str
    tldr: str
    findings: List[str]
    recommendations: List[str]
    row_count: int
    columns: List[str]


# -----------------------------
# Helpers
# -----------------------------
BANNED = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|vacuum|copy)\b",
    re.IGNORECASE,
)


def _single_statement(sql: str) -> bool:
    parts = [p.strip() for p in sql.strip().split(";") if p.strip()]
    return len(parts) == 1


def validate_sql(sql: str) -> str:
    if not sql or not isinstance(sql, str):
        raise HTTPException(400, "SQL üretilemedi.")

    s = sql.strip()

    if not re.match(r"^(select|with)\b", s, re.IGNORECASE):
        raise HTTPException(400, "Sadece SELECT sorgularına izin veriliyor.")

    if BANNED.search(s):
        raise HTTPException(400, "Yasak SQL anahtar kelimesi tespit edildi.")

    if not _single_statement(s):
        raise HTTPException(400, "Tek bir SQL statement olmalı.")

    lowered = s.lower()
    if (" from " in lowered or "\nfrom " in lowered or "\tfrom " in lowered) and ("mart." not in lowered):
        raise HTTPException(400, "Sadece mart şemasına erişim var (mart.*).")

    if re.search(r"\blimit\b", lowered) is None:
        raise HTTPException(400, f"LIMIT zorunlu (maks {MAX_ROWS}).")

    m = re.search(r"\blimit\s+(\d+)\b", lowered)
    if m:
        lim = int(m.group(1))
        if lim > MAX_ROWS:
            s = re.sub(r"(?i)\blimit\s+\d+\b", f"LIMIT {MAX_ROWS}", s)

    return s


def to_jsonable(x: Any) -> Any:
    """Convert psycopg row values (e.g., Decimal) into JSON-serializable values."""
    if isinstance(x, Decimal):
        return float(x)
    if isinstance(x, (datetime, date)):
        return x.isoformat()
    if isinstance(x, dict):
        return {k: to_jsonable(v) for k, v in x.items()}
    if isinstance(x, list):
        return [to_jsonable(v) for v in x]
    if isinstance(x, tuple):
        return [to_jsonable(v) for v in x]
    return x


def _extract_json(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("{") and text.endswith("}"):
        return text
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    return m.group(0) if m else text


async def ollama_json(system: str, user: str) -> Dict[str, Any]:
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": user,
        "system": system,
        "stream": False,
        "format": "json",
        "options": {"temperature": 0.2},
    }
    async with httpx.AsyncClient(timeout=120) as client:
        r = await client.post(OLLAMA_URL, json=payload)
        if r.status_code != 200:
            raise HTTPException(502, f"Ollama hata: {r.status_code} {r.text}")
        data = r.json()

    raw = _extract_json(data.get("response", ""))
    try:
        return json.loads(raw)
    except Exception:
        raise HTTPException(502, f"Ollama JSON parse edilemedi: {raw[:300]}")


def pg_conn():
    return psycopg.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        row_factory=dict_row,
        connect_timeout=5,
        options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} -c default_transaction_read_only=on",
    )


def run_query(sql: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    with pg_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchmany(50)  # preview only
            cols = [desc.name for desc in cur.description] if cur.description else []
    # Convert Decimal and other non-JSON types to JSON-safe types
    rows_jsonable = to_jsonable(rows)
    return rows_jsonable, cols


def summarize_rows(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"preview_row_count": 0, "nulls": {}}

    cols = rows[0].keys()
    nulls = {c: 0 for c in cols}
    for r in rows:
        for c in cols:
            if r.get(c) is None:
                nulls[c] += 1
    return {"preview_row_count": len(rows), "nulls": nulls}


# -----------------------------
# Web UI routes
# -----------------------------
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    try:
        return templates.TemplateResponse("chat.html", {"request": request})
    except Exception as e:
        return HTMLResponse(f"<h1>Template Error</h1><pre>{str(e)}</pre>", status_code=500)


@app.get("/chat", response_class=HTMLResponse)
async def chat(request: Request):
    try:
        return templates.TemplateResponse("chat.html", {"request": request})
    except Exception as e:
        return HTMLResponse(f"<h1>Template Error</h1><pre>{str(e)}</pre>", status_code=500)


# -----------------------------
# API Endpoint
# -----------------------------
@app.post("/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    try:
        question = req.question.strip()

        # 1) NL -> SQL
        sql_obj = await ollama_json(
            SQL_SYSTEM_PROMPT,
            f"Kullanıcı sorusu: {question}\n\nKurallara uygun tek bir SQL üret.",
        )
        sql = sql_obj.get("sql")
        if not sql:
            raise HTTPException(400, "Model SQL döndürmedi.")

        sql = validate_sql(sql)

        # 2) Execute
        try:
            rows, cols = run_query(sql)  # rows already converted to JSON-safe format
        except psycopg.Error as e:
            raise HTTPException(400, f"SQL çalıştırma hatası: {str(e).splitlines()[-1]}")

        stats = summarize_rows(rows)
        stats_jsonable = to_jsonable(stats)

        # 3) Result -> Insight
        try:
            insight_payload = {
                "question": question,
                "sql": sql,
                "columns": cols,
                "preview_rows": rows,  # Already JSON-safe from run_query
                "stats": stats_jsonable,
                "notes": {
                    "preview_limit": 50,
                    "rule": "Only preview rows are shown; do not invent missing aggregates.",
                },
            }
            insight_payload_json = json.dumps(insight_payload, ensure_ascii=False)
        except Exception as e:
            raise HTTPException(500, f"JSON serialization hatası: {str(e)}")

        insight_obj = await ollama_json(
            INSIGHT_SYSTEM_PROMPT,
            insight_payload_json,
        )

        tldr = insight_obj.get("tldr", "")
        findings = insight_obj.get("findings", [])
        recs = insight_obj.get("recommendations", [])

        if not isinstance(findings, list):
            findings = [str(findings)]
        if not isinstance(recs, list):
            recs = [str(recs)]

        return AskResponse(
            sql=sql,
            tldr=str(tldr),
            findings=[str(x) for x in findings][:8],
            recommendations=[str(x) for x in recs][:5],
            row_count=stats.get("preview_row_count", 0),
            columns=cols,
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(500, f"Internal error: {error_detail[:500]}")
