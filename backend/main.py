"""
FastAPI backend for Order to Cash Explorer.
Production-ready with auto-ingest, CORS, exception handling, and health checks.
"""

import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from graph_model import build_graph, GRAPH_SCHEMA
from llm import answer_query

load_dotenv()

app = FastAPI(title="Order to Cash Explorer API")

# CORS - allow all origins for now (will tighten after getting Vercel URL)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths relative to this file
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "o2c.db"
DATA_DIR = BASE_DIR / "data" / "sap-o2c-data"

# Module-level cache for fast responses
_graph_cache = None
_stats_cache = None


# =============================================================================
# GLOBAL EXCEPTION HANDLER
# =============================================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch all unhandled exceptions and return proper JSON error."""
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "detail": str(exc),
            "path": str(request.url.path),
        },
    )


# =============================================================================
# STARTUP
# =============================================================================

def run_ingest():
    """Run the ingest script to build the database."""
    print("[startup] Running ingest.py to build database...")
    ingest_script = BASE_DIR / "ingest.py"
    result = subprocess.run(
        [sys.executable, str(ingest_script)],
        cwd=str(BASE_DIR),
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"[startup] Ingest failed: {result.stderr}")
        raise RuntimeError(f"Ingest failed: {result.stderr}")
    print("[startup] Ingest completed successfully")


@app.on_event("startup")
async def startup():
    """Check for database and run ingest if needed, then warm cache."""
    # Check if database exists
    if not DB_PATH.exists():
        print(f"[startup] Database not found at {DB_PATH}")
        if DATA_DIR.exists():
            run_ingest()
        else:
            print(f"[startup] WARNING: Data directory not found at {DATA_DIR}")
            print("[startup] Database will not be available until data is provided")
            return

    # Warm the cache
    if DB_PATH.exists():
        print("[startup] Warming graph cache...")
        try:
            get_graph()
            get_stats()
            print("[startup] Cache warmed successfully")
        except Exception as e:
            print(f"[startup] Cache warming failed: {e}")


# =============================================================================
# CACHE HELPERS
# =============================================================================

def get_graph():
    """Get cached graph, building it only once."""
    global _graph_cache
    if _graph_cache is None:
        if not DB_PATH.exists():
            raise HTTPException(status_code=503, detail="Database not available")
        _graph_cache = build_graph(DB_PATH)
    return _graph_cache


def get_stats():
    """Get cached stats."""
    global _stats_cache
    if _stats_cache is None:
        graph = get_graph()
        node_counts = Counter(n["type"] for n in graph["nodes"])
        _stats_cache = {
            "total_nodes": len(graph["nodes"]),
            "total_edges": len(graph["edges"]),
            "entity_types": len(node_counts),
            "by_type": dict(node_counts),
        }
    return _stats_cache


# =============================================================================
# REQUEST MODELS
# =============================================================================

class ChatRequest(BaseModel):
    message: str
    history: list = []


# =============================================================================
# API ROUTES
# =============================================================================

@app.get("/")
def root():
    """Root endpoint."""
    return {"message": "Order to Cash Explorer API", "status": "running"}


@app.get("/api/graph")
def api_get_graph():
    """Return full graph with nodes and edges. Cached in memory."""
    try:
        return get_graph()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load graph: {str(e)}")


@app.get("/api/node/{node_id}")
def get_node(node_id: str):
    """Return full metadata for a single node by its ID."""
    try:
        graph = get_graph()
        for node in graph["nodes"]:
            if node["id"] == node_id:
                return node
        raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get node: {str(e)}")


@app.get("/api/stats")
def api_stats():
    """Return graph statistics for the frontend stats bar."""
    try:
        return get_stats()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get stats: {str(e)}")


@app.post("/api/chat")
def chat(request: ChatRequest):
    """Chat endpoint for LLM-powered Q&A about the O2C dataset."""
    try:
        # Build conversation memory: last 4 message pairs (8 messages)
        history = request.history[-8:] if len(request.history) > 8 else request.history
        result = answer_query(request.message, history, DB_PATH)
        return result
    except Exception as e:
        return {
            "response": f"Sorry, I encountered an error: {str(e)}",
            "query_used": None,
            "rows_returned": None,
        }


@app.get("/api/health")
def health():
    """Health check with detailed status, table info, and timestamp."""
    try:
        if not DB_PATH.exists():
            return {
                "status": "degraded",
                "message": "Database not available",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "tables": [],
            }

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        tables = []
        total_rows = 0
        for node_type, schema in GRAPH_SCHEMA["nodes"].items():
            table_name = schema["table"]
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                tables.append({"table": table_name, "rows": count})
                total_rows += count
            except Exception as e:
                tables.append({"table": table_name, "rows": 0, "error": str(e)})

        conn.close()

        return {
            "status": "ok",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": str(DB_PATH),
            "total_tables": len(tables),
            "total_rows": total_rows,
            "tables": tables,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tables": [],
        }
