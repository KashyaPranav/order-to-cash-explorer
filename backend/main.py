import sqlite3
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from graph_model import build_graph, GRAPH_SCHEMA
from llm import answer_query

load_dotenv()

app = FastAPI(title="Order to Cash Explorer API")

# CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_PATH = Path(__file__).parent / "data" / "o2c.db"

# Module-level cache for fast startup
_graph_cache = None
_stats_cache = None


def get_graph():
    """Get cached graph, building it only once."""
    global _graph_cache
    if _graph_cache is None:
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


class ChatRequest(BaseModel):
    message: str
    history: list = []


@app.on_event("startup")
async def warmup_cache():
    """Pre-build graph cache on startup for fast first request."""
    get_graph()
    get_stats()


@app.get("/")
def root():
    return {"message": "Order to Cash Explorer API"}


@app.get("/api/graph")
def api_get_graph():
    """Return full graph with nodes and edges. Cached in memory."""
    return get_graph()


@app.get("/api/node/{node_id}")
def get_node(node_id: str):
    """Return full metadata for a single node by its ID."""
    graph = get_graph()
    for node in graph["nodes"]:
        if node["id"] == node_id:
            return node
    raise HTTPException(status_code=404, detail=f"Node not found: {node_id}")


@app.get("/api/stats")
def api_stats():
    """Return graph statistics for the frontend stats bar."""
    return get_stats()


@app.post("/api/chat")
def chat(request: ChatRequest):
    """Chat endpoint for LLM-powered Q&A about the O2C dataset."""
    # Build conversation memory: last 4 message pairs (8 messages)
    history = request.history[-8:] if len(request.history) > 8 else request.history

    result = answer_query(request.message, history, DB_PATH)
    return result


@app.get("/api/health")
def health():
    """Health check with table stats."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    tables = []
    for node_type, schema in GRAPH_SCHEMA["nodes"].items():
        table_name = schema["table"]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        tables.append({"table": table_name, "rows": count})

    conn.close()

    return {
        "status": "ok",
        "tables": tables,
    }
