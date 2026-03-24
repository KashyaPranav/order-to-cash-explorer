# Order to Cash Explorer

A visual analytics tool for exploring SAP Order-to-Cash (O2C) datasets. It renders business entities as an interactive graph and provides natural language querying via LLM, allowing users to ask questions like "Show me the top 5 customers by order value" and get instant answers backed by real SQL queries.

Built with FastAPI, React, and Groq's LLaMA 3.3 70B model.

![Screenshot](docs/screenshot.png)

---

## Quick Start

### Prerequisites

- Python 3.11+
- Node.js 18+
- Groq API key ([get one free](https://console.groq.com/))

### 1. Clone and setup backend

```bash
cd backend

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Add your Groq API key
echo "GROQ_API_KEY=your_key_here" > .env

# Ingest the dataset (if not already done)
python ingest.py
```

### 2. Start the backend

```bash
cd backend
source venv/bin/activate
uvicorn main:app --reload --port 8000
```

### 3. Setup and start frontend

```bash
cd frontend
npm install
npm run dev
```

### 4. Open the app

Navigate to [http://localhost:5173](http://localhost:5173)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Frontend                                   │
│                         React + Vite (5173)                             │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  ┌──────────────────────┐    ┌────────────────────────────────┐ │   │
│  │  │                      │    │  Side Panel                    │ │   │
│  │  │   Force-Directed     │    │  ┌──────────────────────────┐  │ │   │
│  │  │   Graph              │    │  │ Node Details             │  │ │   │
│  │  │   (react-force-      │    │  │ - Entity type & ID       │  │ │   │
│  │  │    graph-2d)         │    │  │ - All field values       │  │ │   │
│  │  │                      │    │  └──────────────────────────┘  │ │   │
│  │  │   - 1200+ nodes      │    │  ┌──────────────────────────┐  │ │   │
│  │  │   - 700+ edges       │    │  │ Chat Interface           │  │ │   │
│  │  │   - Click to select  │    │  │ - Natural language Q&A   │  │ │   │
│  │  │   - Search & filter  │    │  │ - Entity highlighting    │  │ │   │
│  │  │                      │    │  └──────────────────────────┘  │ │   │
│  │  └──────────────────────┘    └────────────────────────────────┘ │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │ HTTP API
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              Backend                                    │
│                         FastAPI (8000)                                  │
│                                                                         │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
│  │ /api/graph      │  │ /api/chat        │  │ /api/stats            │  │
│  │ Returns cached  │  │ LLM-powered Q&A  │  │ Entity counts         │  │
│  │ nodes + edges   │  │ with guardrails  │  │ for stats bar         │  │
│  └────────┬────────┘  └────────┬─────────┘  └───────────────────────┘  │
│           │                    │                                        │
│           ▼                    ▼                                        │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      graph_model.py                              │   │
│  │  - GRAPH_SCHEMA defines nodes (11 types) and edges (8 relations) │   │
│  │  - build_graph() constructs full graph from SQLite               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                          llm.py                                  │   │
│  │  1. Guardrail check (topic + rate limit)                         │   │
│  │  2. SQL generation via Groq LLaMA 3.3 70B                        │   │
│  │  3. SQL safety validation (read-only)                            │   │
│  │  4. Query execution with retry                                   │   │
│  │  5. Natural language answer generation                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                     │                                   │
│                                     ▼                                   │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         o2c.db (SQLite)                          │   │
│  │  19 tables: sales_order_headers, billing_document_items, etc.    │   │
│  │  ~21,000 rows ingested from JSONL files                          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           Groq API                                      │
│                      LLaMA 3.3 70B Versatile                            │
│  - SQL generation from natural language                                 │
│  - Answer synthesis from query results                                  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## LLM Strategy

The chat interface uses a two-stage LLM approach for answering questions. First, the user's natural language question is sent to LLaMA 3.3 70B with the full database schema as context, asking it to generate a SQLite query. The model receives conversation history (last 4 exchanges) to handle follow-up questions like "now show me just the ones from last month." Once the SQL is generated and validated, it executes against the local SQLite database. The results are then sent back to the LLM with the original question, asking for a concise natural language summary. This two-stage approach means the LLM never sees raw data during generation—it only writes SQL—and can provide accurate answers grounded in actual query results.

---

## Guardrails

The system implements multiple layers of protection to keep interactions safe and on-topic. **Topic filtering** rejects questions that don't contain domain-relevant keywords (orders, billing, customers, etc.) and explicitly blocks patterns like "write me a poem" or "what's the weather." **SQL safety validation** scans generated queries for dangerous keywords (DROP, DELETE, INSERT, UPDATE, ALTER) and rejects anything that isn't purely a SELECT statement. **Rate limiting** caps usage at 10 requests per minute per session to prevent abuse. **Error handling** provides friendly messages when the API times out or queries return no results, without exposing internal details. All guardrails run before the expensive LLM calls when possible, failing fast on invalid inputs.

---

## Project Structure

```
Order to Cash Explorer/
├── backend/
│   ├── data/
│   │   ├── sap-o2c-data/     # Raw JSONL files (input)
│   │   └── o2c.db            # SQLite database (generated)
│   ├── main.py               # FastAPI routes
│   ├── graph_model.py        # Graph schema and builder
│   ├── llm.py                # LLM logic and guardrails
│   ├── ingest.py             # JSONL → SQLite ingestion
│   ├── requirements.txt
│   └── .env                  # GROQ_API_KEY
├── frontend/
│   ├── src/
│   │   ├── App.jsx           # Main React component
│   │   └── App.css           # Styles
│   └── package.json
├── .gitignore
└── README.md
```

---

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/graph` | Full graph (nodes + edges), cached |
| GET | `/api/node/{id}` | Single node metadata |
| GET | `/api/stats` | Counts by entity type |
| POST | `/api/chat` | LLM Q&A with `{message, history}` |
| GET | `/api/health` | Health check with table stats |

---

## Entity Types

| Entity | Source Table | Count |
|--------|--------------|-------|
| SalesOrder | sales_order_headers | 100 |
| SalesOrderItem | sales_order_items | 167 |
| DeliveryHeader | outbound_delivery_headers | 86 |
| DeliveryItem | outbound_delivery_items | 137 |
| BillingHeader | billing_document_headers | 163 |
| BillingItem | billing_document_items | 245 |
| JournalEntry | journal_entry_items_accounts_receivable | 123 |
| Payment | payments_accounts_receivable | 120 |
| BusinessPartner | business_partners | 8 |
| Product | products | 69 |
| Plant | plants | 44 |

---

## License

MIT
