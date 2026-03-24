"""
LLM-powered Q&A for the Order to Cash dataset.

Uses Groq API to generate SQL queries and natural language answers.
Includes guardrails, rate limiting, and SQL safety checks.
"""

import os
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path

from groq import Groq

# =============================================================================
# RATE LIMITING
# =============================================================================

# In-memory rate limit tracker: session_id -> list of timestamps
_rate_limit_tracker = defaultdict(list)
RATE_LIMIT_MAX_REQUESTS = 10
RATE_LIMIT_WINDOW_SECONDS = 60


def check_rate_limit(session_id: str = "default") -> bool:
    """Check if the session has exceeded the rate limit. Returns True if allowed."""
    now = time.time()
    window_start = now - RATE_LIMIT_WINDOW_SECONDS

    # Clean old timestamps
    _rate_limit_tracker[session_id] = [
        ts for ts in _rate_limit_tracker[session_id] if ts > window_start
    ]

    # Check limit
    if len(_rate_limit_tracker[session_id]) >= RATE_LIMIT_MAX_REQUESTS:
        return False

    # Record this request
    _rate_limit_tracker[session_id].append(now)
    return True


# =============================================================================
# SCHEMA DESCRIPTION
# =============================================================================

SCHEMA_DESCRIPTION = """
Database: SAP Order-to-Cash (o2c.db)

Tables and key columns:

1. sales_order_headers
   - salesOrder (PK), salesOrderType, salesOrganization, distributionChannel
   - soldToParty (customer ID), creationDate, createdByUser, totalNetAmount
   - overallDeliveryStatus, transactionCurrency, requestedDeliveryDate
   - customerPaymentTerms, incotermsClassification

2. sales_order_items
   - salesOrder (FK), salesOrderItem, material (product ID)
   - requestedQuantity, requestedQuantityUnit, netAmount, transactionCurrency
   - productionPlant, storageLocation

3. outbound_delivery_headers
   - deliveryDocument (PK), soldToParty, shipToParty, creationDate
   - totalNetAmount, deliveryDate, overallGoodsMovementStatus

4. outbound_delivery_items
   - deliveryDocument (FK), deliveryDocumentItem, actualDeliveryQuantity
   - plant, storageLocation, referenceSdDocument (salesOrder), referenceSdDocumentItem

5. billing_document_headers
   - billingDocument (PK), billingDocumentType, soldToParty, creationDate
   - totalNetAmount, transactionCurrency, accountingDocument

6. billing_document_items
   - billingDocument (FK), billingDocumentItem, material, billingQuantity
   - netAmount, referenceSdDocument (salesOrder), referenceSdDocumentItem

7. billing_document_cancellations
   - billingDocument, cancelledBillingDocument, cancellationDate

8. journal_entry_items_accounts_receivable
   - accountingDocument (PK), companyCode, fiscalYear, postingDate
   - amountInTransactionCurrency, transactionCurrency, customer

9. payments_accounts_receivable
   - accountingDocument, accountingDocumentItem, clearingDate
   - amountInTransactionCurrency, customer, invoiceReference (billingDocument)
   - postingDate, salesDocument

10. business_partners
    - businessPartner (PK), businessPartnerName, businessPartnerType

11. business_partner_addresses
    - businessPartner (FK), country, city, postalCode, streetName

12. customer_company_assignments
    - customer, companyCode, paymentTerms

13. customer_sales_area_assignments
    - customer, salesOrganization, distributionChannel, division

14. products
    - product (PK), productType, baseUnit, productGroup

15. product_descriptions
    - product (FK), language, productDescription

16. product_plants
    - product (FK), plant, purchasingGroup, mrpType

17. product_storage_locations
    - product (FK), plant, storageLocation

18. plants
    - plant (PK), plantName, companyCode, country, city
"""

# =============================================================================
# GUARDRAIL CONFIGURATION
# =============================================================================

# Domain-specific keywords that indicate valid O2C questions
ALLOWED_DOMAIN_KEYWORDS = [
    # Core O2C entities
    "sales order", "sales orders", "order", "orders",
    "billing", "invoice", "invoices", "bill",
    "delivery", "deliveries", "shipment", "shipping", "shipped",
    "payment", "payments", "paid", "pay", "receivable", "receivables",
    "journal", "journal entry", "accounting",
    "customer", "customers", "buyer", "client",
    "product", "products", "material", "materials", "item", "items",
    "plant", "plants", "warehouse", "storage",
    # SAP / O2C terminology
    "sap", "o2c", "order to cash", "order-to-cash",
    "document", "documents",
    "partner", "business partner",
    # Data attributes
    "quantity", "quantities", "amount", "amounts", "total", "net",
    "currency", "price", "value", "cost",
    "date", "created", "posted", "cleared",
    "status", "completed", "pending", "cancelled", "canceled",
    # Query patterns
    "how many", "how much", "count", "number of",
    "list", "show", "find", "get", "display",
    "which", "what", "who",
    "average", "avg", "sum", "total", "min", "max", "minimum", "maximum",
    "top", "bottom", "highest", "lowest", "most", "least",
    "recent", "latest", "oldest", "first", "last",
    "between", "before", "after", "during",
    # Table references
    "table", "tables", "record", "records", "row", "rows", "data",
    "transaction", "transactions",
    "incoterms", "fiscal", "clearing", "mrp",
]

# Patterns that strongly indicate off-topic questions
OFF_TOPIC_PATTERNS = [
    # General knowledge
    r"capital of", r"president of", r"who is", r"who was",
    r"weather", r"temperature", r"forecast",
    r"definition of", r"meaning of", r"what is a\b", r"what are\b",
    # Creative/entertainment
    r"write a", r"write me", r"create a story", r"tell me a joke",
    r"poem", r"song lyrics", r"movie", r"book",
    # Technical/programming (non-data)
    r"code for", r"programming", r"python code", r"javascript",
    r"how to install", r"how to configure",
    # Personal/chat
    r"how are you", r"what can you do", r"who are you",
    r"hello", r"hi there", r"hey",
    # Other domains
    r"recipe", r"cook", r"sports", r"game score",
    r"translate", r"language",
    r"news", r"politics", r"celebrity",
]

# SQL keywords that are NOT allowed (write operations)
FORBIDDEN_SQL_KEYWORDS = [
    "DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE",
    "TRUNCATE", "REPLACE", "ATTACH", "DETACH", "VACUUM",
    "PRAGMA", "REINDEX",
]

# Error messages
OFF_TOPIC_RESPONSE = (
    "This system is designed to answer questions about the Order to Cash dataset only. "
    "Please ask about sales orders, deliveries, billing documents, payments, customers, "
    "products, or related SAP O2C entities."
)

RATE_LIMIT_RESPONSE = (
    "You've reached the maximum number of requests (10 per minute). "
    "Please wait a moment before asking another question."
)

UNSAFE_SQL_RESPONSE = (
    "I can only run read-only queries on the dataset. "
    "Please ask a question that retrieves information rather than modifying data."
)

NO_RESULTS_RESPONSE = (
    "No matching records found for that query in the dataset. "
    "Try broadening your search criteria or checking for typos in any specific values."
)

API_ERROR_RESPONSE = (
    "I had trouble processing that query. Please try rephrasing your question."
)


# =============================================================================
# GUARDRAIL FUNCTIONS
# =============================================================================

def is_on_topic(message: str) -> bool:
    """
    Check if the message is related to the O2C dataset.
    Returns True if the question appears to be on-topic.
    """
    message_lower = message.lower()

    # First, check for explicit off-topic patterns
    for pattern in OFF_TOPIC_PATTERNS:
        if re.search(pattern, message_lower):
            return False

    # Check for domain keywords
    for keyword in ALLOWED_DOMAIN_KEYWORDS:
        if keyword in message_lower:
            return True

    # Check for SQL-like patterns (user might be writing SQL directly)
    sql_patterns = [r"\bselect\b", r"\bfrom\b", r"\bwhere\b", r"\bjoin\b", r"\bgroup by\b"]
    for pattern in sql_patterns:
        if re.search(pattern, message_lower):
            return True

    # If none of the domain keywords are found, reject
    return False


def is_sql_safe(sql: str) -> bool:
    """
    Check if the generated SQL is safe (read-only).
    Returns True if the SQL contains no forbidden keywords.
    """
    sql_upper = sql.upper()

    for keyword in FORBIDDEN_SQL_KEYWORDS:
        # Match as whole word to avoid false positives
        pattern = r'\b' + keyword + r'\b'
        if re.search(pattern, sql_upper):
            return False

    return True


# =============================================================================
# SQL GENERATION AND EXECUTION
# =============================================================================

def generate_sql(client: Groq, message: str, history: list = None, error_context: str = None) -> str:
    """Generate SQL query from natural language using Groq."""
    system_prompt = f"""You are an SQL expert. Generate a SQLite query to answer the user's question.

{SCHEMA_DESCRIPTION}

Rules:
- Respond with ONLY the SQL query, nothing else
- No markdown, no explanation, no code blocks, just raw SQL
- Use proper SQLite syntax
- Use LIMIT 20 for queries that might return many rows
- Use appropriate JOINs when data spans multiple tables
- Column names with mixed case should be quoted with double quotes
- ONLY use SELECT statements - no INSERT, UPDATE, DELETE, DROP, or other modifications
- If the user refers to previous context (like "those orders" or "that customer"), use the conversation history to understand what they mean
"""

    if error_context:
        system_prompt += f"\n\nPrevious query failed with error: {error_context}\nPlease fix the query."

    # Build messages with conversation history for context
    messages = [{"role": "system", "content": system_prompt}]

    # Add conversation history for context (last 4 pairs = 8 messages)
    if history:
        for msg in history[-8:]:
            messages.append({
                "role": msg.get("role", "user"),
                "content": msg.get("content", ""),
            })

    messages.append({"role": "user", "content": message})

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=messages,
        temperature=0,
        max_tokens=500,
        timeout=30,
    )

    sql = response.choices[0].message.content.strip()

    # Clean up any markdown formatting
    if sql.startswith("```"):
        lines = sql.split("\n")
        sql = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
        sql = sql.strip()

    # Remove trailing semicolons or extra whitespace
    sql = sql.rstrip(";").strip()

    return sql


def execute_query(db_path: Path, sql: str) -> tuple[list[dict], list[str]]:
    """Execute SQL query and return results as list of dicts."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(sql)
    rows = cursor.fetchall()

    if rows:
        columns = list(rows[0].keys())
        results = [dict(row) for row in rows]
    else:
        columns = []
        results = []

    conn.close()
    return results, columns


def format_results_as_table(results: list[dict], columns: list[str], max_rows: int = 10) -> str:
    """Format query results as a markdown table."""
    if not results:
        return "No results found."

    # Truncate if too many rows
    display_results = results[:max_rows]
    truncated = len(results) > max_rows

    # Build markdown table
    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"

    rows = []
    for row in display_results:
        values = [str(row.get(col, ""))[:50] for col in columns]
        rows.append("| " + " | ".join(values) + " |")

    table = "\n".join([header, separator] + rows)

    if truncated:
        table += f"\n\n... and {len(results) - max_rows} more rows"

    return table


def generate_answer(client: Groq, question: str, sql: str, results_table: str, row_count: int) -> str:
    """Generate natural language answer from query results."""
    system_prompt = """You are a helpful data analyst. Given a question, the SQL query used, and the results,
provide a clear, concise answer in plain English.

Rules:
- Keep answers to 1-3 sentences unless listing items
- If the result is a list, format it nicely with bullet points
- Include relevant numbers from the data
- Don't mention SQL or technical details unless asked
- If no results, explain what that means for the question
"""

    user_prompt = f"""Question: {question}

Query used: {sql}

Results ({row_count} rows):
{results_table}

Provide a clear answer to the question based on these results."""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.3,
        max_tokens=500,
        timeout=30,
    )

    return response.choices[0].message.content.strip()


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def answer_query(message: str, history: list, db_path: Path, session_id: str = "default") -> dict:
    """
    Main function to answer a user query about the O2C dataset.

    Returns: { response, query_used, rows_returned }
    """
    # Step 0: Rate limit check
    if not check_rate_limit(session_id):
        return {
            "response": RATE_LIMIT_RESPONSE,
            "query_used": None,
            "rows_returned": None,
        }

    # Step 1: Topic guardrail check
    if not is_on_topic(message):
        return {
            "response": OFF_TOPIC_RESPONSE,
            "query_used": None,
            "rows_returned": None,
        }

    # Initialize Groq client
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return {
            "response": "Error: GROQ_API_KEY not configured. Please add it to the .env file.",
            "query_used": None,
            "rows_returned": None,
        }

    client = Groq(api_key=api_key)

    # Step 2: Generate SQL (with conversation history for context)
    try:
        sql = generate_sql(client, message, history=history)
    except Exception:
        return {
            "response": API_ERROR_RESPONSE,
            "query_used": None,
            "rows_returned": None,
        }

    # Step 3: SQL safety check
    if not is_sql_safe(sql):
        return {
            "response": UNSAFE_SQL_RESPONSE,
            "query_used": sql,
            "rows_returned": None,
        }

    # Step 4: Execute query (with retry on error)
    try:
        results, columns = execute_query(db_path, sql)
    except Exception as e:
        # Retry once with error context
        try:
            sql = generate_sql(client, message, history=history, error_context=str(e))

            # Re-check safety after regeneration
            if not is_sql_safe(sql):
                return {
                    "response": UNSAFE_SQL_RESPONSE,
                    "query_used": sql,
                    "rows_returned": None,
                }

            results, columns = execute_query(db_path, sql)
        except Exception:
            return {
                "response": API_ERROR_RESPONSE,
                "query_used": sql,
                "rows_returned": None,
            }

    row_count = len(results)

    # Step 5: Handle zero results
    if row_count == 0:
        return {
            "response": NO_RESULTS_RESPONSE,
            "query_used": sql,
            "rows_returned": 0,
        }

    # Step 6: Generate natural language answer
    try:
        results_table = format_results_as_table(results, columns)
        answer = generate_answer(client, message, sql, results_table, row_count)
    except Exception:
        # Fall back to just showing the table
        answer = format_results_as_table(results, columns, max_rows=20)

    return {
        "response": answer,
        "query_used": sql,
        "rows_returned": row_count,
    }
