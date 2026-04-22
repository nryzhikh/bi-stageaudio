"""
HireTrack Database Sync API
A generic REST API to expose HireTrack database tables for replication and BI tools.

Deployment: Windows Server (E:\\hiretrack-flask-api\\server\\)
Port: 5003
"""

from flask import Flask, Response, jsonify, request, stream_with_context
from functools import wraps
import json
import pyodbc
from datetime import datetime, date, time as datetime_time, timedelta
import decimal
import logging
import os
import gc
import re
import uuid

# ============ APP SETUP ============
app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ============ CONFIGURATION ============
API_PORT = int(os.environ.get('API_PORT', 5003))
API_HOST = os.environ.get('API_HOST', '0.0.0.0')
DSN_NAME = os.environ.get('DSN_NAME', 'HireTrack DSN')

# Database credentials (kept out of the DSN on disk; passed at connect time)
NX_DB_USER = os.environ.get('NX_DB_USER') or None
NX_DB_PASSWORD = os.environ.get('NX_DB_PASSWORD') or None

# Optional: Basic Auth
API_USERNAME = os.environ.get('API_USERNAME') or None
API_PASSWORD = os.environ.get('API_PASSWORD') or None

# Memory safety limits
MAX_FIELD_SIZE = 10000  # Truncate fields larger than this (chars)
MAX_ROWS_PER_BATCH = 5000  # Process rows in batches

# ============ DATABASE CONNECTION ============
def _build_conn_str() -> str:
    parts = [f"DSN={DSN_NAME}"]
    if NX_DB_USER:
        parts.append(f"UID={NX_DB_USER}")
    if NX_DB_PASSWORD:
        parts.append(f"PWD={NX_DB_PASSWORD}")
    return ";".join(parts) + ";"


def get_db_connection():
    """Create database connection via ODBC DSN (credentials from env)"""
    try:
        return pyodbc.connect(_build_conn_str(), timeout=60)
    except pyodbc.Error as e:
        app.logger.error(f"Database connection error: {e}")
        return None

# ============ AUTHENTICATION ============
def check_auth(username, password):
    if API_USERNAME is None:
        return True
    return username == API_USERNAME and password == API_PASSWORD

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if API_USERNAME is None:
            return f(*args, **kwargs)
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return jsonify({"error": "Unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated

# ============ IDENTIFIER SAFETY ============
# HireTrack contains tables with hyphens (px-codes, veh-type) and dollar signs
# (SQL$Timed_Triggers). Unquoted, NexusDB parses "px-codes" as "px MINUS codes"
# and 500s. Direct f-string interpolation of the path param is ALSO a classic
# SQL injection vector. Fix both problems in one helper: whitelist-validate
# the name, then wrap it as an SQL-92 delimited identifier (double quotes).
_TABLE_NAME_RX = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-$#]*$")


def quote_table(table_name: str) -> str:
    """Return an SQL-92 quoted identifier, or raise ValueError.

    Delimited identifiers in NexusDB are case-sensitive. Pass the table name
    exactly as returned by /api/tables (we do, because the route captures it
    straight from the URL path).
    """
    if not isinstance(table_name, str) or not _TABLE_NAME_RX.match(table_name):
        raise ValueError(f"Invalid or unsafe table name: {table_name!r}")
    # Our regex already excluded embedded double quotes, so no escaping needed.
    return f'"{table_name}"'


def coerce_since_value(raw: str):
    """Decode a URL string watermark back into a typed value for pyodbc binding.

    NexusDB's ODBC driver refuses WHERE <timestamp_col> > '<iso-string>' because
    it won't implicit-cast string to TIMESTAMP. By binding a real datetime/int
    we let pyodbc pick the right SQL C type and the driver handles the rest.

    Order matters: try the most specific form first so '12345' binds as int
    and '2026-04-22T16:09:19.655000' binds as datetime.
    """
    if raw is None:
        return None
    # ISO 8601 (what our client writes back into sync_state.json)
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        pass
    # Integer surrogate keys (OpScanID, LogID, ...)
    try:
        return int(raw)
    except (ValueError, TypeError):
        pass
    # Float fallback
    try:
        return float(raw)
    except (ValueError, TypeError):
        pass
    # Last resort: bind as plain string (GUID, short codes, etc.)
    return raw


# ============ HELPERS ============
def serialize_value(val, max_size=MAX_FIELD_SIZE):
    """Convert database values to JSON-serializable format with size limits.

    Ordered from most common to least common types for hot-path efficiency.
    Anything not matched falls through to str(val) so a single exotic cell
    cannot poison jsonify() and 500 the whole table.
    """
    if val is None:
        return None
    try:
        # Hot path: JSON-native primitives.
        if isinstance(val, bool):  # bool is a subclass of int, so check first
            return val
        if isinstance(val, (int, float)):
            return val
        if isinstance(val, str):
            if len(val) > max_size:
                return val[:max_size] + '...[TRUNCATED]'
            return val
        # Temporal types.
        if isinstance(val, (datetime, date)):
            return val.isoformat()
        if isinstance(val, datetime_time):
            return val.isoformat()
        if isinstance(val, timedelta):
            return val.total_seconds()
        # Numeric: Decimal stays a float for downstream-friendliness; note this
        # is lossy for >15 significant digits. Revisit if you need DECIMAL
        # precision preserved end-to-end (switch to str(val) and a DECIMAL
        # column type on the consumer side).
        if isinstance(val, decimal.Decimal):
            return float(val)
        if isinstance(val, uuid.UUID):
            return str(val)
        # Binary.
        if isinstance(val, (bytes, bytearray, memoryview)):
            raw = bytes(val)
            try:
                decoded = raw.decode('utf-8', errors='replace')
                if len(decoded) > max_size:
                    return decoded[:max_size] + '...[TRUNCATED]'
                return decoded
            except Exception:
                return f'[BINARY: {len(raw)} bytes]'
        # Anything else: stringify rather than blow up the whole response.
        return str(val)
    except Exception as e:
        return f'[ERROR: {str(e)[:100]}]'

def serialize_row(columns, row):
    """Convert a database row to a dictionary with serializable values"""
    result = {}
    for col, val in zip(columns, row):
        try:
            result[col] = serialize_value(val)
        except Exception as e:
            result[col] = f'[SERIALIZE_ERROR: {str(e)[:50]}]'
    return result

# ============ API ROUTES ============

@app.route('/')
def index():
    """API Info and Health Check"""
    return jsonify({
        "service": "HireTrack Database Sync API",
        "version": "1.1.0",
        "status": "running",
        "endpoints": {
            "GET /api/tables": "List all available tables",
            "GET /api/table/<name>": "Get table data",
            "GET /api/table/<name>/schema": "Get table column definitions",
            "GET /api/table/<name>/count": "Get total row count",
            "POST /api/query": "Run custom SELECT query"
        },
        "auth": "enabled" if API_USERNAME else "disabled"
    })

@app.route('/health')
def health():
    """Health check endpoint"""
    conn = get_db_connection()
    db_status = "connected" if conn else "disconnected"
    if conn:
        conn.close()
    return jsonify({
        "status": "healthy" if conn else "unhealthy",
        "database": db_status
    })

@app.route('/api/tables')
@requires_auth
def list_tables():
    """List all tables in the database"""
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                SELECT RDB$RELATION_NAME 
                FROM RDB$RELATIONS 
                WHERE RDB$SYSTEM_FLAG = 0 
                AND RDB$VIEW_BLF IS NULL
                ORDER BY RDB$RELATION_NAME
            """)
            tables = [row[0].strip() for row in cursor.fetchall()]
        except Exception:
            tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
        
        app.logger.info(f"Listed {len(tables)} tables")
        return jsonify({
            "tables": tables,
            "count": len(tables)
        })
    except Exception as e:
        app.logger.error(f"Error listing tables: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/api/table/<table_name>')
@requires_auth
def get_table_data(table_name):
    """Stream table data as JSON.

    Rewritten as a streaming generator because 32-bit Python (required for
    the NexusDB driver) OOMs when buffering million-row tables. We now yield
    the response body chunk-by-chunk, holding only one fetch batch in memory.
    The response shape stays identical to the old buffered version:

        {"table": ..., "columns": [...], "data": [...], "count": N}

    Optional query params:
        since_field / since_value - incremental pull; rows WHERE field > value

    If a mid-stream error occurs we can't change HTTP status (headers are
    already flushed), so we append a trailing "error" field and log the
    failure. Client sees valid JSON with a partial data[] + error message.
    """
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    # --- Up-front validation: these are the only places we can still return 4xx
    try:
        q_table = quote_table(table_name)
    except ValueError as e:
        conn.close()
        return jsonify({"error": str(e), "table": table_name}), 400

    since_field = request.args.get('since_field')
    since_value = request.args.get('since_value')
    if (since_field is None) != (since_value is None):
        conn.close()
        return jsonify({
            "error": "since_field and since_value must be provided together",
            "table": table_name,
        }), 400

    sql = f"SELECT * FROM {q_table}"
    params: tuple = ()
    if since_field is not None:
        try:
            q_field = quote_table(since_field)  # same whitelist regex fits column names
        except ValueError as e:
            conn.close()
            return jsonify({"error": f"Invalid since_field: {e}", "table": table_name}), 400
        coerced = coerce_since_value(since_value)
        sql = f"{sql} WHERE {q_field} > ?"
        params = (coerced,)
        app.logger.info(
            f"Incremental filter: {since_field} > {coerced!r} "
            f"({type(coerced).__name__}, raw={since_value!r})"
        )

    # --- Execute before the generator starts so pyodbc.Error surfaces as 500
    cursor = conn.cursor()
    try:
        app.logger.info(
            f"Fetching data from {table_name}"
            + (f" WHERE {since_field} > {since_value!r}" if since_field else "")
        )
        cursor.execute(sql, params)
        columns = [col[0] for col in cursor.description]
    except pyodbc.Error as e:
        app.logger.error(f"Database error preparing {table_name}: {e}")
        cursor.close(); conn.close()
        return jsonify({"error": f"Database error: {str(e)}", "table": table_name}), 500

    def generate():
        """Stream JSON body. Memory cost ~= one batch, not the whole result."""
        count = 0
        error_msg = ""
        try:
            # Header: table + columns. json.dumps is small, no risk here.
            yield json.dumps({"table": table_name, "columns": columns})[:-1]
            yield ', "data": ['

            first = True
            batch_count = 0
            while True:
                rows = cursor.fetchmany(MAX_ROWS_PER_BATCH)
                if not rows:
                    break
                batch_count += 1
                for row in rows:
                    try:
                        obj = serialize_row(columns, row)
                    except Exception as e:
                        app.logger.warning(f"Error serializing row in {table_name}: {e}")
                        obj = {col: None for col in columns}
                    prefix = '' if first else ','
                    first = False
                    # default=str is a belt-and-braces guard for anything
                    # serialize_value missed; it should never fire in practice.
                    yield prefix + json.dumps(obj, default=str)
                    count += 1
                if batch_count % 5 == 0:
                    gc.collect()
            app.logger.info(f"Streamed {count} rows from {table_name}")
        except pyodbc.Error as e:
            error_msg = f"Database error mid-stream: {e}"
            app.logger.error(f"{error_msg} (table={table_name}, streamed={count})")
        except Exception as e:
            error_msg = f"{type(e).__name__}: {e}".strip(': ')
            app.logger.error(f"Stream error for {table_name}: {error_msg} "
                             f"(streamed={count})")
        finally:
            try:
                cursor.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            gc.collect()

        # Closing chunk. Always valid JSON, even after a mid-stream failure.
        tail = f'], "count": {count}'
        if error_msg:
            tail += f', "error": {json.dumps(error_msg)}'
        tail += '}'
        yield tail

    return Response(stream_with_context(generate()), mimetype='application/json')

@app.route('/api/table/<table_name>/schema')
@requires_auth
def get_table_schema(table_name):
    """Get table schema (column names and types)"""
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        q_table = quote_table(table_name)
    except ValueError as e:
        return jsonify({"error": str(e), "table": table_name}), 400

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {q_table} WHERE 1=0")  # schema without data
        
        schema = []
        for col in cursor.description:
            schema.append({
                "name": col[0],
                "type": str(col[1].__name__) if hasattr(col[1], '__name__') else str(col[1]),
                "size": col[3] if len(col) > 3 else None,
                "nullable": col[6] if len(col) > 6 else None
            })
        
        return jsonify({
            "table": table_name,
            "schema": schema,
            "column_count": len(schema)
        })
    except Exception as e:
        return jsonify({"error": str(e), "table": table_name}), 500
    finally:
        conn.close()

@app.route('/api/table/<table_name>/count')
@requires_auth
def get_table_count(table_name):
    """Get total row count for a table"""
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        q_table = quote_table(table_name)
    except ValueError as e:
        return jsonify({"error": str(e), "table": table_name}), 400

    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {q_table}")
        count = cursor.fetchone()[0]
        
        return jsonify({
            "table": table_name,
            "count": count
        })
    except Exception as e:
        return jsonify({"error": str(e), "table": table_name}), 500
    finally:
        conn.close()

@app.route('/api/query', methods=['POST'])
@requires_auth
def run_query():
    """Run a custom SELECT query (read-only)"""
    data = request.get_json()
    if not data or 'query' not in data:
        return jsonify({"error": "Missing 'query' in request body"}), 400
    
    query = data['query'].strip()
    
    # Block write operations
    forbidden = ['INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE', 
                 'TRUNCATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE']
    query_upper = query.upper()
    for word in forbidden:
        if word in query_upper.split():
            app.logger.warning(f"Blocked forbidden operation: {word}")
            return jsonify({"error": f"Write operation '{word}' not allowed"}), 403
    
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500
    
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        
        columns = [col[0] for col in cursor.description]
        rows = []
        
        while True:
            batch = cursor.fetchmany(MAX_ROWS_PER_BATCH)
            if not batch:
                break
            for row in batch:
                rows.append(serialize_row(columns, row))
        
        app.logger.info(f"Custom query returned {len(rows)} rows")
        return jsonify({
            "columns": columns,
            "data": rows,
            "count": len(rows)
        })
    except Exception as e:
        app.logger.error(f"Query error: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()
        gc.collect()

# ============ ERROR HANDLERS ============
@app.errorhandler(500)
def internal_error(error):
    app.logger.error(f"Internal server error: {error}")
    gc.collect()
    return jsonify({"error": "Internal server error"}), 500

@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.error(f"Unhandled exception: {e}")
    gc.collect()
    return jsonify({"error": str(e)}), 500

# ============ MAIN ============
if __name__ == '__main__':
    print("=" * 50)
    print("HireTrack Database Sync API v1.1.0")
    print("=" * 50)
    print(f"Host: {API_HOST}")
    print(f"Port: {API_PORT}")
    print(f"DSN:  {DSN_NAME}")
    print(f"Auth: {'Enabled' if API_USERNAME else 'Disabled'}")
    print(f"Max field size: {MAX_FIELD_SIZE}")
    print(f"Batch size: {MAX_ROWS_PER_BATCH}")
    print("=" * 50)
    app.run(host=API_HOST, port=API_PORT, debug=False, threaded=True)
