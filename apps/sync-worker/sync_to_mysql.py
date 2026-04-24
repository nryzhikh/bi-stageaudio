#!/usr/bin/env python3
"""
HireTrack Database Sync to MySQL
Pulls data from the Sync API and stores in MySQL database.
Designed for local Docker MySQL or remote VPS MySQL.
"""

import csv
import decimal
import itertools
import json
import logging
import requests
import mysql.connector
from mysql.connector import Error
import os
import sys
import argparse
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import ijson
import yaml

# Schema inference sample size. Larger samples catch type edge cases (wide
# strings in later rows, int columns that sneak in a float) that a 1k window
# would miss. 10k rows at ~1 KB each is ~10 MB peak — trivial on laptops.
PRIME_BATCH = 10_000
# Row batch for streaming INSERTs. Each executemany ships ~5-10 MB of bind
# values which stays well under MySQL's 64 MB max_allowed_packet default.
# Raise PRIME_BATCH/STREAM_BATCH together so schema and insert cadences
# match — splitting them gains nothing and complicates progress output.
STREAM_BATCH = 10_000

# ============ CONFIGURATION ============
# All connection info is read from the environment so the container has a
# single config source (docker compose `environment:`). No baked-in secrets
# or hosts: missing values should fail loudly rather than silently pointing
# at the wrong database or API.
DEFAULT_API_URL = os.environ.get('API_URL')
DEFAULT_MYSQL_HOST = os.environ.get('MYSQL_HOST')
DEFAULT_MYSQL_PORT = int(os.environ.get('MYSQL_PORT', '3306'))
DEFAULT_MYSQL_USER = os.environ.get('MYSQL_USER')
DEFAULT_MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD')
DEFAULT_MYSQL_DATABASE = os.environ.get('MYSQL_DATABASE')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))
DEFAULT_CONFIG_PATH = os.path.join(SCRIPT_DIR, 'sync_config.yaml')
DEFAULT_STATE_PATH = os.path.join(REPO_ROOT, 'var', 'runtime', 'sync_state.json')
DEFAULT_REPORT_PATH = os.path.join(REPO_ROOT, 'var', 'artifacts', 'sync_report.csv')
SYNC_STATE_TABLE = '_sync_table_state'
SYNC_RUN_LOCK_NAME = 'hiretrack_sync_run'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_FORMAT = os.environ.get('LOG_FORMAT', 'json')

LOGGER = logging.getLogger('hiretrack.sync_worker')
_LOG_RECORD_DEFAULTS = set(logging.makeLogRecord({}).__dict__)


class JsonLogFormatter(logging.Formatter):
    """Emit structured JSON logs using only the standard logging package."""

    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            'timestamp': datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec='milliseconds'),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info:
            payload['exception'] = self.formatException(record.exc_info)
        for key, value in record.__dict__.items():
            if key not in _LOG_RECORD_DEFAULTS and key not in payload:
                payload[key] = self._json_safe(value)
        return json.dumps(payload, default=str, sort_keys=True)

    @staticmethod
    def _json_safe(value: Any) -> Any:
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        if isinstance(value, (list, tuple)):
            return [JsonLogFormatter._json_safe(v) for v in value]
        if isinstance(value, dict):
            return {str(k): JsonLogFormatter._json_safe(v) for k, v in value.items()}
        return str(value)


class TextLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        base = super().format(record)
        fields = []
        for key, value in sorted(record.__dict__.items()):
            if key not in _LOG_RECORD_DEFAULTS:
                fields.append(f'{key}={value!r}')
        if fields:
            return f'{base} {" ".join(fields)}'
        return base


def configure_logging(level: str = LOG_LEVEL, log_format: str = LOG_FORMAT) -> None:
    handler = logging.StreamHandler()
    if log_format == 'text':
        handler.setFormatter(TextLogFormatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    else:
        handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))


def log_event(level: int, event: str, message: str, **fields: Any) -> None:
    LOGGER.log(level, message, extra={'event': event, **fields})


def load_sync_config(path: str) -> Dict[str, Any]:
    """Read the YAML sync policy. Missing file => no skip, no incremental."""
    if not path or not os.path.exists(path):
        return {"skip": [], "incremental": {}}
    with open(path) as fh:
        data = yaml.safe_load(fh) or {}
    return {
        "skip": list(data.get("skip") or []),
        "incremental": dict(data.get("incremental") or {}),
    }


def load_legacy_state(path: str) -> Dict[str, Dict[str, Any]]:
    """Read the old JSON watermark file for one-time migration into MySQL."""
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path) as fh:
            return json.load(fh)
    except (json.JSONDecodeError, OSError):
        return {}


def encode_state_value(value: Any) -> Tuple[Optional[str], Optional[str]]:
    """Serialize typed watermarks so MySQL metadata preserves comparison semantics."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return 'bool', '1' if value else '0'
    if isinstance(value, int) and not isinstance(value, bool):
        return 'int', str(value)
    if isinstance(value, float):
        return 'float', repr(value)
    if isinstance(value, datetime):
        return 'datetime', value.isoformat()
    return 'str', str(value)


def decode_state_value(value_type: Optional[str], value: Optional[str]) -> Any:
    if value is None or value_type is None:
        return None
    try:
        if value_type == 'bool':
            return value == '1'
        if value_type == 'int':
            return int(value)
        if value_type == 'float':
            return float(value)
        if value_type == 'datetime':
            return datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return value
    return value


class TableStream:
    """Incremental parser for /api/table/<name> responses.

    The server emits one envelope per table:

        {"table": ..., "columns": [...], "data": [{...}, ...],
         "count": N, "error"?: "..."}

    Parsing the body with ijson keeps peak memory at ~one row regardless of
    table size. We expose three phases:

        1. .columns          — populated eagerly in __init__ (server writes
                               columns before the data array, so we can read
                               them without buffering any rows).
        2. .iter_rows()      — generator that yields row dicts one at a time.
        3. .trailer          — {count, error?}. Only populated AFTER
                               iter_rows() is exhausted; reading it early
                               returns an empty dict.

    Why we share a single parser across phases instead of using
    ijson.items(..., 'data.item'): items() re-scans from the start, and we
    need the trailer after the data array ends in the same stream. A manual
    event loop lets us keep state across phases on one HTTP connection.
    """

    def __init__(self, response: requests.Response):
        self._response = response
        # decode_content=True makes raw transparently ungzip if the server
        # compresses (it doesn't today, but cheap insurance).
        response.raw.decode_content = True
        self._parser = ijson.parse(response.raw)
        self.columns: List[str] = []
        self.trailer: Dict[str, Any] = {}
        self._read_columns()

    def _read_columns(self) -> None:
        """Consume parser events until we've captured the columns array."""
        for prefix, event, value in self._parser:
            if prefix == 'columns.item' and event == 'string':
                self.columns.append(value)
            elif prefix == 'columns' and event == 'end_array':
                return

    @staticmethod
    def _normalize(value: Any) -> Any:
        """Flatten ijson's Decimal numbers into int/float for MySQL binding.

        ijson returns Decimal for *all* JSON numbers to preserve precision.
        MySQL-connector handles Decimal fine, but mixing it into our type
        inference (which only knows about int/float/str) bloats every
        numeric column to MEDIUMTEXT. Converting at parse time keeps
        inference accurate and binding fast.
        """
        if isinstance(value, decimal.Decimal):
            if value == value.to_integral_value():
                return int(value)
            return float(value)
        return value

    def iter_rows(self) -> Iterator[Dict[str, Any]]:
        """Yield row dicts until the data array closes.

        After this generator is exhausted, `self.trailer` is populated.
        If the caller stops early (islice / break), the trailer stays
        empty — don't rely on it until full consumption.
        """
        current: Optional[Dict[str, Any]] = None
        for prefix, event, value in self._parser:
            if prefix == 'data.item' and event == 'start_map':
                current = {}
            elif prefix == 'data.item' and event == 'end_map':
                yield current
                current = None
            elif current is not None and prefix.startswith('data.item.') and event in (
                'string', 'number', 'boolean', 'null'
            ):
                key = prefix[len('data.item.'):]
                current[key] = self._normalize(value)
            elif prefix == 'data' and event == 'end_array':
                break
        self._read_trailer()

    def _read_trailer(self) -> None:
        """Drain remaining events into the trailer dict (count, error)."""
        for prefix, event, value in self._parser:
            if prefix == 'count' and event == 'number':
                self.trailer['count'] = int(value)
            elif prefix == 'error' and event == 'string':
                self.trailer['error'] = value

    def close(self) -> None:
        try:
            self._response.close()
        except Exception:
            pass


class HireTrackMySQLSync:
    def __init__(
        self,
        api_url: str,
        mysql_host: str,
        mysql_port: int,
        mysql_user: str,
        mysql_password: str,
        mysql_database: str,
        api_auth: tuple = None
    ):
        self.api_url = api_url.rstrip('/')
        self.mysql_config = {
            'host': mysql_host,
            'port': mysql_port,
            'user': mysql_user,
            'password': mysql_password,
            'database': mysql_database,
            'charset': 'utf8mb4',
            'collation': 'utf8mb4_unicode_ci',
            'use_pure': True
        }
        self.api_auth = api_auth
        self.session = requests.Session()
        if api_auth:
            self.session.auth = api_auth

    def build_run_lock_name(self) -> str:
        """Scope the advisory lock to the target database."""
        return f'{SYNC_RUN_LOCK_NAME}:{self.mysql_config["database"]}'

    def acquire_run_lock(self, conn, timeout_sec: int) -> bool:
        """Use a MySQL advisory lock to prevent overlapping sync runs."""
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT GET_LOCK(%s, %s)', (self.build_run_lock_name(), timeout_sec))
            row = cursor.fetchone()
            return bool(row and row[0] == 1)
        finally:
            cursor.close()

    def release_run_lock(self, conn) -> None:
        cursor = conn.cursor()
        try:
            cursor.execute('SELECT RELEASE_LOCK(%s)', (self.build_run_lock_name(),))
        finally:
            cursor.close()

    def ensure_metadata_tables(self, conn) -> None:
        """Create replication metadata tables used by the worker itself."""
        cursor = conn.cursor()
        try:
            cursor.execute(
                f'CREATE TABLE IF NOT EXISTS `{SYNC_STATE_TABLE}` ('
                '  `source_table` VARCHAR(255) NOT NULL,'
                '  `watermark_type` VARCHAR(32) NULL,'
                '  `watermark_value` LONGTEXT NULL,'
                '  `watermark_field` VARCHAR(255) NULL,'
                '  `pk_field` VARCHAR(255) NULL,'
                '  `last_rows` BIGINT NOT NULL DEFAULT 0,'
                '  `updated_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP '
                '      ON UPDATE CURRENT_TIMESTAMP,'
                '  PRIMARY KEY (`source_table`)'
                ') ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
            )
            conn.commit()
        finally:
            cursor.close()

    def load_table_states(self, conn) -> Dict[str, Dict[str, Any]]:
        """Read incremental watermark state from MySQL metadata tables."""
        cursor = conn.cursor(dictionary=True)
        try:
            cursor.execute(
                f'SELECT source_table, watermark_type, watermark_value, '
                f'watermark_field, pk_field, last_rows, updated_at '
                f'FROM `{SYNC_STATE_TABLE}`'
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()

        state: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            state[row['source_table']] = {
                'watermark': decode_state_value(
                    row.get('watermark_type'),
                    row.get('watermark_value'),
                ),
                'watermark_type': row.get('watermark_type'),
                'watermark_field': row.get('watermark_field'),
                'pk': row.get('pk_field'),
                'last_rows': row.get('last_rows'),
                'updated_at': row.get('updated_at'),
            }
        return state

    def upsert_table_state(
        self,
        conn,
        table_name: str,
        watermark: Any,
        watermark_field: str,
        pk_field: Optional[str],
        last_rows: int,
    ) -> None:
        """Persist a table watermark in MySQL instead of a local JSON file."""
        value_type, value_text = encode_state_value(watermark)
        cursor = conn.cursor()
        try:
            cursor.execute(
                f'INSERT INTO `{SYNC_STATE_TABLE}` ('
                '  `source_table`, `watermark_type`, `watermark_value`,'
                '  `watermark_field`, `pk_field`, `last_rows`'
                ') VALUES (%s, %s, %s, %s, %s, %s) '
                'ON DUPLICATE KEY UPDATE '
                '  `watermark_type` = VALUES(`watermark_type`), '
                '  `watermark_value` = VALUES(`watermark_value`), '
                '  `watermark_field` = VALUES(`watermark_field`), '
                '  `pk_field` = VALUES(`pk_field`), '
                '  `last_rows` = VALUES(`last_rows`), '
                '  `updated_at` = CURRENT_TIMESTAMP',
                (table_name, value_type, value_text, watermark_field, pk_field, last_rows),
            )
            conn.commit()
        finally:
            cursor.close()

    def metadata_has_state(self, conn) -> bool:
        cursor = conn.cursor()
        try:
            cursor.execute(f'SELECT 1 FROM `{SYNC_STATE_TABLE}` LIMIT 1')
            return cursor.fetchone() is not None
        finally:
            cursor.close()

    def import_legacy_state(self, conn, path: str) -> int:
        """One-time migration path from the old JSON state file."""
        legacy = load_legacy_state(path)
        if not legacy or self.metadata_has_state(conn):
            return 0

        cursor = conn.cursor()
        imported = 0
        try:
            for table_name, raw in legacy.items():
                watermark = raw.get('watermark')
                value_type, value_text = encode_state_value(watermark)
                cursor.execute(
                    f'INSERT INTO `{SYNC_STATE_TABLE}` ('
                    '  `source_table`, `watermark_type`, `watermark_value`,'
                    '  `last_rows`, `updated_at`'
                    ') VALUES (%s, %s, %s, %s, %s) '
                    'ON DUPLICATE KEY UPDATE '
                    '  `watermark_type` = VALUES(`watermark_type`), '
                    '  `watermark_value` = VALUES(`watermark_value`), '
                    '  `last_rows` = VALUES(`last_rows`), '
                    '  `updated_at` = VALUES(`updated_at`)',
                    (
                        table_name,
                        value_type,
                        value_text,
                        raw.get('last_rows', 0),
                        raw.get('updated_at'),
                    ),
                )
                imported += 1
            conn.commit()
            return imported
        except Error:
            conn.rollback()
            raise
        finally:
            cursor.close()
    
    def api_request(self, endpoint: str, method: str = 'GET', data: dict = None,
                    params: dict = None, retries: int = 3) -> dict:
        """Make an API request with retry logic.

        On HTTP errors we surface the server's JSON body (if any) so upstream
        problems like SQL errors reach the user instead of a bare status line.
        """
        url = f"{self.api_url}{endpoint}"
        last_error = None

        for attempt in range(retries):
            try:
                if method == 'GET':
                    response = self.session.get(url, params=params, timeout=300)
                else:
                    response = self.session.post(url, json=data, params=params, timeout=300)
                # Prefer the structured error body over the generic status line.
                if response.status_code >= 400:
                    try:
                        body = response.json()
                        err = body.get('error') or body
                    except ValueError:
                        err = response.text[:500] or f"HTTP {response.status_code}"
                    last_error = f"HTTP {response.status_code}: {err}"
                    # Don't retry 4xx — those are deterministic (bad input, auth).
                    if 400 <= response.status_code < 500:
                        return {"error": last_error}
                else:
                    return response.json()
            except requests.exceptions.RequestException as e:
                last_error = str(e)

            if attempt < retries - 1:
                wait_time = (attempt + 1) * 5  # 5, 10, 15 seconds
                log_event(
                    logging.WARNING,
                    'api_request_retry',
                    'API request failed; retrying',
                    endpoint=endpoint,
                    method=method,
                    attempt=attempt + 1,
                    max_attempts=retries,
                    wait_seconds=wait_time,
                    error=last_error,
                )
                time.sleep(wait_time)

        return {"error": str(last_error)}
    
    def check_api_connection(self) -> bool:
        """Test API connection"""
        result = self.api_request('/health')
        return result.get('status') == 'healthy'
    
    def check_mysql_connection(self) -> bool:
        """Test MySQL connection"""
        try:
            conn = mysql.connector.connect(**self.mysql_config)
            conn.close()
            return True
        except Error as e:
            log_event(
                logging.ERROR,
                'mysql_connection_failed',
                'MySQL connection failed',
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                database=self.mysql_config['database'],
                error=str(e),
            )
            return False
    
    def get_tables(self) -> List[str]:
        """Get list of all tables from API"""
        result = self.api_request('/api/tables')
        if 'error' in result:
            log_event(
                logging.ERROR,
                'table_list_failed',
                'Failed to get table list from API',
                error=result['error'],
            )
            return []
        return result.get('tables', [])
    
    def _open_table_stream(self, table: str, since_field: Optional[str] = None,
                           since_value: Any = None) -> requests.Response:
        """Open a streaming GET for /api/table/<table>.

        Uses stream=True so we can hand response.raw to ijson and parse the
        body incrementally. The read timeout is None because a legitimate
        stream of a large table can take many minutes between chunks on a
        slow ODBC driver — timing that out would abort healthy syncs.

        Retries: none here. Streaming retries would require resuming from a
        specific row, which the server doesn't support. If the connection
        drops, let the caller rerun the client (incremental state makes
        this cheap — we skip already-watermarked rows).
        """
        url = f"{self.api_url}/api/table/{table}"
        params = None
        if since_field is not None and since_value is not None:
            params = {'since_field': since_field, 'since_value': str(since_value)}
        return self.session.get(url, params=params, timeout=(30, None), stream=True)
    
    def sanitize_name(self, name: str) -> str:
        """Make column/table name safe for MySQL"""
        # Remove or replace invalid characters
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        # Ensure doesn't start with number
        if safe and safe[0].isdigit():
            safe = '_' + safe
        # MySQL reserved words - wrap in backticks handled separately
        return safe or '_col'
    
    def infer_column_type(self, values: List[Any]) -> str:
        """Infer a MySQL column type from the FULL set of values in the column.

        Row-0-only inference (what this code used to do) caused silent
        VARCHAR(255) truncations whenever row 0 was short or NULL but later
        rows had longer strings. We scan the whole column and pick a type
        wide enough to hold every value we've seen.

        Conservative defaults: MEDIUMTEXT (16 MB) for strings, BIGINT for
        integers. For an analytics / replication target these trade a small
        amount of storage efficiency for zero-surprise correctness.
        """
        has_str = False
        has_float = False
        has_int = False
        has_bool = False
        all_none = True
        for v in values:
            if v is None:
                continue
            all_none = False
            if isinstance(v, bool):      # must be before int (bool subclasses int)
                has_bool = True
            elif isinstance(v, int):
                has_int = True
            elif isinstance(v, float):
                has_float = True
            elif isinstance(v, str):
                has_str = True
            else:
                # Anything else (after server-side serialize_value) gets
                # stringified on the wire, so treat it as a string column.
                has_str = True
        if has_str:
            return 'MEDIUMTEXT'
        if has_float:
            return 'DOUBLE'
        if has_int:
            return 'BIGINT'
        if has_bool:
            return 'TINYINT(1)'
        # Column is 100% NULL in this snapshot; pick the widest type so a
        # future non-NULL value of any kind will fit.
        return 'LONGTEXT' if all_none else 'MEDIUMTEXT'

    def create_table(
        self,
        cursor,
        table_name: str,
        columns: List[str],
        rows: List[Dict],
        pk: Optional[str] = None,
        target_table: Optional[str] = None,
    ) -> List[str]:
        """Create a MySQL table with types inferred from the sampled rows.

        If `pk` is supplied, a PRIMARY KEY constraint is added so later upserts
        can use ON DUPLICATE KEY UPDATE. The pk column type is forced to a
        non-TEXT type because MySQL can't PK a TEXT column directly.

        We intentionally do NOT add an `_synced_at ON UPDATE CURRENT_TIMESTAMP`
        column. Per-row sync timestamps on an UPSERT-heavy workload force a
        full row rewrite (and a full binlog event) on every re-ingest even
        when no source column actually changed, which made the binlog the
        single biggest disk consumer on the production VPS. The last-sync
        timestamp for a whole table lives in `_sync_table_state.updated_at`,
        which is enough for operational observability.
        """
        safe_table = self.sanitize_name(target_table or table_name)
        safe_pk = self.sanitize_name(pk) if pk else None
        safe_columns = []
        col_definitions = []

        for col in columns:
            safe_col = self.sanitize_name(col)
            safe_columns.append(safe_col)
            col_values = [r.get(col) for r in rows]
            col_type = self.infer_column_type(col_values)
            # MySQL refuses BLOB/TEXT as PRIMARY KEY; VARCHAR(255) is a safe narrow
            # alternative wide enough for GUIDs, integer-like IDs, short strings.
            if safe_col == safe_pk and col_type in ('MEDIUMTEXT', 'LONGTEXT'):
                col_type = 'VARCHAR(255)'
            col_definitions.append(f'`{safe_col}` {col_type}')

        if safe_pk:
            if safe_pk not in safe_columns:
                raise ValueError(
                    f"incremental pk '{pk}' not found in columns of {table_name}"
                )
            col_definitions.append(f'PRIMARY KEY (`{safe_pk}`)')

        cursor.execute(f'DROP TABLE IF EXISTS `{safe_table}`')
        cursor.execute(
            f'CREATE TABLE `{safe_table}` ({", ".join(col_definitions)}) '
            'ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci'
        )
        return safe_columns

    def swap_full_refresh_tables(self, cursor, table_name: str, staging_table: str) -> None:
        """Atomically replace the live table with a fully loaded staging table."""
        safe_live = self.sanitize_name(table_name)
        safe_staging = self.sanitize_name(staging_table)
        backup_table = f'_{safe_live}_old'

        cursor.execute(f'DROP TABLE IF EXISTS `{backup_table}`')
        if self.mysql_table_exists(cursor, table_name):
            cursor.execute(
                f'RENAME TABLE `{safe_live}` TO `{backup_table}`, '
                f'`{safe_staging}` TO `{safe_live}`'
            )
            cursor.execute(f'DROP TABLE `{backup_table}`')
        else:
            cursor.execute(f'RENAME TABLE `{safe_staging}` TO `{safe_live}`')

    def mysql_table_exists(self, cursor, table_name: str) -> bool:
        safe = self.sanitize_name(table_name)
        cursor.execute(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
            (safe,),
        )
        return cursor.fetchone() is not None

    def insert_rows(self, cursor, table_name: str, safe_columns: List[str],
                    source_columns: List[str], rows: List[Dict],
                    upsert_pk: Optional[str] = None) -> None:
        """Batched INSERT (or UPSERT if upsert_pk is given).

        Called once per STREAM_BATCH from sync_table's flush(). The inner
        chunking loop is kept so this method is still safe if called with
        a larger list (e.g. from tests or future callers).
        """
        if not rows:
            return
        safe_table = self.sanitize_name(table_name)
        placeholders = ', '.join(['%s'] * len(safe_columns))
        cols_str = ', '.join(f'`{c}`' for c in safe_columns)

        if upsert_pk:
            safe_pk = self.sanitize_name(upsert_pk)
            updates = ', '.join(
                f'`{c}` = VALUES(`{c}`)' for c in safe_columns if c != safe_pk
            )
            insert_sql = (
                f'INSERT INTO `{safe_table}` ({cols_str}) VALUES ({placeholders}) '
                f'ON DUPLICATE KEY UPDATE {updates}'
            )
        else:
            insert_sql = f'INSERT INTO `{safe_table}` ({cols_str}) VALUES ({placeholders})'

        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            values = []
            for row in batch:
                row_values = []
                for col in source_columns:
                    val = row.get(col)
                    if val is not None and not isinstance(val, (int, float, bool)):
                        val = str(val)
                    row_values.append(val)
                values.append(tuple(row_values))
            cursor.executemany(insert_sql, values)

    def create_incremental_staging_table(
        self,
        cursor,
        table_name: str,
        columns: List[str],
        primer_rows: List[Dict[str, Any]],
        pk: Optional[str],
    ) -> Tuple[str, List[str]]:
        """Create a connection-local staging table for incremental merges."""
        safe_table = self.sanitize_name(table_name)
        staging_table = f'_{safe_table}_staging'

        if not self.mysql_table_exists(cursor, table_name):
            safe_columns = self.create_table(cursor, table_name, columns, primer_rows, pk=pk)
        else:
            safe_columns = [self.sanitize_name(c) for c in columns]

        cursor.execute(f'DROP TEMPORARY TABLE IF EXISTS `{staging_table}`')
        cursor.execute(f'CREATE TEMPORARY TABLE `{staging_table}` LIKE `{safe_table}`')
        return staging_table, safe_columns

    def merge_incremental_staging(
        self,
        cursor,
        live_table: str,
        staging_table: str,
        safe_columns: List[str],
        upsert_pk: Optional[str],
    ) -> None:
        """Merge staged incremental rows into the live table."""
        cols_str = ', '.join(f'`{c}`' for c in safe_columns)
        select_str = ', '.join(f'`{c}`' for c in safe_columns)
        safe_live = self.sanitize_name(live_table)

        if upsert_pk:
            safe_pk = self.sanitize_name(upsert_pk)
            updates = ', '.join(
                f'`{c}` = VALUES(`{c}`)' for c in safe_columns if c != safe_pk
            )
            sql = (
                f'INSERT INTO `{safe_live}` ({cols_str}) '
                f'SELECT {select_str} FROM `{staging_table}` '
                f'ON DUPLICATE KEY UPDATE {updates}'
            )
        else:
            sql = (
                f'INSERT INTO `{safe_live}` ({cols_str}) '
                f'SELECT {select_str} FROM `{staging_table}`'
            )
        cursor.execute(sql)
    
    def sync_table(self, conn, table_name: str,
                   incremental: Optional[Dict[str, Any]] = None,
                   state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Stream a single table from the API into MySQL.

        Memory model: peak = PRIME_BATCH rows (schema inference sample) plus
        one STREAM_BATCH being INSERTed. No full-table materialization on
        either side of the wire — the server streams JSON, we parse it
        incrementally via ijson.

        Modes:
          - Full refresh (default): CREATE a staging table + stream INSERT
            into staging. On clean completion, atomically swap staging into
            place with RENAME TABLE. A mid-stream failure leaves the current
            live table untouched.
          - Incremental: if `incremental` is {'field': <col>, 'pk': <col?>},
            pull WHERE field > last_watermark and stream the result into a
            temp staging table. After clean stream completion, merge staging
            into the live table and update `_sync_table_state` in one commit.
            With `pk`, UPSERT makes retries idempotent. Without `pk`, merge is
            append-only and assumes the source really is append-only.

          Watermark advances ONLY on clean stream completion. A mid-stream
          error keeps the previous watermark and never touches the live table,
          so the next run replays from the same starting point.
        """
        metrics: Dict[str, Any] = {
            "table": table_name, "mode": "full", "rows": 0, "bytes": 0,
            "fetch_sec": 0.0, "insert_sec": 0.0, "ok": False, "error": "",
            "watermark": None,
        }
        inc_field = incremental.get('field') if incremental else None
        inc_pk = incremental.get('pk') if incremental else None
        last_watermark = (state or {}).get('watermark') if incremental else None

        if inc_field:
            metrics["mode"] = "incremental" if last_watermark is not None else "incremental-initial"
        else:
            metrics["mode"] = "full"
        log_event(
            logging.INFO,
            'sync_table_started',
            'Table sync started',
            table=table_name,
            mode=metrics['mode'],
            incremental_field=inc_field,
            incremental_pk=inc_pk,
            last_watermark=last_watermark,
        )

        # --- Open the stream. Pre-stream failures (auth, 500 before body)
        # land on response.ok == False; parse the JSON error body so the
        # message matches what non-streaming callers see.
        log_event(logging.INFO, 'table_stream_opening', 'Opening table stream', table=table_name)
        t0 = time.perf_counter()
        try:
            response = self._open_table_stream(
                table_name,
                since_field=inc_field if last_watermark is not None else None,
                since_value=last_watermark,
            )
        except requests.exceptions.RequestException as e:
            metrics["error"] = str(e)[:200]
            log_event(
                logging.ERROR,
                'table_stream_connection_failed',
                'Table stream connection failed',
                table=table_name,
                error=str(e),
            )
            return metrics

        if not response.ok:
            try:
                body = response.json()
                err = body.get('error') or body
            except ValueError:
                err = response.text[:500] or f"HTTP {response.status_code}"
            metrics["error"] = f"HTTP {response.status_code}: {err}"[:200]
            log_event(
                logging.ERROR,
                'table_stream_http_failed',
                'Table stream returned an HTTP error',
                table=table_name,
                status_code=response.status_code,
                error=metrics['error'],
            )
            response.close()
            return metrics

        # --- Parse columns + prime schema-inference buffer. Holding PRIME_BATCH
        # rows is the entire memory cost of this sync relative to row count.
        try:
            table_stream = TableStream(response)
        except Exception as e:
            metrics["error"] = f"Stream init failed: {e}"[:200]
            log_event(
                logging.ERROR,
                'table_stream_parse_failed',
                'Failed to initialize table stream parser',
                table=table_name,
                error=metrics['error'],
            )
            response.close()
            return metrics

        columns = table_stream.columns
        if not columns:
            metrics["error"] = "no columns"
            log_event(
                logging.ERROR,
                'table_stream_no_columns',
                'Table stream response did not include columns',
                table=table_name,
            )
            table_stream.close()
            return metrics

        rows_iter = table_stream.iter_rows()
        primer: List[Dict[str, Any]] = list(itertools.islice(rows_iter, PRIME_BATCH))
        stream_exhausted = len(primer) < PRIME_BATCH
        metrics["fetch_sec"] = time.perf_counter() - t0

        # Server can set `error` in the trailer if the DB cursor fails mid-fetch.
        # If the stream finished inside the primer window, trailer is populated now.
        if stream_exhausted and table_stream.trailer.get('error'):
            err = str(table_stream.trailer['error'])[:200]
            partial = len(primer)
            metrics["error"] = err + (f" [partial={partial}]" if partial else "")
            log_event(
                logging.ERROR,
                'table_stream_server_failed',
                'Table stream ended with a server error',
                table=table_name,
                partial_rows=partial,
                error=err,
            )
            table_stream.close()
            return metrics

        if not primer:
            # Nothing to write. For incremental, keep the old watermark; for full
            # refresh, the existing table (if any) stays intact because we never
            # reached the DROP.
            metrics["ok"] = True
            metrics["watermark"] = last_watermark if incremental else None
            log_event(
                logging.INFO,
                'sync_table_empty',
                'Table sync had no rows to write',
                table=table_name,
                mode=metrics['mode'],
                fetch_seconds=round(metrics['fetch_sec'], 3),
                watermark=metrics['watermark'],
            )
            table_stream.close()
            return metrics

        log_event(
            logging.INFO,
            'sync_table_primed',
            'Table stream schema sample loaded',
            table=table_name,
            primer_rows=len(primer),
            fetch_seconds=round(metrics['fetch_sec'], 3),
            columns=len(columns),
        )

        cursor = conn.cursor()
        rows_written = 0
        max_watermark = last_watermark
        state_written = False
        try:
            target_table = table_name
            if inc_field:
                target_table, safe_columns = self.create_incremental_staging_table(
                    cursor, table_name, columns, primer, pk=inc_pk
                )
                conn.commit()
            else:
                target_table = f'{self.sanitize_name(table_name)}__staging'
                safe_columns = self.create_table(
                    cursor,
                    table_name,
                    columns,
                    primer,
                    pk=inc_pk,
                    target_table=target_table,
                )
                conn.commit()

            t_ins = time.perf_counter()

            def flush(batch: List[Dict[str, Any]]) -> None:
                """Insert a batch into the current target table.

                Incremental sync writes into a temp staging table first, so
                these per-batch commits never expose partial results in the
                live replica. Full refresh still writes directly to the live
                table and remains non-atomic until the full-refresh staging
                path is implemented.
                """
                nonlocal rows_written, max_watermark
                if not batch:
                    return
                if inc_field:
                    for r in batch:
                        v = r.get(inc_field)
                        if v is None:
                            continue
                        if max_watermark is None or v > max_watermark:
                            max_watermark = v
                self.insert_rows(
                    cursor, target_table, safe_columns, columns, batch,
                    upsert_pk=inc_pk,
                )
                conn.commit()
                rows_written += len(batch)
                log_event(
                    logging.INFO,
                    'sync_table_batch_inserted',
                    'Table sync batch inserted',
                    table=table_name,
                    mode=metrics['mode'],
                    rows_written=rows_written,
                    batch_rows=len(batch),
                )

            flush(primer)
            primer = []  # release the prime buffer before pulling the tail

            # Stream the remainder. We only enter this loop when the prime
            # window was full, i.e. the server still has more rows for us.
            if not stream_exhausted:
                batch: List[Dict[str, Any]] = []
                for row in rows_iter:
                    batch.append(row)
                    if len(batch) >= STREAM_BATCH:
                        flush(batch)
                        batch = []
                flush(batch)

            metrics["insert_sec"] = time.perf_counter() - t_ins
            metrics["rows"] = rows_written

            # Trailer is guaranteed populated here because iter_rows ran to
            # completion (the stream either ended naturally or the server
            # closed the body, which ijson reports as end-of-stream).
            trailer_error = table_stream.trailer.get('error')
            if trailer_error:
                err = str(trailer_error)[:200]
                metrics["error"] = f"{err} [partial={rows_written}]"
                log_event(
                    logging.ERROR,
                    'table_stream_server_failed',
                    'Table stream ended with a server error after partial writes',
                    table=table_name,
                    rows_written=rows_written,
                    error=err,
                )
                # Do NOT advance watermark — next run replays from the same
                # starting point so we don't permanently skip missing rows.
                return metrics

            if inc_field:
                self.merge_incremental_staging(
                    cursor,
                    live_table=table_name,
                    staging_table=target_table,
                    safe_columns=safe_columns,
                    upsert_pk=inc_pk,
                )
                if max_watermark is not None:
                    self.upsert_table_state(
                        conn,
                        table_name=table_name,
                        watermark=max_watermark,
                        watermark_field=inc_field,
                        pk_field=inc_pk,
                        last_rows=rows_written,
                    )
                    state_written = True
                else:
                    conn.commit()
            else:
                self.swap_full_refresh_tables(cursor, table_name, target_table)
                conn.commit()

            metrics["watermark"] = max_watermark if inc_field else None
            metrics["ok"] = True
            metrics["state_written"] = state_written
            log_event(
                logging.INFO,
                'sync_table_completed',
                'Table sync completed',
                table=table_name,
                mode=metrics['mode'],
                rows=rows_written,
                fetch_seconds=round(metrics['fetch_sec'], 3),
                insert_seconds=round(metrics['insert_sec'], 3),
                watermark=metrics['watermark'],
                state_written=state_written,
            )
            return metrics

        except Error as e:
            try:
                conn.rollback()
            except Error:
                pass
            metrics["error"] = f"{str(e)[:200]} [partial={rows_written}]"
            metrics["rows"] = rows_written
            log_event(
                logging.ERROR,
                'sync_table_mysql_failed',
                'MySQL error while syncing table',
                table=table_name,
                mode=metrics['mode'],
                rows_written=rows_written,
                error=str(e),
            )
            return metrics
        finally:
            cursor.close()
            table_stream.close()
    
    def sync(self, tables: List[str] = None, exclude: List[str] = None, start_index: int = 0,
             report_path: str = DEFAULT_REPORT_PATH,
             config_path: str = DEFAULT_CONFIG_PATH,
             state_path: str = DEFAULT_STATE_PATH,
             full_refresh: bool = False,
             lock_timeout: int = 0) -> dict:
        """Sync all or specified tables.

        Args:
            tables: Specific tables to sync (None = discover via /api/tables)
            exclude: Additional tables to exclude (merged with config `skip:`)
            start_index: Skip first N tables (0-based, for resuming)
            report_path: CSV path for per-table timing report
            config_path: YAML with `skip:` and `incremental:` sections
            state_path: legacy JSON watermark file to import once into MySQL metadata
            full_refresh: Ignore incremental config — drop and reload every table
            lock_timeout: Seconds to wait for the MySQL advisory run lock
        """
        started_at = datetime.now()
        log_event(
            logging.INFO,
            'sync_run_started',
            'Sync run started',
            api_url=self.api_url,
            mysql_host=self.mysql_config['host'],
            mysql_port=self.mysql_config['port'],
            mysql_database=self.mysql_config['database'],
            started_at=started_at.isoformat(timespec='seconds'),
            config_path=config_path,
            report_path=report_path,
            full_refresh=full_refresh,
            start_index=start_index,
        )
        
        # Test connections
        log_event(logging.INFO, 'api_connection_check_started', 'Checking API connection')
        if not self.check_api_connection():
            log_event(logging.ERROR, 'api_connection_check_failed', 'API connection check failed')
            return {"success": False, "error": "API connection failed"}
        log_event(logging.INFO, 'api_connection_check_ok', 'API connection check succeeded')
        
        log_event(logging.INFO, 'mysql_connection_check_started', 'Checking MySQL connection')
        if not self.check_mysql_connection():
            log_event(logging.ERROR, 'mysql_connection_check_failed', 'MySQL connection check failed')
            return {"success": False, "error": "MySQL connection failed"}
        log_event(logging.INFO, 'mysql_connection_check_ok', 'MySQL connection check succeeded')
        
        # Load policy
        cfg = load_sync_config(config_path)
        skip_set = set(cfg["skip"])
        incremental_cfg = {} if full_refresh else cfg["incremental"]
        log_event(
            logging.INFO,
            'sync_config_loaded',
            'Sync config loaded',
            config_path=config_path,
            skip_count=len(skip_set),
            incremental_count=len(incremental_cfg),
        )

        # Get tables
        if tables is None:
            log_event(logging.INFO, 'table_list_fetch_started', 'Fetching table list from API')
            tables = self.get_tables()

        exclude_set = set(exclude or []) | skip_set
        dropped = [t for t in tables if t in exclude_set]
        tables = [t for t in tables if t not in exclude_set]
        if dropped:
            log_event(
                logging.INFO,
                'tables_excluded',
                'Excluded configured tables from sync run',
                excluded_count=len(dropped),
                excluded_preview=sorted(dropped)[:8],
            )

        if not tables:
            log_event(logging.ERROR, 'sync_run_no_tables', 'No tables to sync')
            return {"success": False, "error": "No tables"}

        total_tables = len(tables)

        # Apply start_index to resume from a specific point
        if start_index > 0:
            if start_index >= len(tables):
                log_event(
                    logging.ERROR,
                    'sync_run_invalid_start_index',
                    'Start index is out of range',
                    start_index=start_index,
                    max_index=len(tables) - 1,
                )
                return {"success": False, "error": "Invalid start index"}
            log_event(
                logging.INFO,
                'sync_run_resume',
                'Resuming sync run from start index',
                start_index=start_index,
                skipped_tables=start_index,
            )
            tables = tables[start_index:]

        log_event(
            logging.INFO,
            'sync_run_tables_selected',
            'Tables selected for sync run',
            selected_tables=len(tables),
            total_tables=total_tables,
            start_index=start_index,
        )

        conn = mysql.connector.connect(**self.mysql_config)
        lock_acquired = False
        try:
            lock_name = self.build_run_lock_name()
            log_event(
                logging.INFO,
                'sync_run_lock_acquire_started',
                'Acquiring sync run lock',
                lock_name=lock_name,
                lock_timeout_seconds=lock_timeout,
            )
            lock_acquired = self.acquire_run_lock(conn, lock_timeout)
            if not lock_acquired:
                log_event(
                    logging.WARNING,
                    'sync_run_lock_busy',
                    'Another sync run is already in progress',
                    lock_name=lock_name,
                )
                return {"success": False, "error": "sync already running"}
            log_event(logging.INFO, 'sync_run_lock_acquired', 'Sync run lock acquired', lock_name=lock_name)

            self.ensure_metadata_tables(conn)
            imported = self.import_legacy_state(conn, state_path)
            state = self.load_table_states(conn)
            log_event(
                logging.INFO,
                'sync_state_loaded',
                'Sync state loaded',
                state_table=SYNC_STATE_TABLE,
                watermarks_loaded=len(state),
                legacy_imported=imported,
                legacy_state_path=state_path if imported else None,
            )

            # Sync
            success = 0
            failed = 0
            failed_tables = []

            all_metrics: List[Dict[str, Any]] = []
            for i, table in enumerate(tables, 1):
                global_idx = i + start_index
                inc_policy = incremental_cfg.get(table)
                table_state = state.get(table) if inc_policy else None
                log_event(
                    logging.INFO,
                    'sync_run_table_started',
                    'Starting table within sync run',
                    table=table,
                    table_index=global_idx,
                    total_tables=total_tables,
                    incremental=bool(inc_policy),
                )
                m = self.sync_table(conn, table, incremental=inc_policy, state=table_state)
                all_metrics.append(m)
                if m["ok"]:
                    if inc_policy and m.get("watermark") is not None:
                        state[table] = {
                            "watermark": m["watermark"],
                            "watermark_field": inc_policy["field"],
                            "pk": inc_policy.get("pk"),
                            "last_rows": m["rows"],
                            "updated_at": datetime.now().isoformat(timespec='seconds'),
                        }
                    success += 1
                else:
                    failed += 1
                    failed_tables.append(table)
        finally:
            if lock_acquired:
                try:
                    self.release_run_lock(conn)
                except Error as e:
                    log_event(
                        logging.WARNING,
                        'sync_run_lock_release_failed',
                        'Could not release sync run lock cleanly',
                        error=str(e),
                    )
            conn.close()

        # Write the per-table timing report. Pairs with tools/hiretrack-ops/
        # probe_sizes.py output.
        try:
            with open(report_path, "w", newline="") as fh:
                fields = ["table", "mode", "rows", "bytes", "fetch_sec",
                          "insert_sec", "ok", "watermark", "error"]
                w = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
                w.writeheader()
                for m in all_metrics:
                    w.writerow(m)
            log_event(
                logging.INFO,
                'sync_report_written',
                'Sync timing report written',
                report_path=report_path,
                table_count=len(all_metrics),
            )
        except Exception as e:
            log_event(
                logging.WARNING,
                'sync_report_write_failed',
                'Could not write sync timing report',
                report_path=report_path,
                error=str(e),
            )
        
        # Summary
        if failed:
            tables_arg = ' '.join(failed_tables)
            log_event(
                logging.ERROR,
                'sync_run_failed_tables',
                'One or more tables failed to sync',
                failed=failed,
                failed_tables=failed_tables,
                retry_command=f'python apps/sync-worker/sync_to_mysql.py --tables {tables_arg}',
            )
        
        # Show resume hint if interrupted mid-sync
        next_index = start_index + success + failed
        resume_command = None
        if next_index < total_tables:
            resume_command = f'python apps/sync-worker/sync_to_mysql.py --start-index {next_index}'

        finished_at = datetime.now()
        log_event(
            logging.INFO if failed == 0 else logging.WARNING,
            'sync_run_completed',
            'Sync run completed',
            success=success,
            failed=failed,
            failed_tables=failed_tables,
            total_tables=total_tables,
            next_index=next_index,
            resume_command=resume_command,
            started_at=started_at.isoformat(timespec='seconds'),
            finished_at=finished_at.isoformat(timespec='seconds'),
            duration_seconds=round((finished_at - started_at).total_seconds(), 3),
            mysql_host=self.mysql_config['host'],
            mysql_port=self.mysql_config['port'],
            mysql_database=self.mysql_config['database'],
            adminer_url='http://localhost:8080',
        )
        
        return {"success": True, "synced": success, "failed": failed, "failed_tables": failed_tables}


def main():
    parser = argparse.ArgumentParser(description='Sync HireTrack database to MySQL')
    parser.add_argument('--api', default=DEFAULT_API_URL, help='API URL')
    parser.add_argument('--host', default=DEFAULT_MYSQL_HOST, help='MySQL host')
    parser.add_argument('--port', default=DEFAULT_MYSQL_PORT, type=int, help='MySQL port')
    parser.add_argument('--user', default=DEFAULT_MYSQL_USER, help='MySQL user')
    parser.add_argument('--password', default=DEFAULT_MYSQL_PASSWORD, help='MySQL password')
    parser.add_argument('--database', default=DEFAULT_MYSQL_DATABASE, help='MySQL database')
    parser.add_argument('--tables', nargs='+',
                        help='Specific tables to sync')
    parser.add_argument('--exclude', nargs='+', default=[],
                        help='Additional tables to exclude on top of config `skip:`')
    parser.add_argument('--config', default=DEFAULT_CONFIG_PATH,
                        help='YAML with skip + incremental policy (default: %(default)s)')
    parser.add_argument('--state', default=DEFAULT_STATE_PATH,
                        help='Legacy JSON watermark file to import once into MySQL metadata')
    parser.add_argument('--full-refresh', action='store_true',
                        help='Ignore incremental config; reload every table end-to-end')
    parser.add_argument('--lock-timeout', type=int, default=0,
                        help='Seconds to wait for the MySQL advisory run lock (default: %(default)s)')
    parser.add_argument('--start-index', type=int, default=0, 
                        help='Resume from table index (0-based). Use to continue after interruption.')
    parser.add_argument('--api-user', default=os.environ.get('API_USERNAME'),
                        help='API username (if auth enabled)')
    parser.add_argument('--api-password', default=os.environ.get('API_PASSWORD'),
                        help='API password (if auth enabled)')
    parser.add_argument('--report', default=DEFAULT_REPORT_PATH,
                        help='CSV path for per-table timing report')
    parser.add_argument('--log-level', default=LOG_LEVEL,
                        help='Python logging level (default: %(default)s)')
    parser.add_argument('--log-format', default=LOG_FORMAT, choices=['json', 'text'],
                        help='Log output format (default: %(default)s)')
    
    args = parser.parse_args()
    configure_logging(args.log_level, args.log_format)

    # Fail fast if compose (or the operator) forgot to wire a required value.
    # Keeps misconfig from degrading into a partial / wrong-target sync.
    required = {
        '--api / API_URL': args.api,
        '--host / MYSQL_HOST': args.host,
        '--user / MYSQL_USER': args.user,
        '--password / MYSQL_PASSWORD': args.password,
        '--database / MYSQL_DATABASE': args.database,
        '--api-user / API_USERNAME': args.api_user,
        '--api-password / API_PASSWORD': args.api_password,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        parser.error('missing required config (set via env or flag): ' + ', '.join(missing))

    api_auth = (args.api_user, args.api_password)
    
    client = HireTrackMySQLSync(
        api_url=args.api,
        mysql_host=args.host,
        mysql_port=args.port,
        mysql_user=args.user,
        mysql_password=args.password,
        mysql_database=args.database,
        api_auth=api_auth
    )
    
    result = client.sync(
        tables=args.tables,
        exclude=args.exclude,
        start_index=args.start_index,
        report_path=args.report,
        config_path=args.config,
        state_path=args.state,
        full_refresh=args.full_refresh,
        lock_timeout=args.lock_timeout,
    )
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
