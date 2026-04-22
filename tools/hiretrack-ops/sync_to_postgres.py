#!/usr/bin/env python3
"""
HireTrack Database Sync to PostgreSQL
Pulls data from the Sync API and stores in PostgreSQL database.
"""

import requests
import psycopg2
from psycopg2 import sql, Error
from psycopg2.extras import execute_values
import os
import sys
import argparse
import re
from datetime import datetime
from typing import List, Dict, Any

# ============ CONFIGURATION ============
DEFAULT_API_URL = os.environ.get('API_URL', 'http://100.100.139.110:5003')
DEFAULT_PG_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
DEFAULT_PG_PORT = int(os.environ.get('POSTGRES_PORT', 5432))
DEFAULT_PG_USER = os.environ.get('POSTGRES_USER', 'hiretrack')
DEFAULT_PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'hiretrack123')
DEFAULT_PG_DATABASE = os.environ.get('POSTGRES_DB', 'hiretrack')


class HireTrackPostgresSync:
    def __init__(
        self,
        api_url: str,
        pg_host: str,
        pg_port: int,
        pg_user: str,
        pg_password: str,
        pg_database: str,
        api_auth: tuple = None
    ):
        self.api_url = api_url.rstrip('/')
        self.pg_config = {
            'host': pg_host,
            'port': pg_port,
            'user': pg_user,
            'password': pg_password,
            'dbname': pg_database,
        }
        self.api_auth = api_auth
        self.session = requests.Session()
        if api_auth:
            self.session.auth = api_auth
    
    def api_request(self, endpoint: str, method: str = 'GET', data: dict = None) -> dict:
        """Make an API request"""
        url = f"{self.api_url}{endpoint}"
        try:
            if method == 'GET':
                response = self.session.get(url, timeout=120)
            else:
                response = self.session.post(url, json=data, timeout=120)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}
    
    def check_api_connection(self) -> bool:
        """Test API connection"""
        result = self.api_request('/health')
        return result.get('status') == 'healthy'
    
    def check_pg_connection(self) -> bool:
        """Test PostgreSQL connection"""
        try:
            conn = psycopg2.connect(**self.pg_config)
            conn.close()
            return True
        except Error as e:
            print(f"PostgreSQL connection error: {e}")
            return False
    
    def get_tables(self) -> List[str]:
        """Get list of all tables from API"""
        result = self.api_request('/api/tables')
        if 'error' in result:
            print(f"Error getting tables: {result['error']}")
            return []
        return result.get('tables', [])
    
    def get_table_count(self, table: str) -> int:
        """Get row count for a table"""
        result = self.api_request(f'/api/table/{table}/count')
        return result.get('count', 0)
    
    def get_table_data(self, table: str) -> dict:
        """Get all table data"""
        return self.api_request(f'/api/table/{table}')
    
    def sanitize_name(self, name: str) -> str:
        """Make column/table name safe for PostgreSQL"""
        safe = re.sub(r'[^a-zA-Z0-9_]', '_', name.lower())
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe or '_col'
    
    def infer_pg_type(self, value: Any) -> str:
        """Infer PostgreSQL column type from Python value"""
        if value is None:
            return 'TEXT'
        if isinstance(value, bool):
            return 'BOOLEAN'
        if isinstance(value, int):
            if -2147483648 <= value <= 2147483647:
                return 'INTEGER'
            return 'BIGINT'
        if isinstance(value, float):
            return 'DOUBLE PRECISION'
        if isinstance(value, str):
            length = len(value)
            if length <= 255:
                return 'VARCHAR(255)'
            return 'TEXT'
        return 'TEXT'
    
    def create_table(self, cursor, table_name: str, columns: List[str], sample_row: Dict) -> List[str]:
        """Create PostgreSQL table with inferred types"""
        safe_table = self.sanitize_name(table_name)
        safe_columns = []
        col_definitions = []
        
        for col in columns:
            safe_col = self.sanitize_name(col)
            safe_columns.append(safe_col)
            sample_value = sample_row.get(col) if sample_row else None
            col_type = self.infer_pg_type(sample_value)
            col_definitions.append(f'"{safe_col}" {col_type}')
        
        col_definitions.append('"_synced_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP')
        
        cursor.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(safe_table)))
        create_sql = f'CREATE TABLE "{safe_table}" ({", ".join(col_definitions)})'
        cursor.execute(create_sql)
        
        return safe_columns
    
    def sync_table(self, conn, table_name: str) -> bool:
        """Sync a single table from API to PostgreSQL"""
        print(f"\n📋 Syncing: {table_name}")
        
        total = self.get_table_count(table_name)
        print(f"   Rows: {total}")
        
        if total == 0:
            print("   ⏭️  Empty table, skipping")
            return True
        
        print("   Fetching...", end='\r')
        result = self.get_table_data(table_name)
        
        if 'error' in result:
            print(f"   ✗ Error: {result['error']}")
            return False
        
        columns = result.get('columns', [])
        all_rows = result.get('data', [])
        print(f"   Fetched: {len(all_rows)} rows    ")
        
        if not columns:
            print("   ✗ No columns")
            return False
        
        cursor = conn.cursor()
        
        try:
            sample_row = all_rows[0] if all_rows else {}
            safe_columns = self.create_table(cursor, table_name, columns, sample_row)
            
            safe_table = self.sanitize_name(table_name)
            cols_str = ', '.join([f'"{c}"' for c in safe_columns])
            insert_sql = f'INSERT INTO "{safe_table}" ({cols_str}) VALUES %s'
            
            # Prepare all values (strip NUL bytes from strings)
            values = []
            for row in all_rows:
                row_values = []
                for col in columns:
                    val = row.get(col)
                    if val is not None and not isinstance(val, (int, float, bool)):
                        val = str(val).replace('\x00', '')  # Strip NUL bytes
                    row_values.append(val)
                values.append(tuple(row_values))
            
            # Bulk insert using execute_values (much faster than executemany)
            batch_size = 5000
            for i in range(0, len(values), batch_size):
                batch = values[i:i + batch_size]
                execute_values(cursor, insert_sql, batch, page_size=batch_size)
                print(f"   Inserting: {min(i + batch_size, len(values))}/{len(values)}", end='\r')
            
            conn.commit()
            print(f"   ✓ Saved {len(all_rows)} rows    ")
            return True
            
        except Error as e:
            print(f"   ✗ PostgreSQL Error: {e}")
            conn.rollback()
            return False
        finally:
            cursor.close()
    
    def sync(self, tables: List[str] = None) -> dict:
        """Sync all or specified tables"""
        print("=" * 50)
        print("HireTrack Database Sync to PostgreSQL")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"API: {self.api_url}")
        print(f"PostgreSQL: {self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['dbname']}")
        print("=" * 50)
        
        print("\n🔌 Testing API connection...")
        if not self.check_api_connection():
            print("✗ Cannot connect to API")
            return {"success": False, "error": "API connection failed"}
        print("✓ API connected")
        
        print("\n🔌 Testing PostgreSQL connection...")
        if not self.check_pg_connection():
            print("✗ Cannot connect to PostgreSQL")
            return {"success": False, "error": "PostgreSQL connection failed"}
        print("✓ PostgreSQL connected")
        
        if tables is None:
            print("\n📊 Fetching table list...")
            tables = self.get_tables()
        
        if not tables:
            print("✗ No tables to sync")
            return {"success": False, "error": "No tables"}
        
        print(f"✓ {len(tables)} tables to sync")
        
        conn = psycopg2.connect(**self.pg_config)
        success = 0
        failed = 0
        
        for table in tables:
            if self.sync_table(conn, table):
                success += 1
            else:
                failed += 1
        
        conn.close()
        
        print("\n" + "=" * 50)
        print("SYNC COMPLETE")
        print(f"✓ Success: {success}")
        if failed:
            print(f"✗ Failed: {failed}")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        print("\n📖 Connect to PostgreSQL:")
        print(f"   psql -h {self.pg_config['host']} -p {self.pg_config['port']} -U {self.pg_config['user']} -d {self.pg_config['dbname']}")
        print("\n   Or open Adminer: http://localhost:8080")
        print("\n   Superset connection string:")
        print(f"   postgresql://{self.pg_config['user']}:{self.pg_config['password']}@hiretrack-postgres:5432/{self.pg_config['dbname']}")
        
        return {"success": True, "synced": success, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description='Sync HireTrack database to PostgreSQL')
    parser.add_argument('--api', default=DEFAULT_API_URL, help='API URL')
    parser.add_argument('--host', default=DEFAULT_PG_HOST, help='PostgreSQL host')
    parser.add_argument('--port', default=DEFAULT_PG_PORT, type=int, help='PostgreSQL port')
    parser.add_argument('--user', default=DEFAULT_PG_USER, help='PostgreSQL user')
    parser.add_argument('--password', default=DEFAULT_PG_PASSWORD, help='PostgreSQL password')
    parser.add_argument('--database', default=DEFAULT_PG_DATABASE, help='PostgreSQL database')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync')
    parser.add_argument('--api-user', help='API username (if auth enabled)')
    parser.add_argument('--api-password', help='API password (if auth enabled)')
    
    args = parser.parse_args()
    
    api_auth = None
    if args.api_user and args.api_password:
        api_auth = (args.api_user, args.api_password)
    
    client = HireTrackPostgresSync(
        api_url=args.api,
        pg_host=args.host,
        pg_port=args.port,
        pg_user=args.user,
        pg_password=args.password,
        pg_database=args.database,
        api_auth=api_auth
    )
    
    result = client.sync(args.tables)
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
