#!/usr/bin/env python3
"""
HireTrack Database Sync Client
Pulls data from the Sync API and stores in local SQLite database.
Run on Mac/Linux to create a local replica for BI tools.
"""

import requests
import sqlite3
import os
import sys
import argparse
from datetime import datetime
from typing import Optional, List

# ============ CONFIGURATION ============
DEFAULT_API_URL = "http://85.192.35.248:5003"
DEFAULT_DB_FILE = "hiretrack_replica.db"

# Tables to sync (None = all tables from API)
DEFAULT_TABLES = None  # or ['JOBS', 'EQLISTS', 'SORT', 'Hetype', 'Users']


class HireTrackSyncClient:
    def __init__(self, api_url: str, db_file: str, auth: tuple = None):
        self.api_url = api_url.rstrip('/')
        self.db_file = db_file
        self.auth = auth
        self.session = requests.Session()
        if auth:
            self.session.auth = auth
    
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
    
    def check_connection(self) -> bool:
        """Test API connection"""
        result = self.api_request('/health')
        return result.get('status') == 'healthy'
    
    def get_tables(self) -> List[str]:
        """Get list of all tables"""
        result = self.api_request('/api/tables')
        if 'error' in result:
            print(f"Error getting tables: {result['error']}")
            return []
        return result.get('tables', [])
    
    def get_table_count(self, table: str) -> int:
        """Get row count for a table"""
        result = self.api_request(f'/api/table/{table}/count')
        return result.get('count', 0)
    
    def get_table_data(self, table: str, limit: int = 10000, offset: int = 0) -> dict:
        """Get table data with pagination"""
        return self.api_request(f'/api/table/{table}?limit={limit}&offset={offset}')
    
    def sanitize_column_name(self, name: str) -> str:
        """Make column name safe for SQLite"""
        safe = ''.join(c if c.isalnum() or c == '_' else '_' for c in name)
        if safe and safe[0].isdigit():
            safe = '_' + safe
        return safe or '_col'
    
    def sync_table(self, conn: sqlite3.Connection, table: str) -> bool:
        """Sync a single table"""
        print(f"\n📋 Syncing: {table}")
        
        # Get total count
        total = self.get_table_count(table)
        print(f"   Rows: {total}")
        
        # Fetch all data (paginated)
        all_rows = []
        columns = None
        offset = 0
        batch_size = 10000
        
        while True:
            result = self.get_table_data(table, limit=batch_size, offset=offset)
            
            if 'error' in result:
                print(f"   ✗ Error: {result['error']}")
                return False
            
            if columns is None:
                columns = result.get('columns', [])
            
            rows = result.get('data', [])
            all_rows.extend(rows)
            
            print(f"   Fetching: {len(all_rows)}/{total}", end='\r')
            
            if len(rows) < batch_size:
                break
            offset += batch_size
        
        print(f"   Fetched: {len(all_rows)} rows")
        
        if not columns:
            print("   ✗ No columns")
            return False
        
        # Create SQLite table
        cursor = conn.cursor()
        safe_cols = [self.sanitize_column_name(c) for c in columns]
        
        cursor.execute(f'DROP TABLE IF EXISTS "{table}"')
        cols_def = ', '.join([f'"{c}" TEXT' for c in safe_cols])
        cursor.execute(f'CREATE TABLE "{table}" ({cols_def})')
        
        # Insert data
        if all_rows:
            placeholders = ', '.join(['?' for _ in safe_cols])
            for row in all_rows:
                values = [str(row.get(c, '') or '') for c in columns]
                cursor.execute(f'INSERT INTO "{table}" VALUES ({placeholders})', values)
        
        conn.commit()
        print(f"   ✓ Saved {len(all_rows)} rows")
        return True
    
    def sync(self, tables: List[str] = None) -> dict:
        """Sync all or specified tables"""
        print("=" * 50)
        print("HireTrack Database Sync")
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"API: {self.api_url}")
        print(f"Database: {self.db_file}")
        print("=" * 50)
        
        # Test connection
        print("\n🔌 Testing API connection...")
        if not self.check_connection():
            print("✗ Cannot connect to API")
            return {"success": False, "error": "Connection failed"}
        print("✓ Connected")
        
        # Get tables
        if tables is None:
            print("\n📊 Fetching table list...")
            tables = self.get_tables()
        
        if not tables:
            print("✗ No tables to sync")
            return {"success": False, "error": "No tables"}
        
        print(f"✓ {len(tables)} tables to sync")
        
        # Sync
        conn = sqlite3.connect(self.db_file)
        success = 0
        failed = 0
        
        for table in tables:
            if self.sync_table(conn, table):
                success += 1
            else:
                failed += 1
        
        conn.close()
        
        # Summary
        print("\n" + "=" * 50)
        print("SYNC COMPLETE")
        print(f"✓ Success: {success}")
        if failed:
            print(f"✗ Failed: {failed}")
        print(f"Database: {os.path.abspath(self.db_file)}")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 50)
        
        return {"success": True, "synced": success, "failed": failed}


def main():
    parser = argparse.ArgumentParser(description='Sync HireTrack database to local SQLite')
    parser.add_argument('--api', default=DEFAULT_API_URL, help='API URL')
    parser.add_argument('--db', default=DEFAULT_DB_FILE, help='Output SQLite file')
    parser.add_argument('--tables', nargs='+', help='Specific tables to sync')
    parser.add_argument('--user', help='API username (if auth enabled)')
    parser.add_argument('--password', help='API password (if auth enabled)')
    
    args = parser.parse_args()
    
    auth = None
    if args.user and args.password:
        auth = (args.user, args.password)
    
    client = HireTrackSyncClient(args.api, args.db, auth)
    result = client.sync(args.tables)
    
    sys.exit(0 if result['success'] else 1)


if __name__ == '__main__':
    main()
