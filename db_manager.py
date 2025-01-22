from langchain_community.utilities import SQLDatabase
from typing import Dict, Optional
import sqlite3
import json

class DatabaseConnectionManager:
    def __init__(self):
        """Initialize the connection manager with a local SQLite database to store connections"""
        self.connections: Dict[str, dict] = {}
        self.db_path = "connections.db"
        self._init_connection_store()
    
    def _init_connection_store(self):
        """Initialize the SQLite database to store connection information"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS connections (
                id TEXT PRIMARY KEY,
                name TEXT,
                type TEXT,
                host TEXT,
                port INTEGER,
                database TEXT,
                username TEXT,
                password TEXT,
                uri TEXT
            )
        ''')
        conn.commit()
        conn.close()
        self._load_connections()
    
    def _load_connections(self):
        """Load all connections from SQLite database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM connections')
        rows = cursor.fetchall()
        conn.close()
        
        for row in rows:
            connection_id = row[0]
            self.connections[connection_id] = {
                'id': row[0],
                'name': row[1],
                'type': row[2],
                'host': row[3],
                'port': row[4],
                'database': row[5],
                'username': row[6],
                'password': row[7],
                'uri': row[8]
            }

    def add_connection(self, connection_details: dict) -> str:
        """Add a new database connection"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO connections (id, name, type, host, port, database, username, password, uri)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            connection_details['id'],
            connection_details['name'],
            connection_details['type'],
            connection_details['host'],
            connection_details['port'],
            connection_details['database'],
            connection_details['username'],
            connection_details['password'],
            connection_details['uri']
        ))
        
        conn.commit()
        conn.close()
        
        self.connections[connection_details['id']] = connection_details
        return connection_details['id']

    def get_connection(self, connection_id: str) -> Optional[dict]:
        """Get connection details by ID"""
        return self.connections.get(connection_id)

    def get_langchain_db(self, connection_id: str) -> Optional[SQLDatabase]:
        """Get a LangChain SQLDatabase instance for the given connection ID"""
        connection = self.get_connection(connection_id)
        if not connection:
            return None
        
        return SQLDatabase.from_uri(
            connection['uri'],
            sample_rows_in_table_info=3
        )

    def list_connections(self) -> list:
        """List all available connections"""
        return list(self.connections.values())

    def remove_connection(self, connection_id: str) -> bool:
        """Remove a database connection"""
        if connection_id not in self.connections:
            return False
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('DELETE FROM connections WHERE id = ?', (connection_id,))
        conn.commit()
        conn.close()
        
        del self.connections[connection_id]
        return True

    def test_connection(self, connection_details: dict) -> bool:
        """Test if a connection can be established"""
        try:
            db = SQLDatabase.from_uri(connection_details['uri'])
            # Try a simple query to verify connection
            db.run('SELECT 1')
            return True
        except Exception:
            return False 