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
=======
from fastapi import HTTPException
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import json
from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv
import psycopg2

# Load environment variables
load_dotenv()

# Initialize encryption key
ENCRYPTION_KEY = os.getenv('ENCRYPTION_KEY', Fernet.generate_key())
cipher_suite = Fernet(ENCRYPTION_KEY)

# Create SQLAlchemy Base
Base = declarative_base()

class DBConnection(Base):
    __tablename__ = 'db_connections'
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    host = Column(String, nullable=False)
    port = Column(Integer, nullable=False)
    database = Column(String, nullable=False)
    username = Column(String, nullable=False)
    password = Column(String, nullable=False)  # Stored encrypted
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used = Column(DateTime, default=datetime.utcnow)

# Initialize SQLite database for storing connections
SQLITE_DATABASE_URL = "sqlite:///./connections.db"
engine = create_engine(SQLITE_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create tables
Base.metadata.create_all(bind=engine)

class DatabaseManager:
    @staticmethod
    def encrypt_password(password: str) -> bytes:
        return cipher_suite.encrypt(password.encode())

    @staticmethod
    def decrypt_password(encrypted_password: bytes) -> str:
        return cipher_suite.decrypt(encrypted_password).decode()

    @staticmethod
    def test_connection(host: str, port: int, database: str, username: str, password: str) -> bool:
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            conn.close()
            return True
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Connection test failed: {str(e)}")

    @staticmethod
    def create_connection_string(host: str, port: int, database: str, username: str, password: str) -> str:
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"

    @staticmethod
    def add_connection(
        name: str,
        host: str,
        port: int,
        database: str,
        username: str,
        password: str
    ) -> dict:
        # Test the connection first
        DatabaseManager.test_connection(host, port, database, username, password)
        
        db = SessionLocal()
        try:
            # Check if connection name already exists
            existing = db.query(DBConnection).filter(DBConnection.name == name).first()
            if existing:
                raise HTTPException(status_code=400, detail="Connection name already exists")
            
            # Encrypt password
            encrypted_password = DatabaseManager.encrypt_password(password)
            
            # Create new connection
            db_conn = DBConnection(
                name=name,
                host=host,
                port=port,
                database=database,
                username=username,
                password=encrypted_password
            )
            
            db.add(db_conn)
            db.commit()
            db.refresh(db_conn)
            
            return {
                "id": db_conn.id,
                "name": db_conn.name,
                "host": db_conn.host,
                "port": db_conn.port,
                "database": db_conn.database,
                "username": db_conn.username
            }
        finally:
            db.close()

    @staticmethod
    def get_connections() -> list:
        db = SessionLocal()
        try:
            connections = db.query(DBConnection).all()
            return [{
                "id": conn.id,
                "name": conn.name,
                "host": conn.host,
                "port": conn.port,
                "database": conn.database,
                "username": conn.username,
                "created_at": conn.created_at.isoformat(),
                "last_used": conn.last_used.isoformat()
            } for conn in connections]
        finally:
            db.close()

    @staticmethod
    def get_connection(connection_id: int) -> dict:
        db = SessionLocal()
        try:
            conn = db.query(DBConnection).filter(DBConnection.id == connection_id).first()
            if not conn:
                raise HTTPException(status_code=404, detail="Connection not found")
            
            # Decrypt password
            password = DatabaseManager.decrypt_password(conn.password)
            
            return {
                "id": conn.id,
                "name": conn.name,
                "host": conn.host,
                "port": conn.port,
                "database": conn.database,
                "username": conn.username,
                "password": password
            }
        finally:
            db.close()

    @staticmethod
    def delete_connection(connection_id: int):
        db = SessionLocal()
        try:
            conn = db.query(DBConnection).filter(DBConnection.id == connection_id).first()
            if not conn:
                raise HTTPException(status_code=404, detail="Connection not found")
            
            db.delete(conn)
            db.commit()
            return {"message": "Connection deleted successfully"}
        finally:
            db.close() 
