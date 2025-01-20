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