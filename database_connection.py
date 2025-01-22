from typing import Dict, Optional
import psycopg2
import pymysql
from pymongo import MongoClient
from pydantic import BaseModel
from fastapi import HTTPException

class DatabaseConnection(BaseModel):
    type: str
    host: str
    port: str
    database_name: str
    username: str
    password: str

async def test_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test database connection based on the provided credentials
    """
    try:
        if connection_data.type == "postgresql":
            return await test_postgresql_connection(connection_data)
        elif connection_data.type == "mysql":
            return await test_mysql_connection(connection_data)
        elif connection_data.type == "mongodb":
            return await test_mongodb_connection(connection_data)
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported database type: {connection_data.type}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Connection failed: {str(e)}"
        )

async def test_postgresql_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test PostgreSQL connection
    """
    try:
        conn = psycopg2.connect(
            host=connection_data.host,
            port=connection_data.port,
            database=connection_data.database_name,
            user=connection_data.username,
            password=connection_data.password
        )
        conn.close()
        return {"status": "success", "message": "Successfully connected to PostgreSQL database"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"PostgreSQL connection failed: {str(e)}"
        )

async def test_mysql_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test MySQL connection
    """
    try:
        conn = pymysql.connect(
            host=connection_data.host,
            port=int(connection_data.port),
            database=connection_data.database_name,
            user=connection_data.username,
            password=connection_data.password
        )
        conn.close()
        return {"status": "success", "message": "Successfully connected to MySQL database"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"MySQL connection failed: {str(e)}"
        )

async def test_mongodb_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test MongoDB connection
    """
    try:
        client = MongoClient(
            host=connection_data.host,
            port=int(connection_data.port),
            username=connection_data.username,
            password=connection_data.password,
            serverSelectionTimeoutMS=5000  # 5 second timeout
        )
        # Test connection by listing database names
        client.list_database_names()
        client.close()
        return {"status": "success", "message": "Successfully connected to MongoDB database"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"MongoDB connection failed: {str(e)}"
        ) 