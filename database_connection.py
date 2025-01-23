from typing import Dict, Optional
import psycopg2
import pymysql
from pymongo import MongoClient
from pydantic import BaseModel
from fastapi import HTTPException

class SSLConfig(BaseModel):
    rejectUnauthorized: bool = False
    sslmode: str = "require"

class DatabaseConnection(BaseModel):
    type: str
    host: str
    port: str
    database_name: str
    username: str
    password: str
    ssl: Optional[SSLConfig] = None

async def test_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test database connection based on the provided credentials
    """
    try:
        if connection_data.type.lower() == "postgresql":
            return await test_postgresql_connection(connection_data)
        elif connection_data.type.lower() == "mysql":
            return await test_mysql_connection(connection_data)
        elif connection_data.type.lower() == "mongodb":
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
    Test PostgreSQL connection with SSL support
    """
    try:
        # Build connection string with SSL parameters
        conn_params = {
            "host": connection_data.host,
            "port": connection_data.port,
            "database": connection_data.database_name,
            "user": connection_data.username,
            "password": connection_data.password,
            "sslmode": "require",  # Required for Render.com
            "connect_timeout": 10   # Add timeout
        }

        print(f"Attempting PostgreSQL connection with params: {conn_params}")
        
        # Try to establish connection
        try:
            conn = psycopg2.connect(**conn_params)
            
            # Test the connection by executing a simple query
            with conn.cursor() as cur:
                cur.execute('SELECT version()')
                version = cur.fetchone()[0]
                print(f"Successfully connected to PostgreSQL: {version}")
            
            conn.close()
            return {
                "status": "success",
                "message": "Successfully connected to PostgreSQL database",
                "version": version
            }
        except psycopg2.OperationalError as e:
            error_msg = str(e).strip()
            print(f"PostgreSQL OperationalError: {error_msg}")
            
            # Provide more specific error messages
            if "password authentication failed" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="Authentication failed: Please check your username and password"
                )
            elif "SSL/TLS required" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="SSL connection required. The database requires a secure connection"
                )
            elif "connection timed out" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="Connection timed out: Please check your host and port settings"
                )
            elif "database" in error_msg and "does not exist" in error_msg:
                raise HTTPException(
                    status_code=400,
                    detail="Database does not exist: Please check your database name"
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Connection failed: {error_msg}"
                )
                
    except Exception as e:
        error_msg = str(e)
        print(f"PostgreSQL connection error: {error_msg}")
        
        raise HTTPException(
            status_code=400,
            detail=f"PostgreSQL connection failed: {error_msg}"
        )

async def test_mysql_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test MySQL connection with SSL support
    """
    try:
        conn_params = {
            "host": connection_data.host,
            "port": int(connection_data.port),
            "database": connection_data.database_name,
            "user": connection_data.username,
            "password": connection_data.password,
        }

        # Add SSL parameters if SSL is configured
        if connection_data.ssl:
            conn_params["ssl"] = {}
            if not connection_data.ssl.rejectUnauthorized:
                conn_params["ssl"]["verify_cert"] = False

        conn = pymysql.connect(**conn_params)
        conn.close()
        return {"status": "success", "message": "Successfully connected to MySQL database"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"MySQL connection failed: {str(e)}"
        )

async def test_mongodb_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test MongoDB connection with SSL support
    """
    try:
        conn_params = {
            "host": connection_data.host,
            "port": int(connection_data.port),
            "username": connection_data.username,
            "password": connection_data.password,
            "serverSelectionTimeoutMS": 5000  # 5 second timeout
        }

        # Add SSL parameters if SSL is configured
        if connection_data.ssl:
            conn_params["ssl"] = True
            if not connection_data.ssl.rejectUnauthorized:
                conn_params["tlsAllowInvalidCertificates"] = True

        client = MongoClient(**conn_params)
        # Test connection by listing database names
        client.list_database_names()
        client.close()
        return {"status": "success", "message": "Successfully connected to MongoDB database"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"MongoDB connection failed: {str(e)}"
        ) 