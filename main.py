from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langchain_agent import LangChainAgent
from typing import Optional, List
import logging
import json
from openai import OpenAI
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "https://storied-tartufo-11d3ec.netlify.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize OpenAI client
client = OpenAI()

class Query(BaseModel):
    text: str
    connection_id: str
    chart_type: Optional[str] = None

class DatabaseConnection(BaseModel):
    name: str
    type: str
    host: str
    port: int
    database: str
    username: str
    password: str

class TestConnection(BaseModel):
    connection_id: str

# Initialize LangChain agent
agent = LangChainAgent()

def suggest_chart_type(api_text: str) -> str:
    """Suggest the best chart type based on the data"""
    prompt = f"""
    Based on the following data, suggest the best chart type (bar, line, area, pie, donut, card):
    {api_text}
    
    Return only the chart type name in lowercase.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2
        )
        return response.choices[0].message.content.strip().lower()
    except Exception as e:
        logger.error(f"Error suggesting chart type: {str(e)}")
        return "card"  # Fallback to card display

def parse_text_to_json(final_answer: str, chart_type: str):
    """Parse text response into ECharts-compatible JSON format"""
    prompt = f"""
    Parse the following text into a JSON format suitable for ECharts {chart_type} chart.
    Extract numerical values and their corresponding labels.
    Use this schema:
    {{
      "xAxis": {{ "type": "category", "data": [] }},
      "yAxis": {{ "type": "value" }},
      "series": [{{ "data": [], "type": "{chart_type}" }}]
    }}
    
    For pie/donut charts, use:
    {{
      "series": [
        {{
          "type": "pie",
          "data": [{{"value": 0, "name": "label"}}]
        }}
      ]
    }}
    
    For card display, use:
    {{
      "type": "card",
      "content": "text summary"
    }}
    
    Text: {final_answer}
    """
    try:
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1
        )
        json_output = response.choices[0].message.content
        
        # Validate JSON
        parsed = json.loads(json_output)
        return parsed
    except json.JSONDecodeError:
        logger.error("Invalid JSON generated by LLM")
        return {"error": "Failed to generate valid chart data"}
    except Exception as e:
        logger.error(f"Error parsing text to JSON: {str(e)}")
        return {"error": str(e)}

@app.post("/connections")
async def create_connection(connection: DatabaseConnection):
    """Create a new database connection"""
    try:
        # Generate connection URI based on database type
        if connection.type.lower() == "postgresql":
            uri = f"postgresql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        elif connection.type.lower() == "mysql":
            uri = f"mysql+pymysql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type")

        # Create connection details
        connection_id = str(uuid.uuid4())
        connection_details = {
            "id": connection_id,
            "name": connection.name,
            "type": connection.type,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.username,
            "password": connection.password,
            "uri": uri
        }

        # Test the connection
        if not agent.db_manager.test_connection(connection_details):
            raise HTTPException(status_code=400, detail="Failed to connect to database")

        # Add the connection
        agent.db_manager.add_connection(connection_details)
        
        # Return connection details without sensitive information
        return {
            "data": {
                "id": connection_id,
                "name": connection.name,
                "type": connection.type,
                "host": connection.host,
                "port": connection.port,
                "database": connection.database,
                "username": connection.username
            }
        }

    except Exception as e:
        logger.error(f"Error creating connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/connections")
async def list_connections():
    """List all database connections"""
    try:
        connections = agent.db_manager.list_connections()
        # Remove sensitive information and format response
        formatted_connections = []
        for conn in connections:
            formatted_connections.append({
                "id": conn["id"],
                "name": conn["name"],
                "type": conn["type"],
                "host": conn["host"],
                "port": conn["port"],
                "database": conn["database"],
                "username": conn["username"]
            })
        return {"data": formatted_connections}
    except Exception as e:
        logger.error(f"Error listing connections: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/connections/{connection_id}")
async def delete_connection(connection_id: str):
    """Delete a database connection"""
    try:
        if agent.db_manager.remove_connection(connection_id):
            return {"message": "Connection deleted successfully"}
        raise HTTPException(status_code=404, detail="Connection not found")
    except Exception as e:
        logger.error(f"Error deleting connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
async def handle_query(query: Query):
    try:
        logger.info(f"Processing query: {query.text}")
        logger.info(f"Using connection: {query.connection_id}")
        
        try:
            result = agent.process_query(query.text, query.connection_id)
        except Exception as agent_error:
            logger.error(f"Error in agent processing: {str(agent_error)}")
            return {
                "status": "error",
                "message": "Failed to process query",
                "error": str(agent_error)
            }
            
        if not result or not isinstance(result, dict):
            raise HTTPException(status_code=500, detail="Invalid response from agent")
            
        logger.info(f"Query processed. Status: {result.get('status')}")
        
        final_answer = result.get("llm_analysis", {}).get("final_answer")
        suggested_chart = suggest_chart_type(final_answer)
        
        # Use suggested chart if none was specified
        chart_type = query.chart_type or suggested_chart
        chart_data = parse_text_to_json(final_answer, chart_type)
        
        response = {
            "status": "success",
            "data": {
                "answer": final_answer,
                "sql_query": result.get("sql_data", {}).get("query"),
                "sql_results": result.get("sql_data", {}).get("results"),
                "chart_data": chart_data,
                "suggested_chart": suggested_chart
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Exception in handle_query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/connections/test")
async def test_connection(test_data: TestConnection):
    """Test a specific database connection"""
    try:
        connection = agent.db_manager.get_connection(test_data.connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Test the connection
        if agent.db_manager.test_connection(connection):
            # Try a simple test query
            test_result = agent.process_query("Show me the list of tables", test_data.connection_id)
            return {
                "status": "success",
                "message": "Connection test successful",
                "test_query_result": test_result
            }
        else:
            raise HTTPException(status_code=400, detail="Failed to connect to database")

    except Exception as e:
        logger.error(f"Error testing connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/connections/verify")
async def verify_connection(connection: DatabaseConnection):
    """Verify a database connection before creating it"""
    try:
        # Generate connection URI based on database type
        if connection.type.lower() == "postgresql":
            uri = f"postgresql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        elif connection.type.lower() == "mysql":
            uri = f"mysql+pymysql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type")

        # Create temporary connection details for testing
        connection_details = {
            "id": "temp",
            "name": connection.name,
            "type": connection.type,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.username,
            "password": connection.password,
            "uri": uri
        }

        # Test the connection
        if agent.db_manager.test_connection(connection_details):
            return {"status": "success", "message": "Connection verification successful"}
        else:
            raise HTTPException(status_code=400, detail="Failed to connect to database")

    except Exception as e:
        logger.error(f"Error verifying connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test")
async def test_endpoint():
    """Test endpoint to verify API functionality"""
    try:
        # Get the first available connection
        connections = agent.db_manager.list_connections()
        if not connections:
            return {
                "status": "error",
                "message": "No database connections available for testing"
            }
        
        test_connection = connections[0]
        test_queries = [
            "Show me total sales by product for 2023",
            "Display the monthly sales trend for Product A in 2023",
            "What is the market share distribution among our competitors?"
        ]
        
        results = []
        for query in test_queries:
            try:
                result = agent.process_query(query, test_connection['id'])
                final_answer = result.get("llm_analysis", {}).get("final_answer")
                suggested_chart = suggest_chart_type(final_answer)
                chart_data = parse_text_to_json(final_answer, suggested_chart)
                
                results.append({
                    "query": query,
                    "status": result.get("status"),
                    "answer": final_answer,
                    "has_sql": bool(result.get("sql_data", {}).get("query")),
                    "has_results": bool(result.get("sql_data", {}).get("results")),
                    "suggested_chart": suggested_chart,
                    "chart_data": chart_data,
                    "error": None
                })
            except Exception as e:
                results.append({
                    "query": query,
                    "status": "error",
                    "answer": None,
                    "has_sql": False,
                    "has_results": False,
                    "suggested_chart": None,
                    "chart_data": None,
                    "error": str(e)
                })
        
        return {
            "status": "success",
            "connection_used": test_connection['name'],
            "test_results": results
        }
    except Exception as e:
        logger.error(f"Error in test endpoint: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

@app.get("/connections/{connection_id}")
async def get_connection(connection_id: str):
    """Get a specific database connection"""
    try:
        connection = agent.db_manager.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        # Return connection details without sensitive information
        return {
            "data": {
                "id": connection["id"],
                "name": connection["name"],
                "type": connection["type"],
                "host": connection["host"],
                "port": connection["port"],
                "database": connection["database"],
                "username": connection["username"]
            }
        }
    except Exception as e:
        logger.error(f"Error getting connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/connections/{connection_id}")
async def update_connection(connection_id: str, connection: DatabaseConnection):
    """Update a database connection"""
    try:
        # Check if connection exists
        existing_connection = agent.db_manager.get_connection(connection_id)
        if not existing_connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Generate connection URI based on database type
        if connection.type.lower() == "postgresql":
            uri = f"postgresql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        elif connection.type.lower() == "mysql":
            uri = f"mysql+pymysql://{connection.username}:{connection.password}@{connection.host}:{connection.port}/{connection.database}"
        else:
            raise HTTPException(status_code=400, detail="Unsupported database type")

        # Create updated connection details
        connection_details = {
            "id": connection_id,
            "name": connection.name,
            "type": connection.type,
            "host": connection.host,
            "port": connection.port,
            "database": connection.database,
            "username": connection.username,
            "password": connection.password,
            "uri": uri
        }

        # Test the connection
        if not agent.db_manager.test_connection(connection_details):
            raise HTTPException(status_code=400, detail="Failed to connect to database")

        # Update the connection
        agent.db_manager.remove_connection(connection_id)
        agent.db_manager.add_connection(connection_details)
        
        # Return updated connection details without sensitive information
        return {
            "data": {
                "id": connection_id,
                "name": connection.name,
                "type": connection.type,
                "host": connection.host,
                "port": connection.port,
                "database": connection.database,
                "username": connection.username
            }
        }

    except Exception as e:
        logger.error(f"Error updating connection: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/connections/test")
async def test_connection(conn: DBConnectionTest):
    """Test a database connection"""
    try:
        DatabaseManager.test_connection(
            host=conn.host,
            port=conn.port,
            database=conn.database,
            username=conn.username,
            password=conn.password
        )
        return {"status": "success", "message": "Connection test successful"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/connections")
async def add_connection(conn: DBConnectionCreate):
    """Add a new database connection"""
    try:
        result = DatabaseManager.add_connection(
            name=conn.name,
            host=conn.host,
            port=conn.port,
            database=conn.database,
            username=conn.username,
            password=conn.password
        )
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/connections")
async def list_connections():
    """List all database connections"""
    try:
        connections = DatabaseManager.get_connections()
        return {"status": "success", "data": connections}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/connections/{connection_id}")
async def get_connection(connection_id: int):
    """Get a specific database connection"""
    try:
        connection = DatabaseManager.get_connection(connection_id)
        return {"status": "success", "data": connection}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.delete("/connections/{connection_id}")
async def delete_connection(connection_id: int):
    """Delete a database connection"""
    try:
        result = DatabaseManager.delete_connection(connection_id)
        return {"status": "success", "data": result}
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
