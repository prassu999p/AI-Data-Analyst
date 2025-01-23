from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from langchain_agent import LangChainAgent
import logging
import json
from openai import OpenAI
from database_connection import DatabaseConnection, test_connection
from typing import Dict, Optional

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
    query: str
    connection: DatabaseConnection
    chart_type: Optional[str] = None
    color_palette: Optional[str] = "warm"

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

@app.post("/query")
async def handle_query(query: Query):
    try:
        logger.info(f"Processing query: {query.query}")
        
        # First test the database connection
        try:
            connection_result = await test_connection(query.connection)
            if connection_result["status"] != "success":
                raise HTTPException(status_code=400, detail="Database connection failed")
        except Exception as conn_error:
            logger.error(f"Database connection error: {str(conn_error)}")
            raise HTTPException(status_code=400, detail=str(conn_error))
        
        try:
            result = agent.process_query(query.query, query.connection)
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
                "visualization_data": chart_data,
                "suggested_chart": suggested_chart
            }
        }
        
        return response
        
    except Exception as e:
        logger.error(f"Exception in handle_query: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/test")
async def test_endpoint():
    """Test endpoint to verify API functionality"""
    test_queries = [
        "Show me total sales by product for 2023",
        "Display the monthly sales trend for Product A in 2023",
        "What is the market share distribution among our competitors?"
    ]
    
    results = []
    for query in test_queries:
        try:
            result = agent.process_query(query)
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
        "test_results": results
    }

@app.post("/test-connection")
async def test_db_connection(connection_data: DatabaseConnection) -> Dict:
    """
    Test database connection with provided credentials
    """
    try:
        result = await test_connection(connection_data)
        return result
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
