from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI
from langchain_community.tools import (
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
    QuerySQLDatabaseTool
)
from langchain.prompts import PromptTemplate
from langchain.chains import create_sql_query_chain
from langchain.memory import ConversationBufferMemory
from dotenv import load_dotenv
import pandas as pd
import os
from tools.query_sql_viz_tool import QuerySQLDatabaseForVizTool
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_core.tools import BaseTool
from typing import List, Dict, Any, Union, Optional
import re
import json

# Load environment variables
load_dotenv()

class VisualizationSQLDatabaseToolkit(SQLDatabaseToolkit):
    """Extended SQL Database toolkit that includes visualization-specific tools."""
    
    def get_tools(self) -> List[BaseTool]:
        """Get the tools in the toolkit."""
        tools = super().get_tools()
        viz_tool = QuerySQLDatabaseForVizTool(db=self.db)
        tools.append(viz_tool)
        return tools

class EnhancedDatabaseManager:
    def __init__(self):
        """Initialize the database manager with all necessary tools and chains"""
        # Database connection
        self.db = SQLDatabase.from_uri(
            os.getenv("DATABASE_URL"),
            include_tables=['sales'],  # Specify your tables
            sample_rows_in_table_info=3
        )
        
        # Initialize the language model
        self.llm = ChatOpenAI(
            temperature=0,
            model_name="gpt-3.5-turbo",
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )

        # Initialize memory
        self.memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )

        # Create the agent executor with VisualizationSQLDatabaseToolkit instead
        self.agent_executor = create_sql_agent(
            llm=self.llm,
            toolkit=VisualizationSQLDatabaseToolkit(db=self.db, llm=self.llm),
            verbose=True,
            agent_kwargs={
                "handle_parsing_errors": True
            }
        )

    def process_query(self, user_query: str) -> dict:
        """Process a natural language query and return both LLM analysis and visualization data"""
        try:
            # Get LLM reasoning and SQL execution
            agent_result = self.agent_executor.invoke({
                "input": user_query,
                "chat_history": self.memory.chat_memory.messages if self.memory else []
            })
            
            # Enhanced debugging
            print("\nFull Agent Result Structure:")
            print("Keys available:", agent_result.keys())
            print("Intermediate Steps:", agent_result.get("intermediate_steps", "Not found"))
            
            # Debug logging
            print("\nDebug - Agent Result:", json.dumps(agent_result, indent=2, default=str))
            
            # Extract SQL query with better error handling
            sql_query = None
            try:
                sql_query = self._extract_sql_query(agent_result)
                if not sql_query:
                    raise ValueError("Extracted SQL query is empty")
            except Exception as e:
                print(f"SQL Extraction failed: {str(e)}")
                # Try to extract from raw string
                raw_text = str(agent_result)
                matches = re.findall(r"SELECT.*?(?=\[|$)", raw_text, re.IGNORECASE | re.DOTALL)
                if matches:
                    sql_query = matches[-1].strip()
                else:
                    raise ValueError("Could not extract SQL query")
            
            print("\nDebug - Extracted SQL Query:", sql_query)
            
            # Extract LLM reasoning
            llm_output = {
                "reasoning": agent_result.get("intermediate_steps", []),
                "final_answer": agent_result.get("output", "")
            }
            
            # Get SQL results using the visualization tool
            viz_tool = QuerySQLDatabaseForVizTool(db=self.db)
            sql_result = viz_tool._run(self._extract_sql_query(agent_result))
            
            # Parse and format the data for visualization
            df = pd.DataFrame(sql_result["raw_data"])
            chart_type = self._determine_visualization_type(user_query, df)
            visualization_data = self._format_for_visualization(df, chart_type)
            
            return {
                "status": "success",
                "llm_analysis": {
                    "reasoning": llm_output["reasoning"],
                    "final_answer": llm_output["final_answer"]
                },
                "sql_data": {
                    "query": sql_result.get("sql_query", ""),
                    "raw_data": sql_result["raw_data"],
                    "columns": sql_result["column_names"]
                },
                "visualization": {
                    "chart_data": visualization_data,
                    "chart_type": chart_type
                }
            }

        except Exception as e:
            print(f"Error processing query: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

    def _extract_sql_query(self, agent_result: Dict) -> str:
        """Extract the SQL query from agent result"""
        # Debug the structure
        print("\nExtracting SQL from structure:")
        for key, value in agent_result.items():
            print(f"Key: {key}")
            print(f"Type: {type(value)}")
        
        # Look in intermediate steps
        steps = agent_result.get("intermediate_steps", [])
        for step in steps:
            if isinstance(step, tuple) and len(step) >= 2:
                action, action_input = step
                if hasattr(action, 'tool') and action.tool == "sql_db_query":
                    # Extract query before the results
                    query = action_input.split('[')[0].strip()
                    if query.upper().startswith("SELECT"):
                        return query

        # Look in the action input directly
        if "action_input" in str(agent_result).lower():
            matches = re.findall(r"Action Input: (SELECT.*?)(?:\[|\n|$)", 
                               str(agent_result), re.IGNORECASE | re.DOTALL)
            if matches:
                return matches[-1].strip()

        raise ValueError("No SQL query found in agent result")

    def _determine_visualization_type(self, query: str, df: pd.DataFrame) -> str:
        """Determine the most appropriate visualization type"""
        query = query.lower()
        columns = df.columns.tolist()

        # Define visualization rules
        rules = [
            # Time series rules
            (lambda q, cols: 'date' in cols and any(x in q for x in ['trend', 'over time', 'monthly', 'daily']), 'line'),
            # Comparison rules
            (lambda q, cols: 'product' in cols and any(x in q for x in ['compare', 'comparison', 'versus']), 'bar'),
            # Distribution rules
            (lambda q, cols: 'total_sales' in cols and any(x in q for x in ['distribution', 'proportion', 'share']), 'pie'),
            # Default rules
            (lambda q, cols: 'product' in cols, 'bar'),
            (lambda q, cols: 'date' in cols, 'line')
        ]

        # Apply rules in order
        for rule, chart_type in rules:
            if rule(query, columns):
                return chart_type

        return 'bar'  # Default fallback

    def _format_for_visualization(self, df: pd.DataFrame, chart_type: str) -> dict:
        """Format the DataFrame for visualization"""
        if df.empty:
            return {}

        # Ensure proper date formatting
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df = df.sort_values('date')

        # Format based on chart type
        formatters = {
            'line': lambda df: {
                "dates": df['date'].dt.strftime('%Y-%m-%d').tolist(),
                "sales": df['total_sales'].tolist(),
                "chart_type": "line"
            },
            'bar': lambda df: {
                "categories": df['product'].tolist() if 'product' in df.columns else df.index.tolist(),
                "sales": df['total_sales'].tolist(),
                "chart_type": "bar"
            },
            'pie': lambda df: {
                "categories": df['product'].tolist() if 'product' in df.columns else df.index.tolist(),
                "sales": df['total_sales'].tolist(),
                "chart_type": "pie"
            }
        }

        return formatters.get(chart_type, lambda df: {})(df)

# Example usage
if __name__ == "__main__":
    db_manager = EnhancedDatabaseManager()
    
    # Test queries
    test_queries = [
        "Show me total sales by product for 2023"
    ]
    
    for query in test_queries:
        print(f"\nTesting query: {query}")
        result = db_manager.process_query(query)
        
        # Simplified output without reasoning steps
        print("\n=== SQL Data ===")
        print("Query:", result["sql_data"]["query"])
        print("\nRaw Data:")
        print(pd.DataFrame(result["sql_data"]["raw_data"]))
        
        print("\n=== Visualization Data ===")
        print("Chart Type:", result["visualization"]["chart_type"])
        print("Chart Data:", result["visualization"]["chart_data"]) 