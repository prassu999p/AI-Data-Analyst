from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from dotenv import load_dotenv
import os
import json
from db_manager import DatabaseConnectionManager

# Load environment variables
load_dotenv()

class LangChainAgent:
    def __init__(self):
        """Initialize the LangChain agent with database connection manager"""
        # Initialize the database connection manager
        self.db_manager = DatabaseConnectionManager()
        
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

    def _create_agent_for_connection(self, connection_id: str):
        """Create a new agent for the specified database connection"""
        # Get database connection
        db = self.db_manager.get_langchain_db(connection_id)
        if not db:
            raise ValueError(f"No database connection found for ID: {connection_id}")

        # Initialize toolkit and agent for this connection
        toolkit = SQLDatabaseToolkit(db=db, llm=self.llm)
        agent = create_sql_agent(
            llm=self.llm,
            toolkit=toolkit,
            verbose=True,
            agent_kwargs={
                "handle_parsing_errors": True
            }
        )
        return agent

    def process_query(self, query: str, connection_id: str) -> dict:
        """Process a natural language query for a specific database connection"""
        try:
            print("\n=== Processing Query ===")
            print(f"Input Query: {query}")
            print(f"Connection ID: {connection_id}")

            # Create agent for this connection
            agent = self._create_agent_for_connection(connection_id)

            # Execute the query through the agent
            agent_result = agent.invoke({
                "input": query,
                "chat_history": self.memory.chat_memory.messages if self.memory else []
            })

            print("\n=== Agent Result ===")
            print(json.dumps(agent_result, indent=2, default=str))

            # Extract the SQL query and results
            sql_query = None
            sql_results = None
            intermediate_steps = agent_result.get("intermediate_steps", [])
            
            for step in intermediate_steps:
                if isinstance(step, tuple) and len(step) >= 2:
                    action, response = step
                    if hasattr(action, 'tool') and action.tool == "sql_db_query":
                        sql_query = action.tool_input
                        sql_results = response

            print("\n=== Extracted SQL Information ===")
            print(f"SQL Query: {sql_query}")
            print(f"SQL Results: {sql_results}")

            # Format the response
            response = {
                "status": "success",
                "llm_analysis": {
                    "reasoning": intermediate_steps,
                    "final_answer": agent_result.get("output", "")
                },
                "sql_data": {
                    "query": sql_query,
                    "results": sql_results
                }
            }

            print("\n=== Final Formatted Response ===")
            print(json.dumps(response, indent=2, default=str))

            return response

        except Exception as e:
            print(f"\n=== Error Processing Query ===")
            print(f"Error: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

def main():
    """
    Main function to demonstrate agent functionality with a single query
    """
    print("\n=== DataViz AI Agent Demo ===")
    
    # Initialize the agent
    agent = LangChainAgent()
    
    # Test query
    test_query = "Display the monthly sales trend for Product A in 2023"
    
    print(f"\nProcessing Query: '{test_query}'")
    print("\n" + "="*50)
    
    # Process the query
    result = agent.process_query(test_query, "default")
    print(result) 
    # Print formatted output
    if result['status'] == 'success':
        print("\nQuery Execution Successful!")
        print("\n1. Generated SQL Query:")
        print("-" * 20)
        print(result['sql_data']['query'])
        
        print("\n2. SQL Results:")
        print("-" * 20)
        print(result['sql_data']['results'])
        
        print("\n3. AI Analysis:")
        print("-" * 20)
        print(result['llm_analysis']['final_answer'])
    else:
        print("\nQuery Execution Failed!")
        print(f"Error: {result.get('message', 'Unknown error')}")

if __name__ == "__main__":
    main() 

