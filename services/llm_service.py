"""LLM service for intelligent chat responses"""
import os
import json
import logging
import requests
from typing import Optional, Dict, Any
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class OpenAIService:
    """Service for LLM integration using OpenAI"""

    def __init__(self):
        # Use OpenAI exclusively
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.model_name = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.provider = "openai"
        self.base_url = "https://api.openai.com/v1"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        if not self.api_key:
            logger.error("OPENAI_API_KEY not found in environment variables")
            raise ValueError("OpenAI API key is required")

        try:
            pass  # Service initialized successfully
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI service: {e}")
            raise
    
    def get_system_prompt(self) -> str:
       """Get the system prompt for log management context"""
       return """You are an AI assistant for Cloud Inventory Log Management System.

            🔧 CAPABILITIES:
            • Query database tables (dsiactivities, dsitransactionlog, and their _archive versions)
            • Guide archiving and data management operations
            • Explain safety rules and validate user requests

            🛡️ CRITICAL SAFETY RULES:
            • Archive: Records must be >7 days old
            • Delete: Only archived records >30 days old
            • Operations require date filters and confirmation
            • All operations are logged and role-restricted

            📅 DATE FORMAT: YYYYMMDDHHMMSS (e.g., 20240315123000)

            🎯 RESPONSE APPROACH:
            • Prioritize data safety in all guidance
            • Use clear formatting with actionable suggestions
            • Guide users through proper procedures - you don't execute operations directly
            • Be concise but comprehensive in explanations"""

    async def generate_response(
        self, 
        user_message: str, 
        user_id: str,
        conversation_context: Optional[str] = None
    ) -> Dict[str, Any]:
        """Generate response using the configured LLM with conversation memory"""
        try:
            # Build the prompt with system context and conversation history
            system_prompt = self.get_system_prompt()
            
            # Prepare messages for OpenAI chat format
            messages = [{"role": "system", "content": system_prompt}]
            
            # Add conversation context if available (includes previous exchanges)
            if conversation_context and conversation_context != "No previous conversation history.":
                # Parse conversation context into individual messages
                if "Previous conversation:" in conversation_context:
                    context_lines = conversation_context.split("\n")
                    
                    for line in context_lines:
                        line = line.strip()
                        if line.startswith("User: "):
                            messages.append({"role": "user", "content": line[6:]})  # Remove "User: "
                        elif line.startswith("Assistant: "):
                            messages.append({"role": "assistant", "content": line[11:]})  # Remove "Assistant: "
            
            # Add current user message
            messages.append({"role": "user", "content": user_message})
            
            # Use OpenAI API with requests
            url = f"{self.base_url}/chat/completions"
            
            payload = {
                "model": self.model_name,
                "messages": messages,
                "temperature": 0.7,
                "max_tokens": 1000,
                "top_p": 0.8
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)
            response.raise_for_status()
            
            response_data = response.json()
            response_text = response_data["choices"][0]["message"]["content"]
            
            if not response_text:
                logger.warning("Empty response from OpenAI")
                return self._get_fallback_response(user_message)
            
            return {
                "response": response_text.strip(),
                "source": "openai"
            }
            
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return self._get_fallback_response(user_message, str(e))
        
    def _get_fallback_response(self, user_message: str, error: Optional[str] = None) -> Dict[str, Any]:
        """Provide fallback response when OpenAI fails"""
        error_msg = f" (Error: {error})" if error else ""
        
        return {
            "response": f"I'm having trouble processing your request right now{error_msg}. "
                       "I'm your Cloud Inventory Log Management assistant and I can help you with:\n\n"
                       "• View table statistics and record counts\n"
                       "• Guide you through archiving procedures\n"  
                       "• Explain safety rules and best practices\n"
                       "What would you like to know about?",
            "suggestions": [
                "Show table statistics",
                "Explain safety rules"
            ],
            "source": "fallback"
        }
    
    def test_connection(self) -> bool:
        """Test if the OpenAI API is working"""
        try:
            # Test OpenAI connection using requests
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": "Hello, respond with 'Hello' if you can hear me."}],
                "max_tokens": 50
            }
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            return bool(data and data["choices"] and "hello" in data["choices"][0]["message"]["content"].lower())
        except Exception as e:
            logger.error(f"OpenAI connection test failed: {e}")
            return False

    async def parse_with_enhanced_tools(self, user_message: str, conversation_context: Optional[str] = None) -> Optional[Any]:
        """Enhanced LLM parsing that always returns MCP tool calls for database operations"""
        try:
            # Process through LLM for context-aware results
            
            # Enhanced prompt with better dynamic understanding and conversation context
            context_section = ""
            if conversation_context and "Previous conversation:" in conversation_context:
                context_section = f"""

            Recent Conversation Context:
            {conversation_context}

            This helps understand references like "show me more", "archive those records", "delete them", etc.
            """
            
            enhanced_prompt = f"""
            You are an expert database operations assistant. Analyze the user's request and convert it to the appropriate MCP tool call.

            User Request: "{user_message}"
            {context_section}

            Available Database Tables:
            - dsiactivities: Main activity logs (current records)
            - dsitransactionlog: Main transaction logs (current records)  
            - dsiactivities_archive: Archived activity logs (old records)
            - dsitransactionlog_archive: Archived transaction logs (old records)

            Available MCP Tools:
            1. get_table_stats - Use for ACTIVITIES/TRANSACTIONS/ARCHIVE queries (shows counts, not records)
            2. archive_records - Use for archiving old records to archive tables
            3. delete_archived_records - Use for deleting records from archive tables
            4. health_check - Use for system health/status checks

            CONFIRMATION HANDLING:
            If this is a confirmation (contains "CONFIRM ARCHIVE" or "CONFIRM DELETE"):
            - Look at the conversation history to find the original preview operation
            - Extract the EXACT same table name and filters that were used
            - Add "confirmed":true to the filters
            - Use archive_records for "CONFIRM ARCHIVE" or delete_archived_records for "CONFIRM DELETE"

            SAFETY RULES - CRITICAL:
            - Archive operations without date filters → System applies default 7-day minimum age filter
            - Delete operations without date filters → System applies default 30-day minimum age filter
            - These defaults prevent accidental processing of ALL records

            Key Analysis Rules:
            - COUNT/HOW MANY/STATISTICS queries → ALWAYS use get_table_stats
            - ACTIVITIES/TRANSACTIONS/ARCHIVE QUERIES → ALWAYS use get_table_stats (show counts, not records)
            - GENERAL DATABASE STATS (e.g., "show table statistics", "database statistics") → use get_table_stats with NO table name (leave empty)
            - Table Selection: Use main tables (dsiactivities, dsitransactionlog) unless specifically asked for archived data
            - Context-aware parsing: If user says "show me more" or "archive those", refer to previous conversation
            - Date filters: Parse natural language dates
               - "older than 10 months" → {{"date_filter": "older_than_10_months"}}
               - "older than 12 months" → {{"date_filter": "older_than_12_months"}}
               - "from last year" → {{"date_filter": "from_last_year"}}
               - "recent" or "latest" → {{"date_filter": "recent"}}

            Examples with detailed analysis:
            "count of activities older than 12 months"
            → Analysis: COUNT query + date filter + specific table
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_months"}}

            "show activities from last month"
            → Analysis: SHOW query + activities table → Use get_table_stats (show counts, not records)
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "from_last_month"}}

            "list transactions"
            → Analysis: LIST query + transactions table → Use get_table_stats (show counts, not records)
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            "display archive records"
            → Analysis: DISPLAY query + archive table → Use get_table_stats (show counts, not records)
            → MCP_TOOL: get_table_stats dsiactivities_archive {{}}

            "show table statistics" or "database statistics"
            → Analysis: GENERAL STATISTICS query + no specific table
            → MCP_TOOL: get_table_stats  {{}}

            "archive activities older than 12 months"
            → Analysis: ARCHIVE operation + date filter + main table
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_12_months"}}

            "archive activities" (no date specified)
            → Analysis: ARCHIVE operation + no date filter specified + main table
            → MCP_TOOL: archive_records dsiactivities {{}}
            → Note: System will automatically apply default 7-day safety filter

            "delete archived activities" (no date specified)
            → Analysis: DELETE operation + no date filter specified + archive table
            → MCP_TOOL: delete_archived_records dsiactivities {{}}
            → Note: System will automatically apply default 30-day safety filter

            "CONFIRM ARCHIVE" (after showing 3 records older than 12 months)
            → Analysis: CONFIRMATION of archive + use same filters from context + add confirmed flag
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_12_months", "confirmed": true}}

            CRITICAL: Respond with exactly: MCP_TOOL: [tool_name] [table_name] [filters_json]

            Analyze the request step by step:
            1. What operation? (count/show/archive/delete/health)
            2. What table? (activities/transactions)
            3. What filters? (dates/agents/types)
            4. Is this a confirmation? Use exact filters from conversation + confirmed:true
            """
            
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": enhanced_prompt}],
                "temperature": 0.2,
                "max_tokens": 150
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            result_text = data["choices"][0]["message"]["content"].strip() if data["choices"] else ""
            
            # Parse the enhanced LLM response
            if "MCP_TOOL:" in result_text:
                return await self._parse_enhanced_mcp_response(result_text, user_message)
            else:
                logger.warning(f"Enhanced LLM did not return MCP_TOOL format for message '{user_message}'. LLM response: '{result_text}'")
                return None
                
        except Exception as e:
            logger.error(f"Enhanced LLM parsing failed for message '{user_message}': {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    async def _parse_enhanced_mcp_response(self, llm_response: str, original_message: str) -> Optional[Any]:
        """Parse enhanced LLM response and return structured result"""
        try:
            # Find the line with MCP_TOOL:
            mcp_line = None
            for line in llm_response.split('\n'):
                if "MCP_TOOL:" in line:
                    mcp_line = line.strip()
                    break
            
            if not mcp_line:
                logger.error(f"No MCP_TOOL line found in LLM response. Full response: '{llm_response}'. Original message: '{original_message}'")
                return None
            
            # Parse the MCP_TOOL line: "MCP_TOOL: [tool_name] [table_name] [filters_json]"
            parts = mcp_line.replace("MCP_TOOL:", "").strip().split(" ", 2)
            tool_name = parts[0].strip()
            table_name = parts[1].strip() if len(parts) > 1 else ""
            filters_str = parts[2].strip() if len(parts) > 2 else "{}"
            
            # Parse filters JSON
            try:
                filters_data = json.loads(filters_str) if filters_str else {}
                filters = filters_data if isinstance(filters_data, dict) else {}
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse filters JSON '{filters_str}': {e}")
                filters = {}
            
            # Execute the MCP tool
            from cloud_mcp.server import (
                archive_records, delete_archived_records, 
                get_table_stats, health_check
            )
            
            mcp_result = None
            
            if tool_name == "archive_records" and table_name:
                mcp_result = await archive_records(table_name, filters, "system")
            elif tool_name == "delete_archived_records" and table_name:
                mcp_result = await delete_archived_records(table_name, filters, "system")
            elif tool_name == "get_table_stats":
                # Handle both specific table stats and general database stats
                if table_name:
                    mcp_result = await get_table_stats(table_name, filters)
                else:
                    # General database stats - use database service directly
                    from services.region_service import get_region_service
                    from services.database_service import DatabaseService
                    
                    try:
                        region_service = get_region_service()
                        current_region = region_service.get_current_region() or region_service.get_default_region()
                        region_db_session = region_service.get_session(current_region)
                        
                        try:
                            db_service = DatabaseService(region_db_session)
                            mcp_result = await db_service.get_detailed_table_stats()
                        finally:
                            region_db_session.close()
                    except Exception as e:
                        logger.error(f"Error getting general database stats: {e}")
                        mcp_result = {
                            "success": False,
                            "error": f"Failed to get general database statistics: {str(e)}"
                        }
            elif tool_name == "health_check":
                mcp_result = await health_check()
            else:
                logger.warning(f"Unknown MCP tool or missing table: tool={tool_name}, table={table_name}")
            
            # Create result object with MCP data
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None  # Will be handled by MCP result
            
            result_obj = EnhancedLLMResult(tool_name, table_name, filters, mcp_result)
            return result_obj
            
        except Exception as e:
            logger.error(f"Enhanced MCP response parsing failed: {e}")
            return None