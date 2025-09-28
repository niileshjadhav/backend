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
            
            # Extract suggestions from response if present
            suggestions = self._extract_suggestions(response_text)
            
            return {
                "response": response_text.strip(),
                "suggestions": suggestions,
                "source": "openai"
            }
            
        except Exception as e:
            logger.error(f"OpenAI API error: {e}")
            return self._get_fallback_response(user_message, str(e))
    
    def _extract_suggestions(self, response_text: str) -> list:
        """Extract suggestions from OpenAI response"""
        # Common suggestions based on log management context
        default_suggestions = [
            "Show table statistics",
            "Explain safety rules"
        ]
        
        # Try to extract suggestions from response if formatted properly
        suggestions = []
        lines = response_text.split('\n')
        
        for line in lines:
            line = line.strip()
            if line.startswith('•') or line.startswith('-'):
                suggestion = line.lstrip('•- ').strip()
                if len(suggestion) > 5 and len(suggestion) < 50:
                    suggestions.append(suggestion)
        
        # Return extracted suggestions or defaults
        return suggestions[:4] if suggestions else default_suggestions[:4]
    
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
    
    async def validate_intent(self, user_message: str, parsed_operation) -> Optional[Any]:
        """Validate and potentially refine a medium-confidence parsed operation using LLM"""
        try:
            validation_prompt = f"""
                You are validating a database operation request. Here's what was parsed:

                **User Message:** "{user_message}"
                **Parsed Action:** {parsed_operation.action}
                **Parsed Table:** {parsed_operation.table}
                **Parsed Filters:** {parsed_operation.filters}
                **Confidence:** {parsed_operation.confidence}

                Please respond with one of:
                1. "CONFIRMED" - if the parsing is correct
                2. "REFINED: [corrected_action] [corrected_table] [corrected_filters]" - if you can improve it
                3. "CONVERSATIONAL" - if this should be handled as a conversation instead

                Be concise and specific.
            """
            
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": validation_prompt}],
                "temperature": 0.3,
                "max_tokens": 200
            }
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            result_text = data["choices"][0]["message"]["content"].strip().upper() if data["choices"] else ""
            
            if "CONFIRMED" in result_text:
                class ValidationResult:
                    def __init__(self):
                        self.confirmed = True
                        self.refined_operation = parsed_operation
                return ValidationResult()
            
            elif "REFINED:" in result_text:
                class ValidationResult:
                    def __init__(self):
                        self.confirmed = True
                        self.refined_operation = parsed_operation
                return ValidationResult()
            
            else:  # CONVERSATIONAL
                class ValidationResult:
                    def __init__(self):
                        self.confirmed = False
                        self.refined_operation = None
                return ValidationResult()
            
        except Exception as e:
            logger.error(f"LLM validation failed: {e}")
            return None
    
    async def parse_with_tools(self, user_message: str) -> Optional[Any]:
        """Parse user message using LLM with MCP tool calling capabilities"""
        try:
            # Enhanced tool prompt to determine which MCP tool to use
            tool_prompt = f"""
            You are a database operations assistant with access to MCP tools. Analyze this user message and determine the best approach:

            User Message: "{user_message}"

            Available MCP Tools:
            1. get_table_stats: For ACTIVITIES/TRANSACTIONS/ARCHIVE queries (shows counts, not records)
            2. query_logs: For OTHER table queries (when user wants to see actual data)
            3. archive_records: Archive old records to archive tables  
            4. delete_archived_records: Delete records from archive tables
            5. health_check: Check system health

            Available Tables:
            - dsiactivities: Activity logs (main table)
            - dsitransactionlog: Transaction logs (main table)
            - dsiactivities_archive: Archived activity logs
            - dsitransactionlog_archive: Archived transaction logs

            Response Format:
            Choose ONE of these responses:
            1. "MCP_TOOL: [tool_name] [table_name] [filters_json]" - if this needs an MCP tool
            2. "CONVERSATIONAL" - if this should be handled as a conversation

            Examples:
            - "show me recent errors" -> "MCP_TOOL: get_table_stats dsiactivities {{'filters': {{'ActivityType': 'ERROR'}}}}"
            - "show activities" -> "MCP_TOOL: get_table_stats dsiactivities {{}}"
            - "list transactions" -> "MCP_TOOL: get_table_stats dsitransactionlog {{}}"
            - "get database stats" -> "MCP_TOOL: get_table_stats dsiactivities {{}}"  
            - "archive old logs from last month" -> "MCP_TOOL: archive_records dsiactivities {{'filters': {{'date_range': 'last_month'}}, 'user_id': 'system'}}"
            - "hello how are you" -> "CONVERSATIONAL"
            """
            
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": tool_prompt}],
                "temperature": 0.3,
                "max_tokens": 200
            }
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            result_text = data["choices"][0]["message"]["content"].strip() if data["choices"] else ""
            
            # Parse the LLM response and call appropriate MCP tool
            if "MCP_TOOL:" in result_text:
                return await self._execute_mcp_tool(result_text, user_message)
            
            else:  # CONVERSATIONAL
                class LLMResult:
                    def __init__(self):
                        self.is_database_operation = False
                        self.operation = None
                        self.mcp_result = None
                return LLMResult()
            
        except Exception as e:
            logger.error(f"LLM tool parsing failed: {e}")
            return None

    async def _execute_mcp_tool(self, llm_response: str, original_message: str) -> Optional[Any]:
        """Execute the appropriate MCP tool based on LLM analysis"""
        try:
            # Parse the LLM response: "MCP_TOOL: [tool_name] [table_name] [filters_json]"
            parts = llm_response.replace("MCP_TOOL:", "").strip().split(" ", 2)
            tool_name = parts[0].strip()
            table_name = parts[1].strip() if len(parts) > 1 else ""
            filters_str = parts[2].strip() if len(parts) > 2 else "{}"
            
            # Parse filters JSON
            try:
                filters_data = json.loads(filters_str) if filters_str else {}
                # If the filters_data contains a 'filters' key, use that, otherwise use the data directly
                if "filters" in filters_data:
                    filters = filters_data["filters"]
                else:
                    # Use the data directly (e.g., {"date_filter": "older_than_10_months"})
                    filters = filters_data
                
                limit = filters_data.get("limit")
                user_id = filters_data.get("user_id", "system")
            except Exception as e:
                logger.warning(f"Filter parsing error: {e}, using filters_str: {filters_str}")
                filters = {}
                limit = None
                user_id = "system"
            
            # Import MCP tools
            from cloud_mcp.server import (
                query_logs, archive_records, delete_archived_records, 
                get_table_stats, health_check
            )
            
            # Execute the appropriate MCP tool
            mcp_result = None
            if tool_name == "query_logs" and table_name:
                mcp_result = await query_logs(table_name, filters, limit or None)  # Use limit if specified, otherwise no limit
            elif tool_name == "archive_records" and table_name:
                mcp_result = await archive_records(table_name, filters, user_id)
            elif tool_name == "delete_archived_records" and table_name:
                mcp_result = await delete_archived_records(table_name, filters, user_id)
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
            class MCPLLMResult:
                def __init__(self, mcp_result):
                    self.is_database_operation = True
                    self.operation = None  # Will be handled by MCP result
                    self.mcp_result = mcp_result
                    self.tool_used = tool_name
                    self.table_used = table_name
                    
            return MCPLLMResult(mcp_result) if mcp_result else None
            
        except Exception as e:
            logger.error(f"MCP tool execution failed: {e}")
            return None

    async def classify_intent(self, user_message: str) -> Optional[Any]:
        """Classify user message intent with enhanced understanding"""
        try:
            # Enhanced intent classification with more context
            intent_prompt = f"""
            You are an expert database assistant. Analyze this user message and classify it.

            User Message: "{user_message}"

            Context Information:
            - We manage activity logs and transaction logs
            - Users often want to count, query, archive, or delete records
            - Date-based queries are common (older than X months/days/years)
            - Statistics and health checks are database operations

            Classification Rules:
            DATABASE OPERATION if message involves:
            - Counting records ("count", "how many", "number of")
            - Querying data ("show", "list", "find", "get", "display")
            - Managing data ("archive", "delete", "move", "clean")
            - Statistics ("stats", "statistics", "summary")
            - System status ("health", "status", "check")
            - Date-based queries ("older than", "from", "since", "before")

            CONVERSATIONAL if message involves:
            - Greetings ("hello", "hi", "thanks")
            - General help ("what can you do", "help me")
            - Unclear requests without specific data operations
            - Non-database related questions

            Examples:
            "count of activities older than 10 months" → DATABASE (counting with date filter)
            "show me recent errors" → DATABASE (querying specific data)
            "hello there" → CONVERSATIONAL (greeting)
            "what can you help me with" → CONVERSATIONAL (general help)
            "archive old records" → DATABASE (data management)
            "how many transactions yesterday" → DATABASE (counting with date)

            Respond with exactly: DATABASE or CONVERSATIONAL
            """
            
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": intent_prompt}],
                "temperature": 0.1,
                "max_tokens": 10
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=15)
            response.raise_for_status()
            data = response.json()
            result_text = data["choices"][0]["message"]["content"].strip().upper() if data["choices"] else ""
            
            class IntentResult:
                def __init__(self, is_database_op):
                    self.is_database_operation = is_database_op
            
            result = IntentResult("DATABASE" in result_text)
            
            return result
            
        except Exception as e:
            logger.error(f"Intent classification failed: {e}")
            # Default to database operation for safety
            class IntentResult:
                def __init__(self, is_database_op):
                    self.is_database_operation = is_database_op
            return IntentResult(True)

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
            2. query_logs - Use for OTHER table queries (when user wants to see actual data from non-activity/transaction tables)
            3. archive_records - Use for archiving old records to archive tables
            4. delete_archived_records - Use for deleting records from archive tables
            5. health_check - Use for system health/status checks

            CONFIRMATION HANDLING:
            If this is a confirmation (contains "CONFIRM ARCHIVE" or "CONFIRM DELETE"):
            - Look at the conversation history to find the original preview operation
            - Extract the EXACT same table name and filters that were used
            - Add "confirmed":true to the filters
            - Use archive_records for "CONFIRM ARCHIVE" or delete_archived_records for "CONFIRM DELETE"

            SAFETY RULES - CRITICAL:
            🛡️ Archive operations without date filters → System applies default 7-day minimum age filter
            🛡️ Delete operations without date filters → System applies default 30-day minimum age filter
            🛡️ These defaults prevent accidental processing of ALL records
            
            Key Analysis Rules:
            ✅ COUNT/HOW MANY/STATISTICS queries → ALWAYS use get_table_stats
            ✅ ACTIVITIES/TRANSACTIONS/ARCHIVE QUERIES → ALWAYS use get_table_stats (show counts, not records)
            ✅ OTHER TABLE QUERIES → Use query_logs for showing actual records
            ✅ GENERAL DATABASE STATS (e.g., "show table statistics", "database statistics") → use get_table_stats with NO table name (leave empty)
            ✅ Table Selection: Use main tables (dsiactivities, dsitransactionlog) unless specifically asked for archived data
            ✅ Context-aware parsing: If user says "show me more" or "archive those", refer to previous conversation
            ✅ Date filters: Parse natural language dates
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
                query_logs, archive_records, delete_archived_records, 
                get_table_stats, health_check
            )
            
            mcp_result = None
            
            if tool_name == "query_logs" and table_name:
                mcp_result = await query_logs(table_name, filters, None)  # No limit to show actual count
            elif tool_name == "archive_records" and table_name:
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