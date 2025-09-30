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

            CONTEXT HANDLING - CRITICAL:
            - ALWAYS examine conversation history to understand incomplete requests
            - If user says "for then X days", "what about Y months", "for activities", etc. - use context to determine table and operation
            - If user says "archive them", "delete them", "archive those records" - use context to determine BOTH table AND filters from previous query
            - PRESERVE BOTH TABLE AND FILTERS: Extract exact table name and date filters from previous query
            - Look for previous queries to understand what table/operation user is referring to
            - Context patterns: "for then", "what about", "how about", "and for", "also for", "archive them", "delete them", "those records"
            - Example: Previous "activities older than 30 days" → "archive them" = MUST use both "dsiactivities" table AND "older_than_30_days" filter
            
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

            ERROR HANDLING & CLARIFICATION - CRITICAL:
            - If user request is a simple greeting (hello, hi, help) → Return None (let conversational system handle)
            - If user asks POLICY/EXPLANATION questions (what does archive mean, how does archiving work, what is archive policy) → Return None (let conversational system handle)
            - If user asks GENERAL INFORMATIONAL questions (what is today's date, what time is it, current date, today's date) → Return None (let conversational system handle)
            - If user asks OFF-TOPIC questions (weather, sports, cooking, news, entertainment, etc.) → Return None (let conversational system handle)
            - If user asks DESTRUCTIVE/DANGEROUS questions (delete database, drop table, truncate, delete all, etc.) → Return None (let conversational system handle)
            - If table name cannot be determined from user request AND it's database-related → MUST respond with: "CLARIFY_TABLE_NEEDED"
            - If filters or date criteria are ambiguous AND it's database-related → MUST respond with: "CLARIFY_FILTERS_NEEDED"  
            - If user request is unclear or incomplete AND it's database-related → MUST respond with: "CLARIFY_REQUEST_NEEDED"
            - If user says vague things like "show data", "archive stuff", "delete things" → MUST ask for clarification
            - DO NOT guess table names - if unclear, ask for clarification
            - DO NOT proceed with MCP_TOOL format unless table names and operations are 100% clear
            - Simple conversational messages should return None, not clarification requests
            - Off-topic requests (not related to database/logs/activities/transactions/archiving) should return None, not clarification requests
            - Destructive requests (delete database, drop table, etc.) should return None, not clarification requests

            Key Analysis Rules:
            - DATA QUERIES: COUNT/HOW MANY/STATISTICS about records → ALWAYS use get_table_stats
            - DATA OPERATIONS: "show activities", "list transactions", "display archive records" → ALWAYS use get_table_stats (show counts, not records)
            - GENERAL DATABASE STATS (e.g., "show table statistics", "database statistics") → use get_table_stats with NO table name (leave empty)
            - POLICY/EXPLANATION QUESTIONS: "what is archive policy", "how does archiving work", "what does archive mean" → Return None (conversational)
            - Table Selection: Use main tables (dsiactivities, dsitransactionlog) unless specifically asked for archived data
            - Context-aware parsing: If user says "show me more", "archive those", "archive them", "delete them", "for then X days", "what about Y months", refer to previous conversation
            - Date filters: Parse natural language dates
               - "older than 10 months" → {{"date_filter": "older_than_10_months"}}
               - "older than 12 months" → {{"date_filter": "older_than_12_months"}}
               - "from last year" → {{"date_filter": "from_last_year"}}
               - "recent" or "latest" → {{"date_filter": "recent"}}
               - "for then X days" → {{"date_filter": "older_than_X_days"}} (use context to determine table)

            Examples with detailed analysis:

            CLEAR REQUESTS (proceed with MCP_TOOL):
            "count of activities older than 12 months"
            → Analysis: COUNT query + date filter + specific table clearly identified
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_months"}}

            "show activities from last month" 
            → Analysis: SHOW query + activities table clearly identified
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "from_last_month"}}

            "list transactions"
            → Analysis: LIST query + transactions table clearly identified
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            "archive activities older than 12 months"
            → Analysis: ARCHIVE operation + date filter + activities table clearly identified
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_12_months"}}

            "show table statistics" or "database statistics"
            → Analysis: GENERAL STATISTICS query + no specific table needed
            → MCP_TOOL: get_table_stats  {{}}

            CONTEXTUAL FOLLOW-UPS (use conversation history):
            Previous: "activities older than 15 days" → User: "for then 12 days"
            → Analysis: Context shows previous query was about activities + new time period
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_days"}}

            Previous: "count transactions" → User: "what about 30 days old"
            → Analysis: Context shows previous query was about transactions + new filter
            → MCP_TOOL: get_table_stats dsitransactionlog {{"date_filter": "older_than_30_days"}}

            Previous: "show archive records" → User: "for activities only"
            → Analysis: Context shows archive request + now specify activities archive
            → MCP_TOOL: get_table_stats dsiactivities_archive {{}}

            CONTEXTUAL ARCHIVE OPERATIONS (use conversation history):
            Previous: "activities older than 30 days" (got count result) → User: "archive them"
            → Analysis: Context shows previous query about activities older than 30 days + now archive those EXACT records
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_30_days"}}

            Previous: "transactions older than 45 days" → User: "archive those records"
            → Analysis: Context shows previous query about transactions + now archive with SAME filter
            → MCP_TOOL: archive_records dsitransactionlog {{"date_filter": "older_than_45_days"}}

            Previous: "count activities older than 12 months" → User: "yes archive them"
            → Analysis: Previous query had activities + "older than 12 months" filter + now archive those
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_12_months"}}

            Previous: "show transactions from last year" → User: "archive those"
            → Analysis: Previous query had transactions + "from last year" filter + now archive those
            → MCP_TOOL: archive_records dsitransactionlog {{"date_filter": "from_last_year"}}

            Previous: "count activities older than 6 months" → User: "yes archive them all"
            → Analysis: Context shows previous query about activities older than 6 months + confirmation to archive
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_6_months"}}

            CONVERSATIONAL REQUESTS (should be handled elsewhere, not by MCP tools):
            "hello", "hi", "hey"
            → Analysis: Simple greeting - this should be handled by conversational system, not MCP
            → Return None (let conversational handler take over)

            "help", "what can you do"
            → Analysis: General help request - this should be handled by conversational system
            → Return None (let conversational handler take over)

            "what is today's date", "what time is it", "current date"
            → Analysis: General informational question - this should be handled by conversational system
            → Return None (let conversational handler take over)

            "what is weather in India", "today's weather in Pune", "weather forecast"
            → Analysis: Weather/off-topic question - this should be handled by conversational system
            → Return None (let conversational handler take over)

            "what does archive mean", "how does archiving work", "what is the archive policy"
            → Analysis: Policy/explanation question - this should be handled by conversational system
            → Return None (let conversational handler take over)

            "For how much old data do you archive", "What is the archive policy", "How does archiving work"
            → Analysis: Policy/explanation questions - not requesting data, asking about processes
            → Return None (let conversational handler take over)

            "Archive means what", "What does archive mean", "Can you explain archiving"
            → Analysis: Definition/explanation requests - not data operations
            → Return None (let conversational handler take over)

            "sports news", "cooking recipes", "latest movies", "stock prices"
            → Analysis: Off-topic requests - not related to database/log management
            → Return None (let conversational handler take over)

            "delete database", "drop table", "truncate table", "delete all records", "remove database"
            → Analysis: Destructive/dangerous requests - should be handled by conversational system with security response
            → Return None (let conversational handler take over)

            "drop table activities", "delete entire database", "truncate dsiactivities", "remove all data"
            → Analysis: Destructive operations outside mandate - conversational system will decline with security explanation
            → Return None (let conversational handler take over)

            UNCLEAR REQUESTS (ask for clarification):
            "show data"
            → Analysis: Unclear which table - activities or transactions?
            → CLARIFY_TABLE_NEEDED

            "archive old stuff"
            → Analysis: Unclear which table and what "old" means
            → CLARIFY_TABLE_NEEDED

            "delete records from last week"
            → Analysis: Unclear which table to delete from
            → CLARIFY_TABLE_NEEDED

            "show me some information"
            → Analysis: Too vague - what information from which table?
            → CLARIFY_REQUEST_NEEDED

            "count things older than something"
            → Analysis: Too vague - which table and what date criteria?
            → CLARIFY_FILTERS_NEEDED

            "archive stuff from a while ago"
            → Analysis: Unclear table and vague time reference
            → CLARIFY_TABLE_NEEDED

            CRITICAL RESPONSE FORMAT:
            You MUST respond with ONLY this exact format (no additional text, analysis, or explanations):
            MCP_TOOL: [tool_name] [table_name] [filters_json]

            For clarification requests, respond with ONLY one of these (no additional text):
            CLARIFY_TABLE_NEEDED
            CLARIFY_FILTERS_NEEDED  
            CLARIFY_REQUEST_NEEDED

            Example valid responses:
            MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_months"}}
            MCP_TOOL: archive_records dsiactivities {{}}
            CLARIFY_TABLE_NEEDED

            DO NOT provide analysis, explanations, or step-by-step reasoning. Respond ONLY with the format above.
            """
            
            url = f"{self.base_url}/chat/completions"
            payload = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": enhanced_prompt}],
                "temperature": 0.1,  
                "max_tokens": 100,   
                "stop": ["\n\n", "Analysis:", "Step"]  # Stop tokens to prevent verbose responses
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            data = response.json()
            result_text = data["choices"][0]["message"]["content"].strip() if data["choices"] else ""
            
            # Parse the enhanced LLM response
            if "MCP_TOOL:" in result_text:
                return await self._parse_enhanced_mcp_response(result_text, user_message)
            elif any(clarify in result_text for clarify in ["CLARIFY_TABLE_NEEDED", "CLARIFY_FILTERS_NEEDED", "CLARIFY_REQUEST_NEEDED"]):
                # Handle clarification requests
                return await self._handle_clarification_request(result_text, user_message)
            else:
                # Check if LLM intentionally returned None for conversational handling
                if result_text.strip().lower() in ['none', 'null', '']:
                    logger.info(f"LLM determined message is conversational, not database operation: '{user_message}'")
                else:
                    logger.warning(f"Enhanced LLM did not return expected format for message '{user_message}'. LLM response: '{result_text}'")
                
                # Try to extract operation intent and provide fallback response for common cases
                if self._is_archive_request(user_message):
                    # Create fallback archive operation
                    logger.info(f"Providing fallback archive operation for: '{user_message}'")
                    return await self._create_fallback_archive_operation(user_message)
                elif self._is_stats_request(user_message):
                    # Create fallback stats operation
                    logger.info(f"Providing fallback stats operation for: '{user_message}'")
                    return await self._create_fallback_stats_operation(user_message)
                
                return None
                
        except Exception as e:
            logger.error(f"Enhanced LLM parsing failed for message '{user_message}': {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    async def _parse_enhanced_mcp_response(self, llm_response: str, original_message: str) -> Optional[Any]:
        """Parse enhanced LLM response and return structured result"""
        try:
            # Clean up the response and find the MCP_TOOL line
            cleaned_response = llm_response.strip()
            mcp_line = None
            
            # Handle case where the entire response is the MCP_TOOL line
            if cleaned_response.startswith("MCP_TOOL:"):
                mcp_line = cleaned_response
            else:
                # Find the line with MCP_TOOL:
                for line in cleaned_response.split('\n'):
                    if "MCP_TOOL:" in line:
                        mcp_line = line.strip()
                        break
            
            if not mcp_line:
                logger.error(f"No MCP_TOOL line found in LLM response. Full response: '{llm_response}'. Original message: '{original_message}'")
                return None
            
            # Parse the MCP_TOOL line: "MCP_TOOL: [tool_name] [table_name] [filters_json]"
            parts = mcp_line.replace("MCP_TOOL:", "").strip().split(" ", 2)
            tool_name = parts[0].strip() if len(parts) > 0 else ""
            table_name = parts[1].strip() if len(parts) > 1 else ""
            filters_str = parts[2].strip() if len(parts) > 2 else "{}"
            
            # Validate tool name is not empty and is valid
            valid_tools = ["get_table_stats", "archive_records", "delete_archived_records", "health_check"]
            if not tool_name:
                logger.error(f"Empty tool name in MCP_TOOL line: '{mcp_line}'. Original message: '{original_message}'")
                return None
            elif tool_name not in valid_tools:
                logger.warning(f"Invalid tool name '{tool_name}' provided by LLM. Valid tools: {valid_tools}")
                # Create error result for invalid tool name
                class InvalidToolResult:
                    def __init__(self, tool_name):
                        self.is_clarification_request = True
                        self.clarification_message = (
                            f"I encountered an invalid operation '{tool_name}'. "
                            "I can help you with the following operations:\n\n"
                            "💡 **Available Operations:**\n"
                            "• **View Data:** \"Show table statistics\" or \"Count activities\"\n"
                            "• **Archive Data:** \"Archive old records\" or \"Archive activities older than 7 days\"\n"
                            "• **Delete Data:** \"Delete archived records\" (with proper date filters)\n"
                            "• **System Info:** \"Health check\" or \"Database status\"\n\n"
                            "**Example:** \"Show statistics for dsiactivities table\""
                        )
                        self.is_database_operation = False
                        self.tool_used = None
                        self.table_used = None
                        # Don't set mcp_result for clarification requests
                        self.mcp_result = None
                return InvalidToolResult(tool_name)
            
            # Validate table name if provided
            valid_tables = ["dsiactivities", "dsitransactionlog", "dsiactivities_archive", "dsitransactionlog_archive", ""]
            if table_name and table_name not in valid_tables:
                logger.warning(f"Invalid table name '{table_name}' provided by LLM. Valid tables: {valid_tables}")
                # Create error result for invalid table name
                class InvalidTableResult:
                    def __init__(self, table_name):
                        self.is_clarification_request = True
                        self.clarification_message = (
                            f"Please specify one of the following valid tables:\n\n"
                            "**Available Tables:**\n"
                            "• `dsiactivities` - Current activity logs\n"
                            "• `dsitransactionlog` - Current transaction logs\n"
                            "• `dsiactivities_archive` - Archived activity logs\n"
                            "• `dsitransactionlog_archive` - Archived transaction logs\n\n"
                            "**Example:** \"Show statistics for dsiactivities table\""
                        )
                        self.is_database_operation = False
                        self.tool_used = None
                        self.table_used = None
                        # Don't set mcp_result for clarification requests
                        self.mcp_result = None
                return InvalidTableResult(table_name)
            
            # Parse filters JSON
            try:
                filters_data = json.loads(filters_str) if filters_str else {}
                filters = filters_data if isinstance(filters_data, dict) else {}
            except json.JSONDecodeError as e:
                logger.warning(f"Failed to parse filters JSON '{filters_str}': {e}")
                # Create error result for invalid filters
                class InvalidFiltersResult:
                    def __init__(self, filters_str, error):
                        self.is_clarification_request = True
                        self.clarification_message = (
                            f"I had trouble understanding the filter criteria. "
                            "Please provide clearer date or filter information:\n\n"
                            "📅 **Date Filter Examples:**\n"
                            "• \"records older than 10 months\"\n"
                            "• \"data from last year\"\n"
                            "• \"recent activities\"\n\n"
                            "**Example:** \"Show activities older than 7 days\""
                        )
                        self.is_database_operation = False
                        self.tool_used = None
                        self.table_used = None
                        # Don't set mcp_result for clarification requests
                        self.mcp_result = None
                return InvalidFiltersResult(filters_str, e)
                # Use empty filters as fallback
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

    def _is_archive_request(self, message: str) -> bool:
        """Check if message is requesting an archive operation (not policy questions)"""
        message_lower = message.lower().strip()
        
        # Exclude policy/explanation questions
        explanation_patterns = [
            'what does archive mean', 'archive means what', 'what is archive', 
            'explain archive', 'how does archive work', 'what is the archive policy',
            'for how much old data do you archive', 'archive policy'
        ]
        
        if any(pattern in message_lower for pattern in explanation_patterns):
            return False
        
        # Only match actual archive operation requests
        archive_operation_keywords = [
            'archive data', 'archive records', 'archive activities', 'archive transactions',
            'move old', 'move to archive', 'start archive', 'run archive'
        ]
        return any(keyword in message_lower for keyword in archive_operation_keywords)

    def _is_stats_request(self, message: str) -> bool:
        """Check if message is requesting statistics/counts"""
        message_lower = message.lower().strip()
        stats_keywords = ['show', 'count', 'list', 'display', 'statistics', 'stats', 'how many']
        return any(keyword in message_lower for keyword in stats_keywords)

    async def _create_fallback_archive_operation(self, user_message: str) -> Any:
        """Create fallback archive operation when LLM format parsing fails"""
        try:
            from cloud_mcp.server import archive_records
            
            # Determine table - default to activities if not specified
            table_name = "dsiactivities"
            if "transaction" in user_message.lower():
                table_name = "dsitransactionlog"
            
            # Use empty filters - system will apply default safety filters
            filters = {}
            
            # Execute archive operation
            mcp_result = await archive_records(table_name, filters, "system")
            
            # Create result object
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None
            
            return EnhancedLLMResult("archive_records", table_name, filters, mcp_result)
            
        except Exception as e:
            logger.error(f"Fallback archive operation failed: {e}")
            return None

    async def _create_fallback_stats_operation(self, user_message: str) -> Any:
        """Create fallback stats operation when LLM format parsing fails"""
        try:
            from cloud_mcp.server import get_table_stats
            
            # Determine table - default to activities if not specified
            table_name = "dsiactivities"
            if "transaction" in user_message.lower():
                table_name = "dsitransactionlog"
            elif "archive" in user_message.lower():
                if "transaction" in user_message.lower():
                    table_name = "dsitransactionlog_archive"
                else:
                    table_name = "dsiactivities_archive"
            
            # Use empty filters for general stats
            filters = {}
            
            # Execute stats operation
            mcp_result = await get_table_stats(table_name, filters, "system")
            
            # Create result object
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None
            
            return EnhancedLLMResult("get_table_stats", table_name, filters, mcp_result)
            
        except Exception as e:
            logger.error(f"Fallback stats operation failed: {e}")
            return None
    async def _handle_clarification_request(self, llm_response: str, original_message: str) -> Any:
        """Handle cases where LLM needs clarification about table names or filters"""
        try:
            clarification_message = ""
            
            if "CLARIFY_TABLE_NEEDED" in llm_response:
                clarification_message = (
                    "I need clarification about which table you'd like to work with. "
                    "Please specify one of the following:\n\n"
                    "**Available Tables:**\n"
                    "• `dsiactivities` - Current activity logs\n"
                    "• `dsitransactionlog` - Current transaction logs\n"
                    "• `dsiactivities_archive` - Archived activity logs\n"
                    "• `dsitransactionlog_archive` - Archived transaction logs\n\n"
                    "**Example:** \"Show statistics for dsiactivities table\" or \"Archive records from dsitransactionlog\""
                )
            elif "CLARIFY_FILTERS_NEEDED" in llm_response:
                clarification_message = (
                    "I need more specific information about the date or filter criteria. "
                    "Please provide more details:\n\n"
                    "📅 **Date Filter Examples:**\n"
                    "• \"records older than 10 months\"\n"
                    "• \"data from last year\"\n"
                    "• \"recent activities\"\n"
                    "**Example:** \"Show activities older than 12 months\""
                )
            elif "CLARIFY_REQUEST_NEEDED" in llm_response:
                clarification_message = (
                    "I'm not sure what you'd like me to do. Could you please clarify your request?\n\n"
                    "💡 **I can help you with:**\n"
                    "• **View Data:** \"Show table statistics\" or \"Count activities\"\n"
                    "• **Archive Data:** \"Archive old records\" or \"Archive activities older than 7 days\"\n"
                    "• **Delete Data:** \"Delete archived records\" (with proper date filters)\n"
                    "• **System Info:** \"Health check\" or \"Database status\"\n\n"
                    "**Example:** \"Archive transactions older than 7 days\""
                )
            
            # Create a clarification result object
            class ClarificationResult:
                def __init__(self, message):
                    self.is_clarification_request = True
                    self.clarification_message = message
                    self.is_database_operation = False
                    self.tool_used = None
                    self.table_used = None 
                    # Don't set mcp_result for clarification requests to avoid confusion
                    self.mcp_result = None
            
            return ClarificationResult(clarification_message)
            
        except Exception as e:
            logger.error(f"Error handling clarification request: {e}")
            return None

    def get_system_prompt(self) -> str:
       """Get the system prompt for log management context"""
       from datetime import datetime
       current_date = datetime.now().strftime("%Y-%m-%d")
       current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
       
       return f"""You are an AI assistant for Cloud Inventory Log Management System.

            CURRENT DATE: {current_date}
            CURRENT DATE & TIME: {current_datetime}

            - CAPABILITIES:
            • Query database tables (dsiactivities, dsitransactionlog, and their _archive versions)
            • Guide archiving and data management operations
            • Explain safety rules and validate user requests

            - CRITICAL SAFETY RULES:
            • Archive: Records must be >7 days old
            • Delete: Only archived records >30 days old
            • Operations require date filters and confirmation
            • All operations are logged and role-restricted

            - DATE FORMAT: YYYYMMDDHHMMSS (e.g., 20240315123000)

            - HANDLING DIFFERENT TYPES OF USER INPUTS:

            **1. GREETINGS & CASUAL CONVERSATION:**
            • For "hello", "hi", "how are you", etc. - Respond warmly and introduce your capabilities
            • Example: "Hello! I'm your Cloud Inventory Log Management assistant. I can help you view database statistics, guide archiving operations, and explain safety procedures."

            **2. GENERAL QUESTIONS ABOUT THE SYSTEM:**
            • For questions about policies, procedures, or how things work - Provide informative explanations
            • Example: "What is archiving?" → Explain the archiving process, safety rules, and benefits
            • Example: "What can you do?" → List your capabilities with examples

            **3. OUT-OF-CONTEXT REQUESTS:**
            • For requests completely unrelated to log management (weather, cooking, etc.) - Politely redirect to your domain
            • Example: "I'm specialized in Cloud Inventory Log Management. I can help you with database operations, archiving procedures, and system statistics. What would you like to know about your log data?"

            **4. DESTRUCTIVE/DANGEROUS REQUESTS:**
            • For destructive operations outside your mandate (delete database, drop table, truncate, etc.) - Firmly decline with security explanation
            • Example: "Delete Database" → "I cannot and will not perform destructive database operations like deleting entire databases or dropping tables. I'm designed with safety-first principles and only support controlled archiving operations with built-in safeguards. I can help you with safe data management within established policies."
            • Example: "Drop table" → "I don't have permissions to drop tables or perform destructive schema operations. My role is limited to safe data viewing and controlled archiving with multiple safety checks. Would you like to see table statistics or learn about our archiving procedures instead?"
            • Emphasize safety controls and redirect to approved operations

            **5. VAGUE DATABASE REQUESTS:**
            • For unclear data requests like "show data", "check something" - Ask specific clarifying questions
            • Provide examples of what you can do rather than generic error messages
            • Example: "I'd be happy to help! Could you specify what data you'd like to see? For example: 'Show activities statistics' or 'Count transactions older than 30 days'"

            **6. TECHNICAL QUESTIONS:**
            • For questions about database structure, table relationships, etc. - Provide detailed technical explanations
            • Include table names, purposes, and relationships

            **7. SPECIFIC DATABASE OPERATIONS:**
            • For clear requests about tables, archiving, statistics - Process normally and provide structured responses
            • Available tables: dsiactivities, dsitransactionlog, dsiactivities_archive, dsitransactionlog_archive

            **8. ERROR HANDLING GUIDELINES:**
            • Don't always default to the same clarification message
            • Tailor your response to the type of confusion or vagueness
            • If table names are unclear → Ask specifically about which table
            • If date criteria are vague → Ask specifically about time ranges
            • If the entire request is unclear → Ask about the goal they're trying to achieve

            - RESPONSE TONE & STYLE:
            • Be helpful, professional, and domain-appropriate
            • Use clear formatting with actionable suggestions
            • Adapt your response style to the user's question type
            • For casual questions → Be conversational and welcoming
            • For technical questions → Be precise and detailed
            • For vague questions → Be guiding and educational
            • Always prioritize data safety in operational guidance
            • When asked about the current date, always refer to: {current_date}

            - EXAMPLES OF APPROPRIATE RESPONSES:

            User: "Hello" → "Hello! I'm your Cloud Inventory Log Management assistant. I can help you view database statistics, manage archiving operations, and explain safety procedures. What would you like to know about your log data?"

            User: "What's the weather?" → "I'm specialized in Cloud Inventory Log Management and can't help with weather information. However, I can help you with database operations, viewing statistics, archiving procedures, and system health checks. What would you like to know about your log data?"

            User: "Delete Database" → "I cannot and will not perform destructive database operations like deleting entire databases. I'm designed with safety-first principles and only support controlled archiving operations with built-in safeguards. My role is limited to safe data viewing and controlled archiving with multiple safety checks. Would you like to see table statistics or learn about our archiving procedures instead?"

            User: "Drop table activities" → "I don't have permissions to drop tables or perform destructive schema operations. Table dropping is a dangerous operation that could cause data loss and is outside my mandate. I can help you with safe operations like viewing table statistics, archiving old records, or explaining our data retention policies. What would you like to know about the activities table?"

            User: "Show me something" → "I'd be happy to show you information! Could you be more specific about what you'd like to see? For example:\n• 'Show activities statistics'\n• 'Count transactions from last month'\n• 'Display archive table information'\n• 'Show database health status'\n\nWhat type of data are you interested in?"

            User: "Archive policy?" → "Our archiving policy includes several safety measures:\n• Records must be older than 7 days before archiving\n• Only archived records older than 30 days can be deleted\n• All operations require confirmation and are logged\n• Archive operations move data to dedicated archive tables (dsiactivities_archive, dsitransactionlog_archive)\n\nWould you like to see statistics for any specific table or learn about performing an archive operation?"

            Remember: Match your response style and detail level to the user's question type and apparent technical knowledge level."""

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
        """Provide contextual fallback response when OpenAI fails"""
        error_msg = f" (Technical issue: {error})" if error else ""
        
        # Analyze the user message to provide more contextual responses
        user_msg_lower = user_message.lower().strip()
        
        # Greeting patterns
        if any(greeting in user_msg_lower for greeting in ['hello', 'hi', 'hey', 'good morning', 'good afternoon']):
            return {
                "response": f"Hello! I'm your Cloud Inventory Log Management assistant{error_msg}. "
                           "I'm here to help you manage your database operations safely and efficiently.\n\n"
                           "💡 **What I can help with:**\n"
                           "• View database statistics and record counts\n"
                           "• Guide archiving and data management operations\n"
                           "• Explain safety policies and procedures\n"
                           "• Monitor system health and performance\n\n"
                           "What would you like to know about your log data?",
                "suggestions": [
                    "Show table statistics",
                    "What can you do?", 
                    "Explain archiving policy",
                    "Show recent activities"
                ],
                "source": "fallback"
            }
        
        # Help or capability questions
        elif any(help_word in user_msg_lower for help_word in ['help', 'what can you do', 'capabilities', 'features']):
            return {
                "response": f"I'm having trouble with my full response system right now{error_msg}, but I can still help! "
                           "I'm specialized in Cloud Inventory Log Management with these capabilities:\n\n"
                           "🔍 **Data Operations:**\n"
                           "• View table statistics and record counts\n"
                           "• Query specific data ranges and filters\n\n"
                           "📦 **Archive Management:**\n"
                           "• Guide safe archiving procedures (7+ day old records)\n"
                           "• Manage archive table operations\n\n"
                           "🛡️ **Safety & Compliance:**\n"
                           "• Enforce data retention policies\n"
                           "• Provide operation confirmations and logging\n\n"
                           "Try asking me about specific tables or operations!",
                "suggestions": [
                    "Show activities statistics",
                    "Show transactions statistics", 
                    "Explain safety rules",
                    "What is archiving?"
                ],
                "source": "fallback"
            }
        
        # Database-related but vague requests
        elif any(db_word in user_msg_lower for db_word in ['show', 'data', 'table', 'database', 'stats', 'count', 'archive']):
            return {
                "response": f"I'm experiencing some technical difficulties{error_msg}, but I can still assist with your database request! "
                           "Could you be more specific about what you'd like to see?\n\n"
                           "📊 **Available Tables:**\n"
                           "• `dsiactivities` - Current activity logs\n"
                           "• `dsitransactionlog` - Current transaction logs\n"
                           "• `dsiactivities_archive` - Archived activity logs\n"
                           "• `dsitransactionlog_archive` - Archived transaction logs\n\n"
                           "**Example specific requests:**\n"
                           "• \"Show statistics for dsiactivities table\"\n"
                           "• \"Count activities older than 30 days\"\n"
                           "• \"Archive transactions from last year\"",
                "suggestions": [
                    "Show activities statistics",
                    "Show transactions statistics",
                    "Show archive statistics", 
                    "Database health check"
                ],
                "source": "fallback"
            }
        
        # Completely off-topic requests
        elif not any(topic_word in user_msg_lower for topic_word in ['log', 'data', 'table', 'database', 'activity', 'transaction', 'archive']):
            return {
                "response": f"I'm having some technical issues right now{error_msg}. "
                           "I'm specialized in Cloud Inventory Log Management and can't help with topics outside that domain. "
                           "However, I'd be happy to help you with:\n\n"
                           "🎯 **My Specialties:**\n"
                           "• Database operations and statistics\n"
                           "• Log data archiving and management\n"
                           "• System safety and compliance procedures\n"
                           "• Data retention policy guidance\n\n"
                           "What would you like to know about your log management system?",
                "suggestions": [
                    "What can you do?",
                    "Show table statistics",
                    "Explain archiving policy",
                    "System health check"
                ],
                "source": "fallback"
            }
        
        # Default fallback for unclear requests
        else:
            return {
                "response": f"I'm experiencing some technical difficulties processing your request{error_msg}. "
                           "I'm your Cloud Inventory Log Management assistant and I'm here to help with database operations.\n\n"
                           "💡 **How I can help:**\n"
                           "• View table statistics and record counts\n"
                           "• Guide you through archiving procedures\n"  
                           "• Explain safety rules and best practices\n"
                           "• Monitor system health and performance\n\n"
                           "Could you try rephrasing your request or choose from the suggestions below?",
                "suggestions": [
                    "Show table statistics",
                    "What can you do?", 
                    "Explain safety rules",
                    "Show recent data"
                ],
                "source": "fallback"
            }


