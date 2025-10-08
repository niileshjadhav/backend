"""LLM service for intelligent chat responses"""
import os
import json
import logging
import requests
from typing import Optional, Dict, Any, List
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
    
    def _extract_context_info(self, conversation_context: Optional[str] = None) -> Dict[str, Any]:
        """Extract table name, filters, and job log context from conversation context"""
        context_info = {
            "last_table": None,
            "last_filters": {},
            "last_operation": None,
            "last_job_operation": None,
            "last_job_filters": {},
            "has_job_context": False
        }
        
        if not conversation_context:
            return context_info
            
        try:
            context_lower = conversation_context.lower()
            
            table_mentions = []
            for table in ["dsitransactionlogarchive", "dsiactivitiesarchive", "dsitransactionlog", "dsiactivities", "job_logs"]:
                if table in context_lower:
                    # Find the position of the last mention
                    last_pos = context_lower.rfind(table)
                    table_mentions.append((last_pos, table))
            
            # Sort by position (most recent first)
            if table_mentions:
                table_mentions.sort(reverse=True)
                context_info["last_table"] = table_mentions[0][1]
            
            # Extract common filter patterns from context
            if "older than" in context_lower:
                import re
                # Look for "older than X days/months/years"
                pattern = r"older than (\d+) (day|month|year)s?"
                match = re.search(pattern, context_lower)
                if match:
                    number, unit = match.groups()
                    context_info["last_filters"] = {"date_filter": f"older_than_{number}_{unit}s"}
            
            if "archive" in context_lower:
                context_info["last_operation"] = "archive"
            elif "count" in context_lower or "statistics" in context_lower:
                context_info["last_operation"] = "stats"
            elif "delete" in context_lower:
                context_info["last_operation"] = "delete"
            
            # Extract job log context
            if "[job context:" in context_lower or "job logs" in context_lower or "job query" in context_lower:
                context_info["has_job_context"] = True
                context_info["last_job_operation"] = "job_logs"
                
                # Extract job-specific filters from context
                import re
                
                # Extract job types
                job_type_match = re.search(r"job_type: ([^,\]]+)", context_lower)
                if job_type_match:
                    context_info["last_job_filters"]["job_type"] = job_type_match.group(1).strip()
                
                # Extract status filters (more specific patterns)
                status_match = re.search(r"status: ([^,\]]+)", context_lower)
                if status_match:
                    status_value = status_match.group(1).strip()
                    context_info["last_job_filters"]["status"] = status_value
                    if status_value.upper() == "FAILED":
                        context_info["last_job_filters"]["failed_only"] = True
                    elif status_value.upper() == "SUCCESS":
                        context_info["last_job_filters"]["successful_only"] = True
                
                # Extract table filters
                tables_match = re.search(r"tables: ([^,\]]+)", context_lower)
                if tables_match:
                    context_info["last_job_filters"]["table_name"] = tables_match.group(1).strip()
                
                # Extract date range filters
                date_match = re.search(r"date_range: ([^,\]]+)", context_lower)
                if date_match:
                    context_info["last_job_filters"]["date_range"] = date_match.group(1).strip()
                
                # Handle direct job_types/job_type patterns
                job_types_match = re.search(r"job_types: ([^,\]]+)", context_lower)
                if job_types_match:
                    context_info["last_job_filters"]["job_type"] = job_types_match.group(1).strip()
                
                # Don't assume failed status unless explicitly mentioned
                # Remove the automatic failed/successful detection that was causing issues
                            
        except Exception as e:
            logger.warning(f"Error extracting context info: {e}")
            
        return context_info

    def _determine_table_from_context(self, user_message: str, context_info: Dict[str, Any]) -> str:
        """Determine table name using message content and context"""
        user_msg_lower = user_message.lower()
        
        if "dsitransactionlogarchive" in user_msg_lower:
            return "dsitransactionlogarchive"
        elif "dsiactivitiesarchive" in user_msg_lower:
            return "dsiactivitiesarchive"
        elif "dsitransactionlog" in user_msg_lower and "archive" not in user_msg_lower:
            return "dsitransactionlog"
        elif "dsiactivities" in user_msg_lower and "archive" not in user_msg_lower:
            return "dsiactivities"
        
        # Second priority: Let LLM determine context-dependent queries
        # Simple check: if no explicit table mentioned and we have context, let LLM decide
        has_explicit_table = ("transaction" in user_msg_lower or "activit" in user_msg_lower or 
                             "dsitransactionlog" in user_msg_lower or "dsiactivities" in user_msg_lower)
        
        # If no explicit table mentioned and we have previous context, preserve it
        # The LLM prompt will handle the intelligent decision-making
        if not has_explicit_table and context_info.get("last_table"):
            return context_info["last_table"]
        
        # Third priority: Explicit table type mentions (fresh requests with table specified)
        # These are NEW queries that explicitly mention table type, use main tables
        
        if "transaction" in user_msg_lower:
            # "transactions older than X" or "show transactions" or "yesterday's transactions" → use main table
            if "archive" in user_msg_lower:
                return "dsitransactionlogarchive"
            return "dsitransactionlog"
        elif "activit" in user_msg_lower:
            # "activities older than X" or "show activities" → use main table  
            if "archive" in user_msg_lower:
                return "dsiactivitiesarchive"
            return "dsiactivities"
        
        # Handle specific yesterday patterns
        if "yesterday" in user_msg_lower:
            if "transaction" in user_msg_lower:
                return "dsitransactionlog"
            elif "activit" in user_msg_lower:
                return "dsiactivities"
        

        
        # Default fallback
        return "dsiactivities"

    def _determine_filters_from_context(self, user_message: str, context_info: Dict[str, Any]) -> Dict[str, Any]:
        """Determine filters using message content and context - Now uses LLM date parsing"""
        user_msg_lower = user_message.lower()
        filters = {}
        
        # Check if message contains date-related terms that should be parsed by LLM
        date_keywords = [
            'january', 'february', 'march', 'april', 'may', 'june',
            'july', 'august', 'september', 'october', 'november', 'december',
            'last', 'past', 'recent', 'older', 'newer', 'yesterday', 'today',
            'week', 'month', 'year', 'day', 'quarter', 'season', 'holiday',
            'ago', 'before', 'after', 'since', 'from', 'to', 'between',
            'this', 'current', 'previous', 'fiscal', 'maintenance', 'busy'
        ]
        
        # If message contains date keywords, pass the raw message as date_filter
        # The LLM date filter will handle the intelligent parsing
        has_date_terms = any(keyword in user_msg_lower for keyword in date_keywords)
        
        if has_date_terms:
            # For messages with date terms, let the LLM parse the entire message
            # This is more reliable than trying to extract with regex
            
            # Check for specific month names to handle the "january" case
            month_names = ['january', 'february', 'march', 'april', 'may', 'june',
                          'july', 'august', 'september', 'october', 'november', 'december']
            
            found_month = None
            for month in month_names:
                if month in user_msg_lower:
                    found_month = month
                    break
            
            if found_month:
                # For month-specific queries, extract the month and year if present
                import re
                year_match = re.search(r'\b(20\d{2})\b', user_message)
                if year_match:
                    filters["date_filter"] = f"{found_month} {year_match.group(1)}"
                else:
                    filters["date_filter"] = found_month
            else:
                # For other date expressions, pass the whole message to LLM
                # Remove table-specific words to focus on date part
                clean_message = user_message
                for word in ['transactions', 'activities', 'records', 'data', 'logs']:
                    clean_message = clean_message.replace(word, '').strip()
                
                # Clean up extra spaces and common words
                clean_message = ' '.join(clean_message.split())
                filters["date_filter"] = clean_message if clean_message else user_message
        
        # If no date terms found but context suggests continuation, check context for filters
        elif not filters and context_info.get('has_job_context'):
            # This might be a follow-up query in job logs context
            pass
        
        return filters

    async def parse_with_enhanced_tools(self, user_message: str, conversation_context: Optional[str] = None) -> Optional[Any]:
        """Enhanced LLM parsing with separate table and filter context tracking"""
        try:
            # Extract context information
            context_info = self._extract_context_info(conversation_context)
            
            # Process through LLM for context-aware results
            context_section = ""
            if conversation_context and "Previous conversation:" in conversation_context:
                context_section = f"""

            Recent Conversation Context:
            {conversation_context}

            Previous Table: {context_info.get('last_table', 'None')}
            Previous Filters: {context_info.get('last_filters', {})}
            Previous Operation: {context_info.get('last_operation', 'None')}
            Has Job Context: {context_info.get('has_job_context', False)}

            This helps understand references like "show me more", "archive those records", "delete them", etc.
            """
            
            enhanced_prompt = f"""
            You are an expert database operations assistant. Analyze user requests using natural language understanding and conversational context.

            User Request: "{user_message}"
            {context_section}

            Available Database Tables:
            - dsiactivities: Main activity logs (current records)
            - dsitransactionlog: Main transaction logs (current records)  
            - dsiactivitiesarchive: Archived activity logs (old records)
            - dsitransactionlogarchive: Archived transaction logs (old records)

            Available MCP Tools:
            1. get_table_stats - For counting records and table statistics with date filters
            2. archive_records - For archiving old records (7+ days old) from main tables to archive tables
            3. delete_archived_records - For deleting old archived records (30+ days old) from archive tables
            4. health_check - For system health/status checks
            5. region_status - For region connection status and current region info
            6. execute_sql_query - For custom SQL queries using natural language (includes ALL job queries and complex conditions)

            CRITICAL ANALYSIS RULES:

            1. STATISTICS/COUNTING QUERIES (USE get_table_stats):
            - "count activities", "activities count", "total activities" → get_table_stats
            - "count transactions", "transactions count", "total transactions" → get_table_stats  
            - "activities older than X days/months" → get_table_stats with date_filter
            - "transactions older than X days/months" → get_table_stats with date_filter
            - "count of archived activities", "archived activities count" → get_table_stats
            - "count of archived transactions", "archived transactions count" → get_table_stats
            - "table statistics", "database statistics", "show table stats" → get_table_stats
            - Simple counting queries with basic date filters → get_table_stats

            2. JOB QUERIES (USE execute_sql_query):
            - "show jobs", "list jobs", "recent jobs", "latest jobs" → execute_sql_query
            - "failed jobs", "successful jobs", "job status" → execute_sql_query
            - "job statistics", "job summary", "job stats" → execute_sql_query
            - "archive jobs", "delete jobs" (about job execution logs, not data) → execute_sql_query
            - "jobs from last week", "jobs today", "recent job executions" → execute_sql_query
            - ALL job-related queries should use execute_sql_query for maximum flexibility
            - Job queries target the job_logs table via SQL generation
            - ANALYSIS/REASONING queries about jobs → execute_sql_query (e.g., "analyse job fail", "why jobs fail", "job failure reasons")

            3. ARCHIVE OPERATIONS (USE archive_records):
            - "archive activities", "archive old activities", "archive activities older than X" → archive_records
            - "archive transactions", "archive old transactions", "archive transactions older than X" → archive_records
            - Must be clear operational intent for moving data to archive tables

            4. DELETE OPERATIONS (USE delete_archived_records):
            - "delete archived activities", "delete old archived activities" → delete_archived_records
            - "delete archived transactions", "delete old archived transactions" → delete_archived_records
            - Must be clear operational intent for permanently removing archived data

            5. REGION QUERIES (USE region_status):
            - EXACT phrases: "region status", "current region", "which region", "region info" → region_status
            - Must be specifically about region/connection status

            6. CUSTOM SQL QUERIES (USE execute_sql_query - COMPLEX CONDITIONS ONLY):
            - Complex WHERE conditions with multiple criteria → execute_sql_query
            - Queries with specific field matching (e.g., ActivityType = 'Event') → execute_sql_query
            - JOIN operations or multi-table queries → execute_sql_query
            - ANALYSIS queries (why, analyse, reason, cause) → execute_sql_query
            - "show activities where ActivityType is Event", "find records with...", "activities by server" → execute_sql_query
            - "list transactions where...", "get data with specific criteria", "filter by multiple conditions" → execute_sql_query
            - "analyse job fail", "why jobs fail", "job failure reasons" → execute_sql_query
            - Queries not matching simple counting/stats patterns above → execute_sql_query

            CONTEXT HANDLING (CRITICAL):
            - PRESERVE context from previous queries for follow-up requests
            - "archive them", "delete them", "count them" → Use EXACT table + filters from previous query
            - Archive context preservation: After "archived X" query, follow-ups stay on archive table

            TABLE SELECTION LOGIC:
            - NEW explicit requests: "count transactions" → dsitransactionlog (main table)
            - CONTEXTUAL references: Use previous table from conversation
            - Archive preservation: "archived X" context + follow-up → keep archive table

            DATE FILTER PARSING (LLM-Enhanced):
            - Use natural language date expressions directly in date_filter
            - "transactions in month of january" → {{"date_filter": "january"}}
            - "older than 10 months" → {{"date_filter": "older than 10 months"}}
            - "from last year" → {{"date_filter": "last year"}}
            - "yesterday's activities" → {{"date_filter": "yesterday"}}
            - "recent data" → {{"date_filter": "recent"}}
            - "Q1 2024 transactions" → {{"date_filter": "Q1 2024"}}
            - "holiday season data" → {{"date_filter": "holiday season"}}
            - LLM will intelligently parse all date expressions

            ERROR HANDLING:
            - Greetings, policy questions, off-topic → Return None
            - Destructive operations (drop table, delete database) → Return None
            - Vague requests without context → CLARIFY_[TYPE]_NEEDED

            RESPONSE FORMAT EXAMPLES:
            "count activities" → MCP_TOOL: get_table_stats dsiactivities {{}}
            "activities older than 10 days" → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older than 10 days"}}
            "count transactions older than 3 months" → MCP_TOOL: get_table_stats dsitransactionlog {{"date_filter": "older than 3 months"}}
            "count of archived activities" → MCP_TOOL: get_table_stats dsiactivitiesarchive {{}}
            "table statistics" → MCP_TOOL: get_table_stats {{}}
            "archive activities older than 7 days" → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older than 7 days"}}
            "delete archived transactions older than 30 days" → MCP_TOOL: delete_archived_records dsitransactionlog {{"date_filter": "older than 30 days"}}
            "show jobs" → MCP_TOOL: execute_sql_query {{"user_prompt": "show jobs"}}
            "recent jobs" → MCP_TOOL: execute_sql_query {{"user_prompt": "recent jobs"}}
            "failed jobs" → MCP_TOOL: execute_sql_query {{"user_prompt": "failed jobs"}}
            "job statistics" → MCP_TOOL: execute_sql_query {{"user_prompt": "job statistics"}}
            "analyse the reason for recent job fail" → MCP_TOOL: execute_sql_query {{"user_prompt": "analyse the reason for recent job fail"}}
            "why did jobs fail" → MCP_TOOL: execute_sql_query {{"user_prompt": "why did jobs fail"}}
            "activities by server" → MCP_TOOL: execute_sql_query {{"user_prompt": "activities by server"}}
            "show activities where ActivityType is Event" → MCP_TOOL: execute_sql_query {{"user_prompt": "show activities where ActivityType is Event"}}
            "region status" → MCP_TOOL: region_status {{}}
            "hello" → None
            "show data" (no context) → CLARIFY_TABLE_NEEDED

            CRITICAL: Respond with ONLY one of these formats:
            MCP_TOOL: [tool_name] [table_name] [filters_json]
            CLARIFY_TABLE_NEEDED
            CLARIFY_FILTERS_NEEDED  
            CLARIFY_REQUEST_NEEDED
            None

            NO analysis, explanations, or additional text.
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
                if self._is_job_logs_request(user_message):
                    # Create fallback job logs operation 
                    return await self._create_fallback_job_logs_operation(user_message, conversation_context)
                elif self._is_archive_request(user_message):
                    # Create fallback archive operation with context
                    return await self._create_fallback_archive_operation(user_message, conversation_context)
                elif self._is_stats_request(user_message):
                    # Create fallback stats operation with context
                    return await self._create_fallback_stats_operation(user_message, conversation_context)
                
                return None
                
        except Exception as e:
            logger.error(f"Enhanced LLM parsing failed for message '{user_message}': {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            
            if self._is_job_logs_request(user_message):
                return await self._create_fallback_job_logs_operation(user_message, conversation_context)
            elif self._is_archive_request(user_message):
                return await self._create_fallback_archive_operation(user_message, conversation_context)
            elif self._is_stats_request(user_message):
                return await self._create_fallback_stats_operation(user_message, conversation_context)
            
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
            # Handle tools that don't need table names specially to avoid JSON parsing issues
            tools_without_tables = ["health_check", "region_status", "execute_sql_query"]
            
            cleaned_line = mcp_line.replace("MCP_TOOL:", "").strip()
            parts = cleaned_line.split(" ", 1)
            tool_name = parts[0].strip() if len(parts) > 0 else ""
            
            if tool_name in tools_without_tables:
                # For tools without tables, everything after tool name is filters
                table_name = ""
                filters_str = parts[1].strip() if len(parts) > 1 else "{}"
            else:
                # For tools with tables, split normally
                all_parts = cleaned_line.split(" ", 2)
                table_name = all_parts[1].strip() if len(all_parts) > 1 else ""
                filters_str = all_parts[2].strip() if len(all_parts) > 2 else "{}"
                
                # Special case: if table_name looks like a JSON object, it's actually filters for a general query
                if table_name.startswith('{') and table_name.endswith('}'):
                    filters_str = table_name
                    table_name = ""
            
            # Validate tool name is not empty and is valid
            valid_tools = ["get_table_stats", "archive_records", "delete_archived_records", "health_check", "region_status", "execute_sql_query"]
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
                            "• **Count/Statistics:** \"Count activities\", \"Activities older than 10 days\", \"Table statistics\"\n"
                            "• **Archive Data:** \"Archive activities older than 7 days\", \"Archive old transactions\"\n"
                            "• **Delete Data:** \"Delete archived activities older than 30 days\" (permanent removal)\n"
                            "• **System Info:** \"Health check\", \"Region status\", \"Database status\"\n"
                            "• **Custom Queries:** Complex WHERE conditions, analysis queries\n\n"
                        )
                        self.is_database_operation = False
                        self.tool_used = None
                        self.table_used = None
                        # Don't set mcp_result for clarification requests
                        self.mcp_result = None
                return InvalidToolResult(tool_name)
            
            # Validate table name if provided (some tools don't need table names)
            valid_tables = ["dsiactivities", "dsitransactionlog", "dsiactivitiesarchive", "dsitransactionlogarchive", ""]
            requires_table = tool_name not in tools_without_tables
            
            # Special case: get_table_stats can work with empty table name for general database stats
            if tool_name == "get_table_stats" and not table_name:
                # This is valid - general database stats
                pass
            elif table_name and table_name not in valid_tables:
                # For get_table_stats with invalid table name, try to use general database stats instead
                if tool_name == "get_table_stats":
                    table_name = ""  # Use empty table name for general stats
                else:
                    # Create error result for invalid table name
                    class InvalidTableResult:
                        def __init__(self, table_name):
                            self.is_clarification_request = True
                            self.clarification_message = (
                                f"Please specify one of the following valid tables:\n\n"
                                "Available Tables:\n\n"
                                "• dsiactivities - Current activity logs\n"
                                "• dsitransactionlog - Current transaction logs\n"
                                "• dsiactivitiesarchive - Archived activity logs\n"
                                "• dsitransactionlogarchive - Archived transaction logs\n\n"
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
                get_table_stats, health_check, region_status
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
            elif tool_name == "region_status":
                mcp_result = await region_status()
            elif tool_name == "query_job_logs":
                from cloud_mcp.server import _query_job_logs
                mcp_result = await _query_job_logs(filters)
            elif tool_name == "get_job_summary_stats":
                from cloud_mcp.server import _get_job_summary_stats
                mcp_result = await _get_job_summary_stats(filters)
            elif tool_name == "execute_sql_query":
                from cloud_mcp.server import _execute_sql_query
                user_prompt = filters.get("user_prompt", "") if filters else ""
                mcp_result = await _execute_sql_query(user_prompt, filters)
                
                # Extract table name from SQL query if available
                if mcp_result and mcp_result.get('generated_sql') and not table_name:
                    table_name = self._extract_primary_table_from_sql(mcp_result['generated_sql'])
            else:  
                logger.warning(f"Unknown MCP tool or missing table: tool={tool_name}, table={table_name}")
            
            # Create result object with MCP data and context preservation
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result, context_preserved=False):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None  # Will be handled by MCP result
                    self.context_preserved = context_preserved
                    # Store for future context reference
                    self.context_info = {
                        "table": table,
                        "filters": filters,
                        "operation": tool
                    }
            
            result_obj = EnhancedLLMResult(tool_name, table_name, filters, mcp_result, context_preserved=False)
            return result_obj
            
        except Exception as e:
            logger.error(f"Enhanced MCP response parsing failed: {e}")
            return None

    def _is_job_logs_request(self, message: str) -> bool:
        """Check if message is requesting job logs/execution information - INCLUDES COUNTING QUERIES"""
        message_lower = message.lower().strip()
        
        # EXACT job execution patterns
        exact_job_patterns = [
            'show jobs', 'list jobs', 'job logs', 'recent jobs',
            'last job', 'latest job', 'job statistics', 'job summary',
            'failed jobs', 'successful jobs', 'job status'
        ]
        
        # Job counting patterns - NEW!
        job_counting_patterns = [
            'how many job logs', 'count job logs', 'job logs count', 'total job logs',
            'how many jobs', 'count jobs', 'jobs count', 'total jobs',
            'number of job logs', 'number of jobs'
        ]
        
        # Job analysis patterns - NEW!
        job_analysis_patterns = [
            'job failure analysis', 'job fail analysis', 'analyse job fail', 'analyze job fail',
            'job failure reasons', 'why jobs fail', 'job error analysis', 'job issue analysis'
        ]
        
        # Check if message exactly matches or starts with exact patterns
        for pattern in exact_job_patterns:
            if message_lower == pattern or message_lower.startswith(pattern + ' '):
                return True
        
        # Check for job counting patterns - NEW!
        for pattern in job_counting_patterns:
            if pattern in message_lower:
                return True
        
        # Check for job analysis patterns - NEW!
        for pattern in job_analysis_patterns:
            if pattern in message_lower:
                return True
        
        # Check for variations with "there are" or similar
        if 'job log' in message_lower and any(word in message_lower for word in ['how many', 'count', 'total', 'number of']):
            return True
        
        if 'jobs' in message_lower and any(word in message_lower for word in ['how many', 'count', 'total', 'number of']):
            return True
        
        # Check for analysis queries containing job/jobs
        if any(word in message_lower for word in ['job', 'jobs']) and any(analysis_word in message_lower for analysis_word in ['analys', 'reason', 'why', 'cause', 'fail']):
            return True
        
        return False

    def _is_archive_request(self, message: str) -> bool:
        """Check if message is requesting an archive operation - EXACT OPERATIONAL PHRASES ONLY"""
        message_lower = message.lower().strip()
        
        # Check for policy/explanation questions first (these should not be archive operations)
        explanation_indicators = [
            'what does', 'what is', 'explain', 'how does', 'policy', 'means what',
            'for how much', 'can you explain'
        ]
        
        # If it's clearly asking for explanation/policy, it's not an operation request
        if any(indicator in message_lower for indicator in explanation_indicators) and 'archive' in message_lower:
            return False
        
        # EXACT operational archive phrases only - very restrictive
        exact_archive_patterns = [
            'archive records', 'archive old data', 'start archive',
            'archive activities', 'archive transactions', 'archive old records'
        ]
        
        # Check if message exactly matches these operational patterns
        for pattern in exact_archive_patterns:
            if pattern in message_lower:
                return True
        
        return False

    def _is_stats_request(self, message: str) -> bool:
        """Check if message is requesting simple statistics/counts - IMPROVED PATTERN MATCHING"""
        message_lower = message.lower().strip()
        
        # EXCLUDE job-related queries first - these should use SQL tool
        job_exclusion_patterns = [
            'job logs', 'job log', 'jobs', 'job execution', 'job status',
            'job failure', 'job success', 'job summary', 'job statistics'
        ]
        
        # If it's a job-related query, don't treat as stats request
        if any(pattern in message_lower for pattern in job_exclusion_patterns):
            return False
        
        # Main table counting patterns
        main_table_patterns = [
            'count activities', 'activities count', 'total activities',
            'count transactions', 'transactions count', 'total transactions',
            'table statistics', 'database statistics', 'show table stats',
            'database overview', 'table summary'
        ]
        
        # Archived table counting patterns - NEW!
        archived_patterns = [
            'count of archived activities', 'archived activities count', 'total archived activities',
            'count of archived transactions', 'archived transactions count', 'total archived transactions',
            'count archived activities', 'count archived transactions',
            'archived activities statistics', 'archived transactions statistics',
            'show archived activities count', 'show archived transactions count'
        ]
        
        # Flexible counting patterns with "of" - NEW!
        flexible_patterns = [
            'count of activities', 'count of transactions',
            'total count of activities', 'total count of transactions'
        ]
        
        # Date-filtered counting patterns - NEW! (This is the key missing pattern)
        date_filtered_patterns = [
            'activities older than', 'transactions older than',
            'activities from', 'transactions from',
            'recent activities', 'recent transactions',
            'activities in', 'transactions in',
            'activities between', 'transactions between',
            'activities last', 'transactions last',
            'activities this', 'transactions this'
        ]
        
        # Check for main table patterns
        for pattern in main_table_patterns:
            if pattern in message_lower:
                # Make sure it doesn't have complex WHERE conditions
                if not any(condition in message_lower for condition in ['where', 'like', 'containing', 'specific', 'particular']):
                    return True
        
        # Check for date-filtered patterns - CRITICAL for "activities older than 10 days"
        for pattern in date_filtered_patterns:
            if pattern in message_lower:
                # Make sure it's not asking for complex analysis or specific field matching
                if not any(condition in message_lower for condition in ['where', 'like', 'containing', 'specific', 'particular', 'analyse', 'analyze', 'why', 'reason']):
                    return True
        
        # Check for archived table patterns - NEW!
        for pattern in archived_patterns:
            if pattern in message_lower:
                # Allow "archived" requests without additional filtering restrictions
                return True
        
        # Check for flexible patterns - NEW!
        for pattern in flexible_patterns:
            if pattern in message_lower:
                # Allow simple "count of" requests
                return True
        
        # Special patterns for database overview
        overview_patterns = [
            'overview of all database tables',
            'overview of database tables',
            'all database tables',
            'database overview'
        ]
        
        if any(pattern in message_lower for pattern in overview_patterns):
            return True
        
        return False

    async def _create_fallback_archive_operation(self, user_message: str, conversation_context: str = None) -> Any:
        """Create fallback archive operation with separate table and filter context handling"""
        try:
            from cloud_mcp.server import archive_records
            
            # Extract context information
            context_info = self._extract_context_info(conversation_context)
            
            # Determine table using improved context-awareness
            table_name = self._determine_table_from_context(user_message, context_info)
            
            # Determine filters using context-awareness
            filters = self._determine_filters_from_context(user_message, context_info)
                        
            # Execute archive operation
            mcp_result = await archive_records(table_name, filters, "system")
            
            # Create result object with context preservation indicator
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result, context_preserved=False):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None
                    self.context_preserved = context_preserved
                    # Store for future context reference
                    self.context_info = {
                        "table": table,
                        "filters": filters,
                        "operation": tool
                    }
            
            context_used = bool(context_info.get('last_table'))
            return EnhancedLLMResult("archive_records", table_name, filters, mcp_result, context_used)
            
        except Exception as e:
            logger.error(f"Fallback archive operation failed: {e}")
            return None

    async def _create_fallback_stats_operation(self, user_message: str, conversation_context: str = None) -> Any:
        """Create fallback stats operation with separate table and filter context handling"""
        try:
            from cloud_mcp.server import get_table_stats
            
            # Extract context information
            context_info = self._extract_context_info(conversation_context)
            
            # Determine table using improved context-awareness
            table_name = self._determine_table_from_context(user_message, context_info)
            
            # Determine filters using context-awareness
            filters = self._determine_filters_from_context(user_message, context_info)
            
            # Special handling for yesterday queries that might be misrouted
            if "yesterday" in user_message.lower() and not filters:
                filters = {"date_filter": "yesterday"}
                        
            # Execute stats operation
            mcp_result = await get_table_stats(table_name, filters)
            
            # Create result object with context preservation indicator
            class EnhancedLLMResult:
                def __init__(self, tool, table, filters, mcp_result, context_preserved=False):
                    self.tool_used = tool
                    self.table_used = table
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None
                    self.context_preserved = context_preserved
                    # Store for future context reference
                    self.context_info = {
                        "table": table,
                        "filters": filters,
                        "operation": tool
                    }
            
            context_used = bool(context_info.get('last_table'))
            return EnhancedLLMResult("get_table_stats", table_name, filters, mcp_result, context_used)
            
        except Exception as e:
            logger.error(f"Fallback stats operation failed: {e}")
            return None
    
    async def _create_fallback_job_logs_operation(self, user_message: str, conversation_context: str = None) -> Any:
        """Create fallback job logs operation for job execution queries"""
        try:
            from cloud_mcp.server import _query_job_logs
            
            # Default filters for most job execution queries - limit all job lists to 5
            filters = {"limit": 5, "format": "table"}
            
            # Check for specific patterns that might need different handling
            user_msg_lower = user_message.lower()
            
            # If asking for multiple jobs, keep limit at 5
            if any(plural in user_msg_lower for plural in ['jobs', 'recent jobs', 'show jobs']):
                filters["limit"] = 5
                filters["format"] = "table"  
            
            # If asking for ALL jobs, still limit to 5 for consistency
            if any(all_pattern in user_msg_lower for all_pattern in ['all jobs', 'get all jobs']):
                filters["limit"] = 5
                filters["format"] = "table"  
            
            # Detect job type filters (including past tense patterns)
            if 'archive job' in user_msg_lower or 'job archived' in user_msg_lower or 'archived job' in user_msg_lower:
                filters["job_type"] = "ARCHIVE"
                filters["limit"] = 5
            elif 'delete job' in user_msg_lower or 'job deleted' in user_msg_lower or 'deleted job' in user_msg_lower:
                filters["job_type"] = "DELETE"
                filters["limit"] = 5
            
            # Detect status filters
            if 'failed' in user_msg_lower:
                filters["status"] = "FAILED"
                filters["limit"] = 5
                filters["format"] = "table"  
            elif 'successful' in user_msg_lower or 'success' in user_msg_lower:
                filters["status"] = "SUCCESS"
                filters["limit"] = 5
                filters["format"] = "table"  
            
            # Detect date filters
            if 'today' in user_msg_lower:
                filters["date_range"] = "today"
                filters["limit"] = 5
                filters["format"] = "table"  
            elif 'yesterday' in user_msg_lower:
                filters["date_range"] = "yesterday"
                filters["limit"] = 5
                filters["format"] = "table"  
            elif 'last week' in user_msg_lower:
                filters["date_range"] = "last_7_days"
                filters["limit"] = 5
                filters["format"] = "table"  
            elif 'last month' in user_msg_lower:
                filters["date_range"] = "last_month"
                filters["limit"] = 5
                filters["format"] = "table"  
            elif 'executed last week' in user_msg_lower or 'were executed last week' in user_msg_lower:
                filters["date_range"] = "last_7_days"
                filters["limit"] = 5
                filters["format"] = "table"  
            # Handle custom date ranges
            elif self._has_custom_date_range(user_msg_lower):
                filters["date_range"] = self._extract_custom_date_range(user_message)
                filters["limit"] = 5
                filters["format"] = "table"  
            
            # If asking for job statistics/summary
            if any(stat in user_msg_lower for stat in ['statistics', 'summary', 'stats']):
                from cloud_mcp.server import _get_job_summary_stats
                mcp_result = await _get_job_summary_stats(filters)
                tool_name = "get_job_summary_stats"
            else:
                # Regular job logs query
                mcp_result = await _query_job_logs(filters)
                tool_name = "query_job_logs"
            
            # Create result object
            class EnhancedLLMResult:
                def __init__(self, tool, filters, mcp_result):
                    self.tool_used = tool
                    self.table_used = "" 
                    self.filters = filters
                    self.mcp_result = mcp_result
                    self.is_database_operation = True
                    self.operation = None
                    self.context_preserved = False
                    self.context_info = {
                        "table": "",
                        "filters": filters,
                        "operation": tool
                    }
            
            return EnhancedLLMResult(tool_name, filters, mcp_result)
            
        except Exception as e:
            logger.error(f"Fallback job logs operation failed: {e}")
            return None
        
    async def _handle_clarification_request(self, llm_response: str, original_message: str) -> Any:
        """Handle cases where LLM needs clarification about table names or filters"""
        try:
            clarification_message = ""
            
            if "CLARIFY_TABLE_NEEDED" in llm_response:
                clarification_message = (
                    "I need clarification about which table you'd like to work with. "
                    "Please specify one of the following:\n\n"
                    "Available Tables:\n"
                    "• dsiactivities - Current activity logs\n"
                    "• dsitransactionlog - Current transaction logs\n"
                    "• dsiactivitiesarchive - Archived activity logs\n"
                    "• dsitransactionlogarchive - Archived transaction logs\n\n"
                )
            elif "CLARIFY_FILTERS_NEEDED" in llm_response:
                clarification_message = (
                    "I need more specific information about the date or filter criteria. "
                    "Please provide more details:\n\n"
                    "Date Filter Examples:\n"
                    "• \"records older than 10 months\"\n"
                    "• \"data from last year\"\n"
                    "• \"recent activities\"\n"
                )
            elif "CLARIFY_REQUEST_NEEDED" in llm_response:
                clarification_message = (
                    "I'm not sure what you'd like me to do. Could you please clarify your request?\n\n"
                    "I can help you with:\n\n"
                    "• View Data: \"Show table statistics\" or \"Count activities\"\n"
                    "• Archive Data: \"Archive old records\" or \"Archive activities older than 7 days\"\n"
                    "• Delete Data: \"Delete archived records\" (with proper date filters)\n"
                    "• System Info: \"Health check\" or \"Database status\"\n\n"
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
            • Check region connection status and current region information
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
            • Available tables: dsiactivities, dsitransactionlog, dsiactivitiesarchive, dsitransactionlogarchive

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
            User: "Hello" → "Hello! I'm your Cloud Inventory Log Management assistant. I can help you view database statistics, check region connections, manage archiving operations, and explain safety procedures. What would you like to know about your log data?"
            User: "What's the weather?" → "I'm specialized in Cloud Inventory Log Management and can't help with weather information. However, I can help you with database operations, viewing statistics, archiving procedures, and system health checks. What would you like to know about your log data?"
            User: "Delete Database" → "I cannot and will not perform destructive database operations like deleting entire databases. I'm designed with safety-first principles and only support controlled archiving operations with built-in safeguards. My role is limited to safe data viewing and controlled archiving with multiple safety checks. Would you like to see table statistics or learn about our archiving procedures instead?"
            User: "Drop table activities" → "I don't have permissions to drop tables or perform destructive schema operations. Table dropping is a dangerous operation that could cause data loss and is outside my mandate. I can help you with safe operations like viewing table statistics, archiving old records, or explaining our data retention policies. What would you like to know about the activities table?"
            User: "Show me something" → "I'd be happy to show you information! Could you be more specific about what you'd like to see? For example:\n• 'Show activities statistics'\n• 'Count transactions from last month'\n• 'Display archive table information'\n• 'Show database health status'\n\nWhat type of data are you interested in?"
            User: "Archive policy?" → "Our archiving policy includes several safety measures:\n• Records must be older than 7 days before archiving\n• Only archived records older than 30 days can be deleted\n• All operations require confirmation and are logged\n• Archive operations move data to dedicated archive tables (dsiactivitiesarchive, dsitransactionlogarchive)\n\nWould you like to see statistics for any specific table or learn about performing an archive operation?"
            User: "Which region is connected?" → [Use region_status tool to show current region connections, available regions, and connection status for all regions]
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
    
    async def chat_completion(
        self, 
        messages: List[Dict[str, str]], 
        temperature: float = 0.1, 
        max_tokens: int = 500
    ) -> Dict[str, Any]:
        """
        Direct chat completion method for LLM date filter and other services
        
        Args:
            messages: List of message dictionaries with 'role' and 'content'
            temperature: Sampling temperature (0.0 to 1.0)
            max_tokens: Maximum tokens in response
            
        Returns:
            OpenAI-compatible response format
        """
        try:
            url = f"{self.base_url}/chat/completions"
            
            payload = {
                "model": self.model_name,
                "messages": messages,
                "temperature": temperature,
                "max_tokens": max_tokens,
                "top_p": 0.9
            }
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=30)
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            logger.error(f"Chat completion error: {e}")
            # Return error response in OpenAI format
            return {
                "choices": [{
                    "message": {
                        "content": f"Error: Unable to process request - {str(e)}"
                    }
                }],
                "error": str(e)
            }
        
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
                    "Which region is connected?",
                    "What can you do?", 
                    "Explain archiving policy"
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
                    "Which region is connected?",
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
                           "Available Tables:\n\n"
                           "• dsiactivities - Current activity logs\n"
                           "• dsitransactionlog - Current transaction logs\n"
                           "• dsiactivitiesarchive - Archived activity logs\n"
                           "• dsitransactionlogarchive - Archived transaction logs\n\n",
                "suggestions": [
                    "Show activities statistics",
                    "Which region is connected?",
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
                    "Which region is connected?",
                    "Show table statistics",
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
                    "Which region is connected?", 
                    "What can you do?",
                    "System health check"
                ],
                "source": "fallback"
            }
    
    def _has_custom_date_range(self, user_msg_lower: str) -> bool:
        """Check if the message contains a custom date range pattern"""
        import re
        
        # Patterns for date ranges
        patterns = [
            r'september\s+\d+\s+to\s+september\s+\d+',  # "september 15 to september 30"
            r'from\s+september\s+\d+\s+to\s+september\s+\d+',  # "from september 15 to september 30"
            r'\d+/\d+\s+(?:and|to)\s+\d+/\d+',  # "9/15 and 9/30" or "9/15 to 9/30"
            r'between\s+\d+/\d+\s+and\s+\d+/\d+',  # "between 9/15 and 9/30"
            r'get\s+all\s+jobs\s+between\s+\d+/\d+\s+and\s+\d+/\d+',  # "get all jobs between 9/15 and 9/30"
            r'october\s+\d+\s+to\s+october\s+\d+',  # "october 1 to october 3"
            r'from\s+october\s+\d+\s+to\s+october\s+\d+',  # "from october 1 to october 3"
            r'from\s+\d+/\d+\s+to\s+\d+/\d+',  # "from 9/15 to 9/30"
            r'jobs\s+from\s+\d+/\d+\s+to\s+\d+/\d+',  # "jobs from 9/15 to 9/30"
        ]
        
        for pattern in patterns:
            if re.search(pattern, user_msg_lower):
                return True
        return False
    
    def _extract_custom_date_range(self, user_message: str) -> str:
        """Extract and format custom date range from user message"""
        import re
        from datetime import datetime
        
        user_msg_lower = user_message.lower()
        current_year = datetime.now().year
        
        # Pattern 1: "september 15 to september 30" or "from september 15 to september 30"
        pattern1 = r'(?:from\s+)?september\s+(\d+)\s+to\s+september\s+(\d+)'
        match1 = re.search(pattern1, user_msg_lower)
        if match1:
            start_day = match1.group(1)
            end_day = match1.group(2)
            return f"from_9/{start_day}/{current_year}_to_9/{end_day}/{current_year}"
        
        # Pattern 2: "9/15 and 9/30" or "9/15 to 9/30" or "between 9/15 and 9/30" or "from 9/15 to 9/30"
        pattern2 = r'(?:between\s+|from\s+)?(\d+)/(\d+)\s+(?:and|to)\s+(\d+)/(\d+)'
        match2 = re.search(pattern2, user_msg_lower)
        if match2:
            start_month = match2.group(1)
            start_day = match2.group(2)
            end_month = match2.group(3)
            end_day = match2.group(4)
            return f"from_{start_month}/{start_day}/{current_year}_to_{end_month}/{end_day}/{current_year}"
        
        # Pattern 3: "october 1 to october 3" or "from october 1 to october 3"
        pattern3 = r'(?:from\s+)?october\s+(\d+)\s+to\s+october\s+(\d+)'
        match3 = re.search(pattern3, user_msg_lower)
        if match3:
            start_day = match3.group(1)
            end_day = match3.group(2)
            return f"from_10/{start_day}/{current_year}_to_10/{end_day}/{current_year}"
        
        # Pattern 4: "jobs from M/D to M/D"
        pattern4 = r'jobs\s+from\s+(\d+)/(\d+)\s+to\s+(\d+)/(\d+)'
        match4 = re.search(pattern4, user_msg_lower)
        if match4:
            start_month = match4.group(1)
            start_day = match4.group(2)
            end_month = match4.group(3)
            end_day = match4.group(4)
            return f"from_{start_month}/{start_day}/{current_year}_to_{end_month}/{end_day}/{current_year}"
        
        # Default fallback - couldn't parse the date range
        import logging
        logger = logging.getLogger(__name__)
        logger.warning(f"Could not parse custom date range from: {user_message}")
        return "today"  # Fallback to today
    
    def _extract_table_names_from_sql(self, sql_query: str) -> List[str]:
        """Extract table names from SQL query"""
        import re
        
        if not sql_query:
            return []
        
        # Convert to uppercase for matching
        sql_upper = sql_query.upper()
        
        tables = []
        
        # Extract all table names from FROM and JOIN clauses
        from_pattern = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        join_pattern = r'\b(?:INNER\s+|LEFT\s+|RIGHT\s+|FULL\s+)?JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)'
        
        # Find all FROM matches
        from_matches = re.findall(from_pattern, sql_upper)
        tables.extend([match.lower() for match in from_matches])
        
        # Find all JOIN matches
        join_matches = re.findall(join_pattern, sql_upper)
        tables.extend([match.lower() for match in join_matches])
        
        # Filter to only include our known database tables
        known_tables = [
            'dsiactivities', 'dsitransactionlog', 'job_logs',
            'dsiactivitiesarchive', 'dsitransactionlogarchive'
        ]
        
        filtered_tables = []
        for table in tables:
            if table in known_tables:
                filtered_tables.append(table)
            # Also check if it contains our table names (for aliases like 'a' for activities)
            elif any(known in table for known in known_tables):
                filtered_tables.append(table)
        
        # Remove duplicates and return
        return list(set(filtered_tables))
    
    def _extract_primary_table_from_sql(self, sql_query: str) -> str:
        """Extract the primary table name from SQL query (FROM clause)"""
        tables = self._extract_table_names_from_sql(sql_query)
        if not tables:
            return None
            
        # Prioritize main tables over archive tables
        main_tables = [t for t in tables if not t.endswith('archive')]
        if main_tables:
            return main_tables[0]
        
        # Return first table if no main tables found
        return tables[0] if tables else None


