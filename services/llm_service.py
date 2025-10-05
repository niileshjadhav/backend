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
            for table in ["dsitransactionlogarchive", "dsiactivitiesarchive", "dsitransactionlog", "dsiactivities"]:
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
        """Determine filters using message content and context"""
        user_msg_lower = user_message.lower()
        filters = {}
        
        # First priority: Explicit filters in current message
        import re
        
        # Look for date patterns in current message
        date_patterns = [
            (r"older than (\d+) months?", lambda m: {"date_filter": f"older_than_{m.group(1)}_months"}),
            (r"older than (\d+) days?", lambda m: {"date_filter": f"older_than_{m.group(1)}_days"}),
            (r"older than (\d+) years?", lambda m: {"date_filter": f"older_than_{m.group(1)}_years"}),
            (r"from last year", lambda m: {"date_filter": "from_last_year"}),
            (r"from last month", lambda m: {"date_filter": "from_last_month"}),
            (r"yesterday('s)?\s+(transactions|activities)", lambda m: {"date_filter": "yesterday"}),
            (r"(transactions|activities)\s+from\s+yesterday", lambda m: {"date_filter": "yesterday"}),
            (r"(from\s+the\s+)?past\s+(\d+)\s+days", lambda m: {"date_filter": f"from_last_{m.group(2)}_days"}),
            (r"(from\s+)?last\s+(\d+)\s+days", lambda m: {"date_filter": f"from_last_{m.group(2)}_days"}),
            (r"recent|latest", lambda m: {"date_filter": "recent"})
        ]
        
        for pattern, filter_func in date_patterns:
            match = re.search(pattern, user_msg_lower)
            if match:
                filters.update(filter_func(match))
                break
        
        # If no explicit filters in message, intelligently determine if this is a follow-up based on context
        if not filters:
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
            Previous Job Operation: {context_info.get('last_job_operation', 'None')}
            Previous Job Filters: {context_info.get('last_job_filters', {})}
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
            1. get_table_stats - For data queries (counts, statistics, "show", "list")
            2. archive_records - For archiving old records to archive tables
            3. delete_archived_records - For deleting records from archive tables
            4. health_check - For system health/status checks
            5. region_status - For region connection status and current region info
            6. query_job_logs - For job execution history, logs, status
            7. get_job_summary_stats - For job performance metrics, success rates

            CRITICAL ANALYSIS RULES:

            1. JOB QUERIES (HIGHEST PRIORITY):
            - Any mention of "job", "jobs", "job logs" → Use job tools
            - Job details/logs: "show jobs", "recent jobs", "failed jobs" → query_job_logs
            - Job statistics: "count of jobs", "job summary" → get_job_summary_stats
            - "last job" → {{"limit": 1, "format": "table"}} (NO status filter unless specified)
            - "last successful job" → {{"status": "SUCCESS", "limit": 1, "format": "table"}}
            - "show jobs" → {{"limit": 5, "format": "table"}}
            - "successful jobs from last week" → {{"status": "SUCCESS", "date_range": "last_7_days", "format": "table"}}
            - "failed jobs" → {{"status": "FAILED", "format": "table"}}
            - "archive jobs" = JOB LOGS about archive operations, NOT archived data

            2. DATA QUERIES:
            - "count", "show", "list", "statistics" about records → get_table_stats
            - "count activities" → get_table_stats dsiactivities {{}}
            - "archived transactions" → get_table_stats dsitransactionlogarchive {{}}
            - Main vs Archive: Use archive tables only when "archive"/"archived" explicitly mentioned

            3. OPERATIONS:
            - "archive" with records/activities/transactions → archive_records
            - "delete" with archived data → delete_archived_records

            4. REGION QUERIES:
            - "region", "connected", "current region" → region_status

            CONTEXT HANDLING (CRITICAL):
            - PRESERVE context from previous queries for follow-up requests
            - "archive them", "delete them", "count them" → Use EXACT table + filters from previous query
            - Example: Previous "activities older than 30 days" → "archive them" = archive_records dsiactivities {{"date_filter": "older_than_30_days"}}
            - Archive context preservation: After "archived X" query, follow-ups stay on archive table

            TABLE SELECTION LOGIC:
            - NEW explicit requests: "count transactions" → dsitransactionlog (main table)
            - CONTEXTUAL references: Use previous table from conversation
            - Archive preservation: "archived X" context + follow-up → keep archive table

            DATE FILTER PARSING:
            - "older than 10 months" → {{"date_filter": "older_than_10_months"}}
            - "from last year" → {{"date_filter": "from_last_year"}}
            - "yesterday" → {{"date_filter": "yesterday"}}
            - "today" → {{"date_range": "today"}} (for jobs)

            ERROR HANDLING:
            - Greetings, policy questions, off-topic → Return None
            - Destructive operations (drop table, delete database) → Return None
            - Vague requests without context → CLARIFY_[TYPE]_NEEDED

            RESPONSE FORMAT EXAMPLES:
            "count activities older than 12 months" → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_months"}}
            "last job" → MCP_TOOL: query_job_logs {{"limit": 1, "format": "table"}}
            "failed jobs today" → MCP_TOOL: query_job_logs {{"status": "FAILED", "date_range": "today"}}
            "archive old activities" → MCP_TOOL: archive_records dsiactivities {{}}
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
            tools_without_tables = ["health_check", "region_status", "query_job_logs", "get_job_summary_stats"]
            
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
            valid_tools = ["get_table_stats", "archive_records", "delete_archived_records", "health_check", "region_status", "query_job_logs", "get_job_summary_stats"]
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
        """Check if message is requesting job logs/execution information"""
        message_lower = message.lower().strip()
        
        # Job execution patterns
        job_execution_patterns = [
            'last job', 'latest job', 'most recent job', 'recent job',
            'last job executed', 'latest job executed', 'most recent job executed',
            'last executed job', 'recent executed job', 'show job', 'job logs',
            'job history', 'job status', 'job execution', 'executed job',
            'show jobs', 'recent jobs', 'job statistics', 'job summary',
            'archive jobs', 'delete jobs', 'failed jobs', 'successful jobs',
            'jobs from', 'jobs today', 'jobs yesterday', 'all jobs', 'get jobs',
            'jobs between', 'list jobs', 'get all jobs', 'display jobs',
            'jobs executed', 'jobs were executed', 'jobs have run',
            'jobs ran', 'what jobs', 'which jobs', 'jobs from last',
            'last job archived', 'job archived', 'archived job',
            'last job deleted', 'job deleted', 'deleted job'
        ]
        
        return any(pattern in message_lower for pattern in job_execution_patterns)

    def _is_archive_request(self, message: str) -> bool:
        """Check if message is requesting an archive operation using semantic analysis"""
        message_lower = message.lower().strip()
        
        # Check for policy/explanation questions first (these should not be archive operations)
        explanation_indicators = [
            'what does', 'what is', 'explain', 'how does', 'policy', 'means what',
            'for how much', 'can you explain'
        ]
        
        # If it's clearly asking for explanation/policy, it's not an operation request
        if any(indicator in message_lower for indicator in explanation_indicators) and 'archive' in message_lower:
            return False
        
        # Use semantic understanding - look for action-oriented language patterns
        # that suggest actual archive operations rather than just mentions
        action_patterns = [
            ('archive', ['data', 'records', 'activities', 'transactions', 'old', 'them', 'those']),
            ('move', ['archive', 'old']),
            ('start', ['archive']),
            ('run', ['archive'])
        ]
        
        for action, objects in action_patterns:
            if action in message_lower:
                # Check if any object words appear near the action
                if any(obj in message_lower for obj in objects):
                    return True
                # Or if it's a simple direct command like "archive"
                if len(message_lower.split()) <= 3 and action == message_lower.strip():
                    return True
        
        return False

    def _is_stats_request(self, message: str) -> bool:
        """Check if message is requesting statistics/counts using semantic analysis"""
        message_lower = message.lower().strip()
        
        # Look for information-seeking patterns rather than just keywords
        query_patterns = [
            # Direct information requests
            'show', 'display', 'list', 'view', 'overview',
            # Counting requests  
            'count', 'how many', 'number of',
            # Statistical requests
            'statistics', 'stats', 'summary',
            # Question patterns
            'what are', 'what is', 'how much',
            # Job execution patterns (implicit queries)
            'last', 'latest', 'most recent', 'recent'
        ]
        
        # Check for database-related objects that suggest data queries
        data_objects = [
            'activities', 'transactions', 'records', 'data', 'logs',
            'archive', 'archived', 'table', 'database', 'job', 'jobs'
        ]
        
        # Special patterns for database overview
        overview_patterns = [
            'overview of all database tables',
            'overview of database tables',
            'all database tables',
            'database overview'
        ]
        
        # Check for overview patterns first
        if any(pattern in message_lower for pattern in overview_patterns):
            return True
        
        # More intelligent detection: look for query patterns combined with data objects
        has_query_pattern = any(pattern in message_lower for pattern in query_patterns)
        has_data_object = any(obj in message_lower for obj in data_objects)
        
        # Also detect simple contextual references that might be stats requests
        contextual_patterns = ['them', 'those', 'it', 'that data', 'total', 'in total']
        has_contextual_ref = any(pattern in message_lower for pattern in contextual_patterns)
        
        # Special handling for job execution patterns
        job_execution_patterns = [
            'last job', 'latest job', 'most recent job', 'recent job',
            'last job executed', 'latest job executed', 'most recent job executed',
            'last executed job', 'recent executed job'
        ]
        is_job_execution_query = any(pattern in message_lower for pattern in job_execution_patterns)
        
        return (has_query_pattern and has_data_object) or (has_query_pattern and has_contextual_ref) or is_job_execution_query

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


