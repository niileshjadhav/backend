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
        """Extract table name and filters from conversation context"""
        context_info = {
            "last_table": None,
            "last_filters": {},
            "last_operation": None
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
            # "transactions older than X" or "show transactions" → use main table
            if "archive" in user_msg_lower:
                return "dsitransactionlogarchive"
            return "dsitransactionlog"
        elif "activit" in user_msg_lower:
            # "activities older than X" or "show activities" → use main table  
            if "archive" in user_msg_lower:
                return "dsiactivitiesarchive"
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
            (r"recent|latest", lambda m: {"date_filter": "recent"})
        ]
        
        for pattern, filter_func in date_patterns:
            match = re.search(pattern, user_msg_lower)
            if match:
                filters.update(filter_func(match))
                break
        
        # If no explicit filters in message, check if we should inherit from context
        if not filters and context_info.get("last_filters"):
            # Inherit filters for follow-up operations like "archive them", "count", etc.
            follow_up_keywords = ["them", "those", "it", "archive", "delete", "count"]
            if any(keyword in user_msg_lower for keyword in follow_up_keywords):
                filters = context_info["last_filters"].copy()
        
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

            This helps understand references like "show me more", "archive those records", "delete them", etc.
            """
            
            enhanced_prompt = f"""
            You are an expert database operations assistant. Analyze the user's request and convert it to the appropriate MCP tool call.

            User Request: "{user_message}"
            {context_section}

            Available Database Tables:
            - dsiactivities: Main activity logs (current records)
            - dsitransactionlog: Main transaction logs (current records)  
            - dsiactivitiesarchive: Archived activity logs (old records)
            - dsitransactionlogarchive: Archived transaction logs (old records)

            Available MCP Tools:
            1. get_table_stats - Use for ACTIVITIES/TRANSACTIONS/ARCHIVE queries (shows counts, not records)
            2. archive_records - Use for archiving old records to archive tables
            3. delete_archived_records - Use for deleting records from archive tables
            4. health_check - Use for system health/status checks
            5. region_status - Use for region connection status and current region information
            6. query_job_logs - Use for JOB LOGS queries (shows job execution history, status, records affected)
            7. get_job_summary_stats - Use for JOB STATISTICS queries (shows job performance metrics, success rates)

            JOB LOGS FILTERING RULES - CRITICAL:
            - SINGULAR vs PLURAL: "job" (singular) should use "limit": 1, "jobs" (plural) should use default limit
            - "show jobs" (general): Always add "limit": 5 and "format": "table" to get recent 5 jobs in table format
            - LAST/LATEST/MOST RECENT: Always add "limit": 1 and "format": "table" to get only the most recent record in table format
            - STATUS FILTERS: "successful" → "SUCCESS", "failed" → "FAILED", "running" → "IN_PROGRESS"
            - DATE FILTERS: "today" → "date_range": "today", "yesterday" → "date_range": "yesterday"
            - MULTIPLE FILTERS: Combine filters as needed, e.g., {{"status": "SUCCESS", "limit": 1, "date_range": "today"}}
            - Examples:
              * "last job" → {{"limit": 1, "format": "table"}}
              * "last successful job" → {{"status": "SUCCESS", "limit": 1, "format": "table"}}
              * "reason of last failed job" → {{"status": "FAILED", "limit": 1, "format": "reason_only"}}
              * "failed jobs today" → {{"status": "FAILED", "date_range": "today"}}
              * "latest archive job" → {{"job_type": "ARCHIVE", "limit": 1, "format": "table"}}

            CONTEXT HANDLING - CRITICAL:
            - ALWAYS examine conversation history to understand incomplete requests
            - If user says "for then X days", "what about Y months", "for activities", etc. - use context to determine table and operation
            - If user says "archive them", "delete them", "archive those records" - use context to determine BOTH table AND filters from previous query
            - PRESERVE BOTH TABLE AND FILTERS: Extract exact table name and date filters from previous query
            - Look for previous queries to understand what table/operation user is referring to
            - Context patterns: "for then", "what about", "how about", "and for", "also for", "archive them", "delete them", "those records", "count", "count them", "show count"
            - Example: Previous "activities older than 30 days" → "archive them" = MUST use both "dsiactivities" table AND "older_than_30_days" filter
            - Example: Previous "archived transactions older than 10 days" → "count" = MUST use "dsitransactionlogarchive" table (preserve archive context)
            - Example: Previous "archived transactions older than 10 days" → "delete them" = MUST use "dsitransactionlogarchive" table (preserve archive context)
            - Example: Previous "archived activities older than 30 days" → "delete them" = MUST use "dsiactivitiesarchive" table (preserve archive context)
            - Example: Previous "archived transactions" → "delete those records" = MUST use "dsitransactionlogarchive" table (preserve archive context)
            - Example: Previous "archived activities older than 30 days" → "show transactions" = MUST use "dsitransactionlog" table (NEW explicit table request)
            
            TABLE CONTINUITY RULES - CRITICAL:
            - If previous operation was on "dsitransactionlog" → any follow-up "count", "show", "list" should ALSO use "dsitransactionlog"
            - If previous operation was on "dsiactivities" → any follow-up "count", "show", "list" should ALSO use "dsiactivities"
            - Look for "[Context: Previous operation on table: TABLE_NAME]" markers in conversation history
            - When user says just "count" without specifying table, use the MOST RECENT table from conversation history
            - DO NOT default to "dsiactivities" if recent context shows operations on "dsitransactionlog" or archive tables
            - SMART CONTEXT DECISION: Use your intelligence to determine if the query references previous results or is a new request:
              * References to previous: "archive them", "delete those", "count", "older than X", "yes archive", "remove all that data" → PRESERVE previous table context
              * New explicit requests: "show activities", "transactions older than X", "archive activities" → Use specified table type
              * CRITICAL: After "archived transactions" query, "delete them" MUST use dsitransactionlogarchive (preserve archive context)
              * CRITICAL: After "archived activities" query, "delete them" MUST use dsiactivitiesarchive (preserve archive context)
              * Use natural language understanding rather than rigid keyword matching
            
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
            - JOB QUERIES (HIGHEST PRIORITY): Any mention of "job", "jobs", "job logs", "job statistics", "job summary" → ALWAYS use job-specific tools
              * Job counts/statistics: "count of jobs", "how many jobs", "job statistics" → Use get_job_summary_stats
              * Job logs/details: "show jobs", "job logs", "recent jobs" → Use query_job_logs  
            - DATA QUERIES: COUNT/HOW MANY/STATISTICS about records (NOT jobs) → ALWAYS use get_table_stats
            - DATA OPERATIONS: "show activities", "list transactions", "display archive records" → ALWAYS use get_table_stats (show counts, not records)
            - CRITICAL: "archived transactions" or "archived activities" = QUERY operations (show stats), NOT delete operations
            - DELETE operations require explicit confirmation or follow-up commands like "delete them"
            - MAIN vs ARCHIVE TABLE SELECTION:
              * "count transactions", "show transactions", "list transactions" → Use MAIN table (dsitransactionlog)
              * "count archived transactions", "show archived transactions" → Use ARCHIVE table (dsitransactionlogarchive)
              * Only use archive tables when "archive" or "archived" is explicitly mentioned
            - GENERAL DATABASE STATS (e.g., "show table statistics", "database statistics") → use get_table_stats with NO table name (leave empty)
            - REGION QUERIES: "which region", "current region", "region status", "connected regions", "what region" → ALWAYS use region_status
            - POLICY/EXPLANATION QUESTIONS: "what is archive policy", "how does archiving work", "what does archive mean" → Return None (conversational)
            - INTELLIGENT Table Selection: 
              * Use your language understanding to distinguish between NEW requests vs REFERENCES to previous results
              * NEW requests: User mentions specific table type → Use that table (main tables unless explicitly archived)
              * CRITICAL: "count transactions", "show activities" = NEW requests → Use MAIN tables (ignore previous archive context)
              * REFERENCES: User refers to previous results in any natural way → Preserve exact previous table context
              * Trust your understanding of natural language intent over rigid pattern matching
            - NATURAL LANGUAGE CONTEXT UNDERSTANDING: Use your intelligence to understand when users are:
              1. Referring to previous results (any natural expression like "archive those", "delete the old stuff", "delete them", "remove all that", etc.)
              2. Making new requests (explicit table mentions or completely new topics)
              3. NEVER ask for clarification when context is clear from conversation history
              4. If previous query was about "archived X", then "delete them" clearly refers to deleting those archived records
              5. Don't rely on hardcoded patterns - understand the conversational flow and user intent
            - CONTEXT PRESERVATION: If previous query was about archive tables, follow-up date queries (older than X, from Y) should STAY on archive tables!
            - Date filters: Parse natural language dates
               - "older than 10 months" → {{"date_filter": "older_than_10_months"}}
               - "older than 12 months" → {{"date_filter": "older_than_12_months"}}
               - "from last year" → {{"date_filter": "from_last_year"}}
               - "recent" or "latest" → {{"date_filter": "recent"}}
               - "for then X days" → {{"date_filter": "older_than_X_days"}} (use context to determine table)

            Examples with detailed analysis:

            CLEAR REQUESTS (proceed with MCP_TOOL):
            "count of activities older than 12 months"
            → Analysis: COUNT query + date filter + specific table clearly identified + NO archive context
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_months"}}

            "show activities from last month" 
            → Analysis: SHOW query + activities table clearly identified + NO archive context
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "from_last_month"}}

            "count of archived activities older than 6 months"
            → Analysis: COUNT query + ARCHIVE explicitly mentioned + date filter
            → MCP_TOOL: get_table_stats dsiactivitiesarchive {{"date_filter": "older_than_6_months"}}

            "count transactions"
            → Analysis: NEW explicit request - count main transaction table (NOT archive)  
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}
            
            "archived transactions"
            → Analysis: QUERY operation - user wants to see archived transaction stats/count (NOT delete)
            → MCP_TOOL: get_table_stats dsitransactionlogarchive {{}}
            
            "count of archived transactions older than 3 months"
            → Analysis: QUERY operation + ARCHIVE explicitly mentioned + date filter
            → MCP_TOOL: get_table_stats dsitransactionlogarchive {{"date_filter": "older_than_3_months"}}

            "count transactions"
            → Analysis: COUNT query + transactions table + NO archive mentioned → Use MAIN table
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}
            
            "list transactions"
            → Analysis: LIST query + transactions table clearly identified + NO archive context
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            "archive activities older than 12 months"
            → Analysis: ARCHIVE operation + date filter + activities table clearly identified
            → MCP_TOOL: archive_records dsiactivities {{"date_filter": "older_than_12_months"}}

            "which region is connected now" or "current region status" or "what region"
            → Analysis: REGION STATUS query + region connection information needed
            → MCP_TOOL: region_status  {{}}

            "show table statistics" or "database statistics"
            → Analysis: GENERAL STATISTICS query + no specific table needed
            → MCP_TOOL: get_table_stats  {{}}

            "show jobs" or "recent jobs"
            → Analysis: JOB LOGS query + shows recent job execution details + limit to 5 most recent
            → MCP_TOOL: query_job_logs {{"limit": 5, "format": "table"}}

            "show job logs" or "recent job logs" or "job execution history"
            → Analysis: JOB LOGS query + shows job execution details
            → MCP_TOOL: query_job_logs {{"limit": 10}}

            "show me failed jobs" or "failed job logs"
            → Analysis: JOB LOGS query + status filter for failed jobs
            → MCP_TOOL: query_job_logs {{"status": "FAILED"}}

            "show me successful jobs" or "successful job logs"
            → Analysis: JOB LOGS query + status filter for successful jobs
            → MCP_TOOL: query_job_logs {{"status": "SUCCESS"}}

            "last successful job" or "latest successful job" or "most recent successful job"
            → Analysis: JOB LOGS query + status filter + limit to 1 record (singular "job")
            → MCP_TOOL: query_job_logs {{"status": "SUCCESS", "limit": 1, "format": "table"}}

            "last job" or "latest job" or "most recent job"
            → Analysis: JOB LOGS query + limit to 1 record (singular "job", most recent)
            → MCP_TOOL: query_job_logs {{"limit": 1, "format": "table"}}

            "show last failed job" or "latest failed job"
            → Analysis: JOB LOGS query + status filter + limit to 1 record
            → MCP_TOOL: query_job_logs {{"status": "FAILED", "limit": 1, "format": "table"}}

            "reason of last failed job" or "reason for last failed job" or "why did last job fail" or "last failed job reason" or "what caused last job to fail" or "failure reason for last job" or "error message from last failed job"
            → Analysis: JOB LOGS query + status filter + limit to 1 record + reason only format
            → MCP_TOOL: query_job_logs {{"status": "FAILED", "limit": 1, "format": "reason_only"}}

            "job statistics" or "job summary" or "job performance"
            → Analysis: JOB STATISTICS query + shows success rates and metrics
            → MCP_TOOL: get_job_summary_stats {{}}

            "count of successful jobs" or "how many successful jobs" or "number of successful jobs" or "successful job count" or "successful jobs count"
            → Analysis: JOB STATISTICS query + shows only successful job count
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "successful"}}

            "count of failed jobs" or "how many failed jobs" or "number of failed jobs" or "failed job count" or "failed jobs count"
            → Analysis: JOB STATISTICS query + shows only failed job count
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "failed"}}

            "count of jobs" or "how many jobs" or "number of jobs" or "total job count" or "job count" or "jobs count"
            → Analysis: JOB STATISTICS query + shows only total job count
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "total"}}

            "count of successful jobs in last month" or "count of successful jobs from last month" or "successful jobs in last month"
            → Analysis: JOB STATISTICS query with date filter + shows only successful job count for previous calendar month
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "successful", "date_range": "last_month"}}

            "count of failed jobs in last month" or "count of failed jobs from last month" or "failed jobs in last month"
            → Analysis: JOB STATISTICS query with date filter + shows only failed job count for previous calendar month  
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "failed", "date_range": "last_month"}}

            "count of jobs in last month" or "count of jobs from last month" or "jobs in last month"
            → Analysis: JOB STATISTICS query with date filter + shows only total job count for previous calendar month
            → MCP_TOOL: get_job_summary_stats {{"format": "count_only", "count_type": "total", "date_range": "last_month"}}

            "count of jobs today" or "jobs today" or "job count today"
            → Analysis: JOB STATISTICS query with date filter + shows job counts for today
            → MCP_TOOL: get_job_summary_stats {{"date_range": "today"}}

            "count of jobs this week" or "jobs this week" or "job count this week"
            → Analysis: JOB STATISTICS query with date filter + shows job counts for this week
            → MCP_TOOL: get_job_summary_stats {{"date_range": "this_week"}}

            "count of jobs from September 15 to September 30" or "jobs from 9/15 to 9/30" or "job count from 9/15/2025 to 9/30/2025"
            → Analysis: JOB STATISTICS query with custom date range + convert to from_9/15/2025_to_9/30/2025 format
            → MCP_TOOL: get_job_summary_stats {{"date_range": "from_9/15/2025_to_9/30/2025"}}

            "count of failed jobs from September 15 to September 30" or "failed jobs from 9/15 to 9/30"
            → Analysis: JOB STATISTICS query with custom date range for failed jobs
            → MCP_TOOL: get_job_summary_stats {{"date_range": "from_9/15/2025_to_9/30/2025"}}

            "count of successful jobs from October 1 to October 3" or "successful jobs from 10/1 to 10/3"
            → Analysis: JOB STATISTICS query with custom date range for successful jobs
            → MCP_TOOL: get_job_summary_stats {{"date_range": "from_10/1/2025_to_10/3/2025"}}

            "jobs from today" or "today's jobs"
            → Analysis: JOB LOGS query + date filter for today
            → MCP_TOOL: query_job_logs {{"date_range": "today"}}

            "archive jobs from last week"
            → Analysis: JOB LOGS query + job type filter + date filter
            → MCP_TOOL: query_job_logs {{"job_type": "ARCHIVE", "date_range": "last_7_days"}}

            "jobs with 0 records affected" or "jobs with zero records affected" or "jobs that affected 0 records"
            → Analysis: JOB LOGS query + filter for jobs with no records affected
            → MCP_TOOL: query_job_logs {{"zero_records_only": true}}

            "jobs with no records affected" or "jobs that didn't affect any records"
            → Analysis: JOB LOGS query + filter for jobs with no records affected
            → MCP_TOOL: query_job_logs {{"zero_records_only": true}}

            "jobs that affected records" or "jobs with records affected" or "jobs that changed data"
            → Analysis: JOB LOGS query + filter for jobs with records affected
            → MCP_TOOL: query_job_logs {{"has_records_only": true}}

            "jobs that affected more than 100 records" or "jobs with over 100 records"
            → Analysis: JOB LOGS query + minimum records filter
            → MCP_TOOL: query_job_logs {{"min_records_affected": 100}}

            "jobs that affected less than 10 records" or "jobs with under 10 records"
            → Analysis: JOB LOGS query + maximum records filter
            → MCP_TOOL: query_job_logs {{"max_records_affected": 9}}

            "delete jobs" or "deletion jobs" or "jobs that deleted data"
            → Analysis: JOB LOGS query + job type filter for delete operations
            → MCP_TOOL: query_job_logs {{"job_type": "DELETE"}}

            "archive jobs" or "archiving jobs" or "jobs that archived data"
            → Analysis: JOB LOGS query + job type filter for archive operations
            → MCP_TOOL: query_job_logs {{"job_type": "ARCHIVE"}}

            "failed archive jobs" or "failed archiving jobs"
            → Analysis: JOB LOGS query + job type filter + status filter
            → MCP_TOOL: query_job_logs {{"job_type": "ARCHIVE", "status": "FAILED"}}

            "successful delete jobs" or "successful deletion jobs"
            → Analysis: JOB LOGS query + job type filter + status filter
            → MCP_TOOL: query_job_logs {{"job_type": "DELETE", "status": "SUCCESS"}}

            "jobs on dsiactivities table" or "jobs for activities table"
            → Analysis: JOB LOGS query + table name filter
            → MCP_TOOL: query_job_logs {{"table_name": "dsiactivities"}}

            "jobs on archive tables" or "jobs for archived data"
            → Analysis: JOB LOGS query + table name filter for archive tables
            → MCP_TOOL: query_job_logs {{"table_name": ["dsiactivitiesarchive", "dsitransactionlogarchive"]}}

            "jobs containing error" or "jobs with error message" or "jobs that mention timeout"
            → Analysis: JOB LOGS query + text search in reason field
            → MCP_TOOL: query_job_logs {{"reason_contains": "error"}}

            "yesterday's failed jobs" or "failed jobs from yesterday"
            → Analysis: JOB LOGS query + date range + status filter
            → MCP_TOOL: query_job_logs {{"date_range": "yesterday", "status": "FAILED"}}

            "running jobs" or "jobs in progress" or "currently running jobs"
            → Analysis: JOB LOGS query + status filter for in-progress
            → MCP_TOOL: query_job_logs {{"status": "IN_PROGRESS"}}

            "which region is connected" or "current region" or "region status"
            → Analysis: REGION STATUS query + region information needed
            → MCP_TOOL: region_status  {{}}

            CONTEXTUAL FOLLOW-UPS (use conversation history):
            Previous: "activities older than 15 days" → User: "for then 12 days"
            → Analysis: Context shows previous query was about activities + new time period
            → MCP_TOOL: get_table_stats dsiactivities {{"date_filter": "older_than_12_days"}}

            Previous: "count transactions" → User: "what about 30 days old"
            → Analysis: Context shows previous query was about transactions + new filter
            → MCP_TOOL: get_table_stats dsitransactionlog {{"date_filter": "older_than_30_days"}}

            Previous: "archive transactions older than 30 days" → User: "count"
            → Analysis: Context shows previous operation was on transactions + now count same table
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            Previous: "count of archived transactions" → User: "older than 20 days"
            → Analysis: Context shows previous query was about ARCHIVE transactions + new date filter + PRESERVE archive context
            → MCP_TOOL: get_table_stats dsitransactionlogarchive {{"date_filter": "older_than_20_days"}}

            Previous: "show transactions" → User: "count"
            → Analysis: Context shows previous query was about transactions + now count same table
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            Previous: "transactions statistics" → User: "count them"
            → Analysis: Context shows previous query was about transactions + now count same table
            → MCP_TOOL: get_table_stats dsitransactionlog {{}}

            Previous: "show archive records" → User: "for activities only"
            → Analysis: Context shows archive request + now specify activities archive
            → MCP_TOOL: get_table_stats dsiactivitiesarchive {{}}

            Previous: "show archived transactions" → User: "older than 5 days"
            → Analysis: Context shows archive transactions + new date filter + PRESERVE archive context
            → MCP_TOOL: get_table_stats dsitransactionlogarchive {{"date_filter": "older_than_5_days"}}

            CRITICAL: DISTINGUISH QUERY vs DELETE OPERATIONS:
            
            QUERY OPERATIONS (show/count archived data):
            "archived transactions" → User wants to SEE archived transactions
            → Analysis: QUERY operation - show count/stats of archived records
            → MCP_TOOL: get_table_stats dsitransactionlogarchive {{}}
            
            "archived activities older than 6 months" → User wants to SEE archived activities  
            → Analysis: QUERY operation - show count/stats with date filter
            → MCP_TOOL: get_table_stats dsiactivitiesarchive {{"date_filter": "older_than_6_months"}}
            
            DELETE OPERATIONS (delete archived data):
            Previous query: "archived transactions" → User: "delete them"
            → Analysis: User wants to DELETE archived records from previous query
            → MCP_TOOL: delete_archived_records dsitransactionlogarchive {{}}
            
            Previous query: "archived activities older than 6 months" → User: "delete those"
            → Analysis: User wants to DELETE archived records with same filter  
            → MCP_TOOL: delete_archived_records dsiactivitiesarchive {{"date_filter": "older_than_6_months"}}
            
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
                    # Create fallback archive operation with context
                    logger.info(f"Providing fallback archive operation for: '{user_message}'")
                    return await self._create_fallback_archive_operation(user_message, conversation_context)
                elif self._is_stats_request(user_message):
                    # Create fallback stats operation with context
                    logger.info(f"Providing fallback stats operation for: '{user_message}'")
                    return await self._create_fallback_stats_operation(user_message, conversation_context)
                
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
            
            if table_name and table_name not in valid_tables:
                logger.warning(f"Invalid table name '{table_name}' provided by LLM. Valid tables: {valid_tables}")
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
                        
            # Execute stats operation
            mcp_result = await get_table_stats(table_name, filters, "system")
            
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


