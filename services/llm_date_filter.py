"""
LLM-based Date Filter Service
Universal date parsing and filtering system using LLM intelligence
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
import json
import re
from .llm_service import OpenAIService

logger = logging.getLogger(__name__)

class LLMDateFilter:
    """
    Universal date filter service using LLM for intelligent date parsing
    Can be applied to any database query or filtering operation
    """
    
    def __init__(self):
        self.llm_service = OpenAIService()
    
    async def parse_date_filter(
        self, 
        user_input: str, 
        context: Optional[Dict[str, Any]] = None,
        reference_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Parse any date expression into a universal filter format
        
        Args:
            user_input: Natural language date expression
            context: Additional context (table type, previous queries, etc.)
            reference_date: Reference date for relative calculations
            
        Returns:
            Universal date filter that can be applied anywhere
        """
        try:
            ref_date = reference_date or datetime.now()
            
            # Build context string
            context_info = ""
            if context:
                context_info = f"""
Additional Context:
- Table Type: {context.get('table_type', 'unknown')}
- Previous Filters: {context.get('previous_filters', {})}
- User History: {context.get('user_history', 'none')}
"""

            prompt = f"""
You are an expert date parsing system for database queries. Parse the following date expression into a structured filter.

Current Date/Time: {ref_date.strftime('%Y-%m-%d %H:%M:%S')}
User Input: "{user_input}"
{context_info}

PARSING RULES:
1. Handle ALL types of date expressions:
   - Relative: "last week", "older than 30 days", "past 6 months"
   - Absolute: "January 2024", "2024-01-01", "Q1 2025"
   - Ranges: "from January to March", "between 2024-01-01 and 2024-12-31"
   - Contextual: "this quarter", "yesterday", "recent", "latest"
   - Complex: "holiday season", "busy period", "maintenance window"

2. For vague expressions, use reasonable business assumptions:
   - "recent/latest" = last 7 days
   - "old data" = older than 1 year
   - "holiday season" = December 1 to January 7
   - "business hours" = 9 AM to 5 PM weekdays

3. Always provide both start and end dates for ranges
4. Use 00:00:00 for start times and 23:59:59 for end times

RESPONSE FORMAT (JSON only):
{{
    "success": true,
    "filter_type": "date_range|single_date|relative_filter",
    "operation": "between|greater_than|less_than|equals",
    "start_date": "YYYY-MM-DD HH:MM:SS",
    "end_date": "YYYY-MM-DD HH:MM:SS",
    "description": "Human readable description",
    "sql_condition": "SQL WHERE clause condition",
    "assumptions": ["any assumptions made"],
    "confidence": 0.95
}}

Examples:
- "last 30 days" → {{"operation": "greater_than", "start_date": "{(ref_date - timedelta(days=30)).strftime('%Y-%m-%d')} 00:00:00"}}
- "January 2024" → {{"operation": "between", "start_date": "2024-01-01 00:00:00", "end_date": "2024-01-31 23:59:59"}}
- "older than 6 months" → {{"operation": "less_than", "end_date": "{(ref_date - timedelta(days=180)).strftime('%Y-%m-%d')} 23:59:59"}}

Respond with JSON only, no explanations.
"""

            # Get LLM response
            response = await self.llm_service.chat_completion([
                {"role": "user", "content": prompt}
            ])
            
            if not response or not response.get("choices"):
                return self._create_error_response("LLM service unavailable")
            
            content = response["choices"][0]["message"]["content"].strip()
            
            # Clean JSON response (remove code blocks if present)
            if content.startswith("```json"):
                content = content[7:]
            if content.endswith("```"):
                content = content[:-3]
            content = content.strip()
            
            # Parse JSON response
            try:
                parsed_filter = json.loads(content)
                
                if not parsed_filter.get("success"):
                    return self._create_error_response("LLM parsing failed")
                
                # Validate and enhance the filter
                enhanced_filter = await self._enhance_filter(parsed_filter, user_input)
                
                return enhanced_filter
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e}, Content: {content}")
                return self._create_error_response(f"Invalid JSON response: {content[:100]}...")
                
        except Exception as e:
            logger.error(f"Error parsing date filter '{user_input}': {e}")
            return self._create_error_response(f"Parsing error: {str(e)}")
    
    async def _enhance_filter(self, parsed_filter: Dict[str, Any], original_input: str) -> Dict[str, Any]:
        """
        Enhance and validate the parsed filter
        """
        try:
            # Ensure required fields
            enhanced = {
                "success": True,
                "original_input": original_input,
                "filter_type": parsed_filter.get("filter_type", "date_range"),
                "operation": parsed_filter.get("operation", "between"),
                "description": parsed_filter.get("description", original_input),
                "confidence": parsed_filter.get("confidence", 0.8),
                "assumptions": parsed_filter.get("assumptions", []),
                "timestamp": datetime.now().isoformat()
            }
            
            # Parse dates
            if "start_date" in parsed_filter:
                enhanced["start_date"] = self._parse_datetime(parsed_filter["start_date"])
            
            if "end_date" in parsed_filter:
                enhanced["end_date"] = self._parse_datetime(parsed_filter["end_date"])
            
            # Generate SQL condition if not provided
            if "sql_condition" not in parsed_filter:
                enhanced["sql_condition"] = self._generate_sql_condition(enhanced)
            else:
                enhanced["sql_condition"] = parsed_filter["sql_condition"]
            
            # Generate different database formats
            enhanced["formats"] = {
                "activities_transactions": self._format_for_activities_transactions(enhanced),
                "job_logs": self._format_for_job_logs(enhanced),
                "generic_datetime": self._format_for_generic_datetime(enhanced),
                "date_only": self._format_for_date_only(enhanced)
            }
            
            return enhanced
            
        except Exception as e:
            logger.error(f"Error enhancing filter: {e}")
            return self._create_error_response(f"Enhancement error: {str(e)}")
    
    def _parse_datetime(self, date_str: str) -> datetime:
        """Parse datetime string into datetime object"""
        try:
            # Try different formats
            formats = [
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
                "%Y-%m-%dT%H:%M:%S",
                "%m/%d/%Y %H:%M:%S",
                "%m/%d/%Y"
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
            
            # Fallback: try to parse with dateutil if available
            try:
                from dateutil import parser as dateutil_parser
                return dateutil_parser.parse(date_str)
            except ImportError:
                pass
            
            raise ValueError(f"Could not parse date: {date_str}")
            
        except Exception as e:
            logger.error(f"Date parsing error: {e}")
            raise
    
    def _generate_sql_condition(self, enhanced_filter: Dict[str, Any]) -> str:
        """Generate SQL WHERE clause condition"""
        try:
            operation = enhanced_filter.get("operation", "between")
            
            if operation == "between" and "start_date" in enhanced_filter and "end_date" in enhanced_filter:
                start = enhanced_filter["start_date"].strftime("%Y-%m-%d %H:%M:%S")
                end = enhanced_filter["end_date"].strftime("%Y-%m-%d %H:%M:%S")
                return f"date_field BETWEEN '{start}' AND '{end}'"
            
            elif operation == "greater_than" and "start_date" in enhanced_filter:
                start = enhanced_filter["start_date"].strftime("%Y-%m-%d %H:%M:%S")
                return f"date_field >= '{start}'"
            
            elif operation == "less_than" and "end_date" in enhanced_filter:
                end = enhanced_filter["end_date"].strftime("%Y-%m-%d %H:%M:%S")
                return f"date_field <= '{end}'"
            
            elif operation == "equals" and "start_date" in enhanced_filter:
                target = enhanced_filter["start_date"].strftime("%Y-%m-%d")
                return f"DATE(date_field) = '{target}'"
            
            return "1=1"  # No filter
            
        except Exception as e:
            logger.error(f"SQL generation error: {e}")
            return "1=1"
    
    def _format_for_activities_transactions(self, enhanced_filter: Dict[str, Any]) -> Dict[str, Any]:
        """Format for DSI Activities/Transactions tables (YYYYMMDDHHMMSS format)"""
        try:
            operation = enhanced_filter.get("operation", "between")
            result = {"operation": operation}
            
            if "start_date" in enhanced_filter:
                result["start_date"] = enhanced_filter["start_date"].strftime("%Y%m%d%H%M%S")
            
            if "end_date" in enhanced_filter:
                result["end_date"] = enhanced_filter["end_date"].strftime("%Y%m%d%H%M%S")
            
            return result
            
        except Exception as e:
            logger.error(f"Activities/Transactions format error: {e}")
            return {"operation": "between"}
    
    def _format_for_job_logs(self, enhanced_filter: Dict[str, Any]) -> Dict[str, Any]:
        """Format for Job Logs table (ISO datetime format)"""
        try:
            operation = enhanced_filter.get("operation", "between")
            result = {"operation": operation}
            
            if "start_date" in enhanced_filter:
                result["start_date"] = enhanced_filter["start_date"].isoformat()
            
            if "end_date" in enhanced_filter:
                result["end_date"] = enhanced_filter["end_date"].isoformat()
            
            return result
            
        except Exception as e:
            logger.error(f"Job Logs format error: {e}")
            return {"operation": "between"}
    
    def _format_for_generic_datetime(self, enhanced_filter: Dict[str, Any]) -> Dict[str, Any]:
        """Format for generic datetime columns"""
        try:
            operation = enhanced_filter.get("operation", "between")
            result = {"operation": operation}
            
            if "start_date" in enhanced_filter:
                result["start_date"] = enhanced_filter["start_date"].strftime("%Y-%m-%d %H:%M:%S")
            
            if "end_date" in enhanced_filter:
                result["end_date"] = enhanced_filter["end_date"].strftime("%Y-%m-%d %H:%M:%S")
            
            return result
            
        except Exception as e:
            logger.error(f"Generic datetime format error: {e}")
            return {"operation": "between"}
    
    def _format_for_date_only(self, enhanced_filter: Dict[str, Any]) -> Dict[str, Any]:
        """Format for date-only columns"""
        try:
            operation = enhanced_filter.get("operation", "between")
            result = {"operation": operation}
            
            if "start_date" in enhanced_filter:
                result["start_date"] = enhanced_filter["start_date"].strftime("%Y-%m-%d")
            
            if "end_date" in enhanced_filter:
                result["end_date"] = enhanced_filter["end_date"].strftime("%Y-%m-%d")
            
            return result
            
        except Exception as e:
            logger.error(f"Date only format error: {e}")
            return {"operation": "between"}
    
    def _create_error_response(self, error_message: str) -> Dict[str, Any]:
        """Create standardized error response"""
        return {
            "success": False,
            "error": error_message,
            "filter_type": "error",
            "operation": "none",
            "confidence": 0.0,
            "timestamp": datetime.now().isoformat()
        }
    
    def apply_to_query(self, query, date_filter: Dict[str, Any], date_field: str, table_type: str = "generic") -> Any:
        """
        Apply date filter to a SQLAlchemy query
        
        Args:
            query: SQLAlchemy query object
            date_filter: Parsed date filter from parse_date_filter()
            date_field: Name of the date field in the table
            table_type: Type of table (activities, transactions, job_logs, generic)
            
        Returns:
            Modified query with date filter applied
        """
        try:
            if not date_filter.get("success"):
                logger.warning("Cannot apply failed date filter")
                return query
            
            operation = date_filter.get("operation", "between")
            
            # Get appropriate format based on table type
            format_key = {
                "activities": "activities_transactions",
                "transactions": "activities_transactions", 
                "job_logs": "job_logs",
                "generic": "generic_datetime"
            }.get(table_type, "generic_datetime")
            
            formatted_filter = date_filter["formats"][format_key]
            
            if operation == "between":
                if "start_date" in formatted_filter and "end_date" in formatted_filter:
                    query = query.filter(
                        getattr(query.column_descriptions[0]['type'], date_field).between(
                            formatted_filter["start_date"],
                            formatted_filter["end_date"]
                        )
                    )
            
            elif operation == "greater_than":
                if "start_date" in formatted_filter:
                    query = query.filter(
                        getattr(query.column_descriptions[0]['type'], date_field) >= formatted_filter["start_date"]
                    )
            
            elif operation == "less_than":
                if "end_date" in formatted_filter:
                    query = query.filter(
                        getattr(query.column_descriptions[0]['type'], date_field) <= formatted_filter["end_date"]
                    )
            
            elif operation == "equals":
                if "start_date" in formatted_filter:
                    # For date equality, check the whole day
                    from sqlalchemy import func, Date
                    query = query.filter(
                        func.date(getattr(query.column_descriptions[0]['type'], date_field)) == formatted_filter["start_date"].split()[0]
                    )
            
            logger.info(f"Applied date filter: {date_filter['description']}")
            return query
            
        except Exception as e:
            logger.error(f"Error applying date filter to query: {e}")
            return query  # Return original query if filtering fails
    
    def get_filter_summary(self, date_filter: Dict[str, Any]) -> str:
        """
        Get human-readable summary of applied filter
        """
        if not date_filter.get("success"):
            return "No date filter applied (parsing failed)"
        
        description = date_filter.get("description", "Date filter")
        confidence = date_filter.get("confidence", 0.0)
        assumptions = date_filter.get("assumptions", [])
        
        summary = f"Filter: {description} (confidence: {confidence:.1%})"
        
        if assumptions:
            summary += f"\nAssumptions: {', '.join(assumptions)}"
        
        return summary

# Global instance
llm_date_filter = LLMDateFilter()