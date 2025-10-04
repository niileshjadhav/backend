"""Enhanced chat service with full MCP integration and role-based operations - Cache Removed"""
from sqlalchemy.orm import Session
from schemas import ChatResponse
from models import ChatOpsLog
import re
from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any
from .llm_service import OpenAIService
from .auth_service import AuthService
from schemas import ParsedOperation
from .crud_service import CRUDService
from .region_service import get_region_service

logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self):
        self.llm_service = OpenAIService()
        self.auth_service = AuthService()
        # Initialize CRUD service later with database session
        
    async def process_chat(
        self, 
        user_message: str, 
        db: Session, 
        user_token: str = None,
        session_id: str = None,
        user_id: str = None,
        region: str = None
    ) -> ChatResponse:
        """Process chat with hybrid routing, region validation, and role-based operations"""
        try:
            # Authenticate user if token provided
            user_info = None
            if user_token:
                try:
                    user_info = self.auth_service.get_user_from_token(user_token)
                    # Override user_id from token if available
                    if not user_id and user_info:
                        user_id = user_info.get("username", "unknown")
                except Exception as e:
                    logger.warning(f"Token validation failed: {e}")
            
            # Use fallback values if not provided
            final_user_id = user_id or "anonymous"
            final_session_id = session_id or f"session_{datetime.now().timestamp()}"
            user_role = user_info.get("role", "Monitor") if user_info else "Monitor"
            
            # REGION VALIDATION - Critical requirement
            region_service = get_region_service()
            if not region:
                region = region_service.get_default_region()
            elif not region_service.is_region_valid(region):
                logger.error(f"Invalid region: {region}")
                error_message = f"Invalid Region\n\nRegion '{region}' is not valid. Available regions: {', '.join(region_service.get_valid_regions())}"
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(error_message, "UNKNOWN")
                )
            
            # Ensure region is set on the service
            region_service.set_current_region(region)

            # Only log operational commands, not conversational messages
            should_log = self._should_log_operation(user_message)
            chat_log = None
            
            if should_log:
                # Create and save chat log for operational commands only
                chat_log = ChatOpsLog(
                    session_id=final_session_id,
                    user_message=user_message,
                    region=region,
                    user_id=final_user_id,
                    user_role=user_role,
                    message_type="query",
                    operation_status="processing"
                )
                db.add(chat_log)
                db.commit()
                db.refresh(chat_log)
            
            # Step 0: Handle confirmations for archive/delete operations (security critical)
            if self._is_confirmation_message(user_message):
                # For confirmations, ensure we have a chat_log
                if not chat_log:
                    chat_log = ChatOpsLog(
                        session_id=final_session_id,
                        user_message=user_message,
                        region=region,
                        user_id=final_user_id,
                        user_role=user_role,
                        message_type="command",
                        operation_status="processing"
                    )
                    db.add(chat_log)
                    db.commit()
                    db.refresh(chat_log)
                
                return await self._handle_operation_confirmation(
                    user_message, user_info, db, chat_log, region
                )
            
            # Step 0.5: Handle general table statistics requests directly (bypass LLM for reliability)
            if self._is_general_stats_request(user_message):
                # General stats requests are not logged as they're lightweight operations
                return await self._handle_general_stats_request(user_info, db, region)
            
            # Step 0.6: Handle region status requests directly (bypass LLM for reliability)
            if self._is_region_status_request(user_message):
                # Region status requests are not logged as they're lightweight operations
                return await self._handle_region_status_request(user_info, db, region)
            
            # Step 0.7: Handle greeting messages directly (bypass LLM to avoid clarification)
            if self._is_greeting_message(user_message):
                # Greeting messages are not logged as they're conversational
                user_id = user_info.get("username", "anonymous") if user_info else "anonymous"
                user_role = user_info.get("role", "Monitor") if user_info else "Monitor"
                return self._create_welcome_response(user_id, user_role, region)
            
            # Step 1: Let LLM decide everything in one intelligent call
            conversation_history = self._get_conversation_history(final_session_id, db)
            
            try:
                llm_result = await self.llm_service.parse_with_enhanced_tools(
                    user_message=user_message, 
                    conversation_context=conversation_history
                )
                
                if llm_result:
                    # Check if this is a clarification request first (highest priority)
                    if hasattr(llm_result, 'is_clarification_request') and llm_result.is_clarification_request:
                        # Handle clarification request - no need for chat_log since it's just a clarification
                        return await self._handle_llm_clarification_response(llm_result, region)
                    
                    # Check if this is a valid MCP operation (must have mcp_result AND tool_used)
                    if llm_result.mcp_result and getattr(llm_result, 'tool_used', None):
                        # For database operations, ensure we have a chat_log
                        if not chat_log:
                            chat_log = ChatOpsLog(
                                session_id=final_session_id,
                                user_message=user_message,
                                region=region,
                                user_id=final_user_id,
                                user_role=user_role,
                                message_type="query",
                                operation_status="processing"
                            )
                            db.add(chat_log)
                            db.commit()
                            db.refresh(chat_log)
                        
                        # CRITICAL : Store table name, operation type, and filters so confirmation process can find it later
                        if chat_log:
                            chat_log.operation_type = getattr(llm_result, 'tool_used', None)
                            if chat_log.operation_type:
                                chat_log.operation_type = chat_log.operation_type.upper()
                            chat_log.table_name = getattr(llm_result, 'table_used', None)
                            filters = getattr(llm_result, 'filters', None)
                            # Ensure filters is properly serializable as JSON
                            if filters is not None and isinstance(filters, dict):
                                chat_log.filters_applied = filters
                            else:
                                chat_log.filters_applied = {} if filters is None else filters
                            db.commit()
                        
                        # Format the response
                        formatted_response = self._format_response_by_tool(llm_result, region, final_session_id)
                        
                        # CRITICAL : Update ChatOpsLog with the formatted bot response so confirmation can find preview operations
                        if chat_log and formatted_response:
                            chat_log.bot_response = formatted_response.response
                            db.commit()
                        
                        return formatted_response
                    else:
                        # LLM result exists but no valid MCP operation - fall back to conversational
                        return await self._handle_conversational(
                            user_message, user_info, db, chat_log, region, final_session_id
                        )
                else:
                    # No LLM result - conversational response
                    return await self._handle_conversational(
                        user_message, user_info, db, chat_log, region, final_session_id
                    )
            except Exception as e:
                logger.error(f"LLM processing failed: {e}")
                # Fallback to conversational
                return await self._handle_conversational(
                    user_message, user_info, db, chat_log, region, final_session_id
                )
                
        except Exception as e:
            logger.error(f"Error in process_chat: {e}")
            error_message = f"System Error: {str(e)}\n\nThere is an issue in processing chat."
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region if 'region' in locals() else "UNKNOWN")
            )

    async def _handle_llm_clarification_response(
        self, 
        llm_result, 
        region: str
    ) -> ChatResponse:
        """Handle clarification requests from LLM parsing"""
        try:
            clarification_message = llm_result.clarification_message
            
            # Create structured content for clarification
            structured_content = {
                "type": "clarification_card",
                "title": "Need More Information",
                "icon": "",
                "region": region.upper(),
                "message": clarification_message,
                "suggestions": [
                    "Show activities statistics",
                    "Show transactions statistics", 
                    "Archive activities older than 7 days",
                    "Delete archived records older than 30 days"
                ],
                "context": {
                    "response_type": "clarification",
                    "timestamp": datetime.now().isoformat()
                }
            }
            
            return ChatResponse(
                response=clarification_message,
                response_type="clarification",
                structured_content=structured_content,
                suggestions=[
                    "Show activities statistics",
                    "Show transactions statistics", 
                    "Archive activities older than 7 days",
                    "Delete archived records older than 30 days"
                ]
            )
            
        except Exception as e:
            logger.error(f"Error handling LLM clarification response: {e}")
            # Fallback to error response
            error_message = "I'm having trouble understanding your request. Could you please rephrase it?"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(error_message, region)
            )

    async def _handle_conversational(
        self, 
        user_message: str, 
        user_info: dict, 
        db: Session, 
        chat_log: ChatOpsLog,
        region: str,
        session_id: str = None
    ) -> ChatResponse:
        """Handle conversational messages using LLM without database operations"""
        try:
            # Use provided session_id or get from chat_log
            current_session_id = session_id or (chat_log.session_id if chat_log else f"session_{datetime.now().timestamp()}")
            conversation_history = self._get_conversation_history(current_session_id, db)
            
            user_id = user_info.get("username", "anonymous") if user_info else "anonymous"
            user_role = user_info.get("role", "Monitor") if user_info else "Monitor"
            
            # Check if this is a greeting/welcome message
            if self._is_greeting_message(user_message):
                return self._create_welcome_response(user_id, user_role, region)
            
            # Generate conversational response using OpenAI
            llm_response = await self.llm_service.generate_response(
                user_message=user_message,
                user_id=user_id,
                conversation_context=conversation_history
            )
            
            response_text = llm_response.get("response", "I'm here to help with your Cloud Inventory log management questions!")
            suggestions = ["Show table statistics", "Explain safety rules"]

            # Create structured content for conversational responses
            structured_content = self._create_conversational_structured_content(
                response_text, user_role, region, suggestions
            )
            
            # Only update chat log if it exists (for operational messages)
            if chat_log:
                chat_log.bot_response = response_text
                chat_log.operation_status = "conversational"
                db.commit()
            
            return ChatResponse(
                response=response_text,
                suggestions=suggestions,
                response_type="conversational",
                structured_content=structured_content
            )
            
        except Exception as e:
            logger.error(f"Conversational handling error: {e}")
            error_structured_content = self._create_error_structured_content(
                "I'm having trouble responding right now. How can I help you with your log management needs?",
                region
            )
            return ChatResponse(
                response="I'm having trouble responding right now. How can I help you with your log management needs?",
                response_type="error",
                structured_content=error_structured_content
            )

    def _format_response_by_tool(self, llm_result, region: str, session_id: str = None) -> ChatResponse:
        """Format response based on the MCP tool used by LLM"""
        try:
            mcp_result = llm_result.mcp_result
            tool_used = llm_result.tool_used
            table_used = llm_result.table_used
            
            # Safety check: if tool_used is None or empty, this should not happen with our new logic
            if not tool_used:
                logger.error(f"_format_response_by_tool called with None/empty tool_used. This indicates an issue in the calling logic.")
                error_message = "Processing Error\n\nThere was an issue processing your request. Please try rephrasing it or contact support."
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        "Processing error: empty tool name",
                        region
                    )
                )
            
            # Handle special case for general stats requests (all tables)
            if tool_used == "get_table_stats" and not table_used:
                # This is a general database statistics request
                return self._format_general_stats_response(mcp_result, region)
            
            # Format response based on tool used
            if tool_used == "get_table_stats":
                return self._format_stats_response(mcp_result, table_used, region)
                
            elif tool_used == "archive_records":
                return self._format_archive_response(mcp_result, table_used, region, session_id)
                
            elif tool_used == "delete_archived_records":
                return self._format_delete_response(mcp_result, table_used, region, session_id)
                
            elif tool_used == "health_check":
                return self._format_health_response(mcp_result, region)
                
            elif tool_used == "region_status":
                return self._format_region_status_response(mcp_result, region)
                
            elif tool_used == "query_job_logs":
                return self._format_job_logs_response(mcp_result, region)
                
            elif tool_used == "get_job_summary_stats":
                return self._format_job_summary_response(mcp_result, region)
                
            else:
                # Unknown or null tool - this should not happen with our new logic
                if tool_used is None:
                    logger.error(f"_format_response_by_tool received None tool_used - this indicates a logic error in the calling code")
                    error_message = f"Processing Error\n\nThere was an issue processing your request. Please try rephrasing it."
                else:
                    logger.warning(f"Unknown MCP tool: {tool_used}")
                    error_message = f"Unknown Operation\n\nThe system tried to use an unknown operation: {tool_used}. Please try rephrasing your request."
                
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        f"Unknown operation: {tool_used or 'None'}",
                        region
                    )
                )
                
        except Exception as e:
            logger.error(f"Response formatting error: {e}")
            error_message = f"Processing Error: {str(e)}\n\nPlease try rephrasing your request."
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region)
            )

    def _is_confirmation_message(self, message: str) -> bool:
        """Check if message is a confirmation for archive/delete operations"""
        message_upper = message.upper().strip()
        confirmation_patterns = [
            'CONFIRM ARCHIVE', 'CONFIRM DELETE', 'YES', 'PROCEED', 'SURE', 'GO AHEAD', 'EXECUTE',
            'CANCEL', 'ABORT', 'DONT', 'NO'
        ]
        return any(pattern in message_upper for pattern in confirmation_patterns)

    def _is_general_stats_request(self, message: str) -> bool:
        """Check if message is asking for general table statistics"""
        message_lower = message.lower().strip()
        general_stats_patterns = [
            'show table statistics', 'table statistics', 'database statistics',
            'show database stats', 'show table stats', 'database stats', 'DB stats',
            'show all table stats', 'show stats for all tables', 'table summary',
            'database summary', 'show all tables', 'list all tables'
        ]
        return any(pattern in message_lower for pattern in general_stats_patterns)

    def _is_region_status_request(self, message: str) -> bool:
        """Check if message is asking for region connection status"""
        message_lower = message.lower().strip()
        region_patterns = [
            'which region', 'current region', 'region status', 'region connection',
            'connected region', 'what region', 'region info', 'show region',
            'region information', 'connection status', 'which region is connected',
            'what region is connected', 'current region status', 'region details'
        ]
        return any(pattern in message_lower for pattern in region_patterns)

    def _is_greeting_message(self, message: str) -> bool:
        """Check if message is a greeting/initialization message"""
        message_lower = message.lower().strip()
        
        # First check for exact simple greetings
        simple_greetings = ['hello', 'hi', 'hey', 'yo', 'greetings', 'howdy']
        if message_lower in simple_greetings:
            return True
        
        # Check for greeting phrases that should be detected (more specific matching)
        greeting_phrases = [
            'good morning', 'good afternoon', 'good evening',
            'logged in as', 'i\'m logged in', 'working with region',
            'selected but not connected'
        ]
        
        for phrase in greeting_phrases:
            if phrase in message_lower:
                return True
        
        # Check for messages that start with greetings
        greeting_starters = ['hello ', 'hi ', 'hey ', 'greetings ']
        for starter in greeting_starters:
            if message_lower.startswith(starter):
                return True
        
        # Check for role-related initialization messages (more specific)
        if message_lower.endswith(' role.') or 'role.' in message_lower:
            return True
        
        return False

    def _should_log_operation(self, message: str) -> bool:
        """Determine if this message should be logged in chatops_log table"""
        message_lower = message.lower().strip()
        
        # Always log operational commands (archive, delete, confirm operations)
        operational_keywords = [
            'archive', 'delete', 'confirm archive', 'confirm delete', 
            'remove', 'purge', 'clean', 'cancel', 'abort'
        ]
        
        # Log complex queries but not simple conversational messages
        query_keywords = [
            'find', 'search', 'query', 'filter', 'count', 'select',
            'where', 'older than', 'newer than', 'between', 'records'
        ]
        
        # Don't log simple conversational messages
        conversational_patterns = [
            'hello', 'hi', 'help', 
            'how are you', 'thanks', 'thank you', 'goodbye', 'bye',
            'what is', 'explain', 'tell me about', 'how does'
        ]
        
        # Don't log simple stats requests (these are lightweight operations)
        simple_stats_patterns = [
            'show stats', 'table stats', 'statistics', 'show table statistics',
            'database stats', 'table summary', 'show all tables'
        ]
        
        # Check if it's a simple conversational message
        if any(pattern in message_lower for pattern in conversational_patterns):
            return False
            
        # Check if it's a simple stats request
        if any(pattern in message_lower for pattern in simple_stats_patterns):
            return False
            
        # Log if it contains operational keywords
        if any(keyword in message_lower for keyword in operational_keywords):
            return True
            
        # Log if it contains complex query keywords
        if any(keyword in message_lower for keyword in query_keywords):
            return True
            
        # Default: don't log (conversational)
        return False

    async def _handle_operation_confirmation(
        self, 
        user_message: str, 
        user_info: dict, 
        db: Session, 
        chat_log: ChatOpsLog,
        region: str
    ) -> ChatResponse:
        """Handle confirmation of archive/delete operations using conversation memory"""
        try:
            # Check if user has permission for operations
            if not user_info or user_info.get("role") != "Admin":
                error_message = "Access Denied\n\nArchive and delete operations require Admin privileges."
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        "Archive and delete operations require Admin privileges.",
                        region
                    )
                )
            
            # Get conversation history to understand what operation is being confirmed
            conversation_history = self._get_conversation_history(chat_log.session_id, db, limit=3)
            
            message_upper = user_message.upper()
            
            # Check for cancellation first
            if "CANCEL" in message_upper or "ABORT" in message_upper or "NO" in message_upper:
                # Try to extract table and operation information from recent operations
                recent_logs = db.query(ChatOpsLog).filter(
                    ChatOpsLog.session_id == chat_log.session_id
                ).order_by(ChatOpsLog.timestamp.desc()).limit(5).all()
                
                # Find the most recent preview operation to get table information
                preview_operation = None
                operation_type = "Operation"
                table_name = None
                
                for log in recent_logs:
                    if log.bot_response and ("Archive Preview" in log.bot_response or "Delete Preview" in log.bot_response):
                        preview_operation = log
                        if "Archive Preview" in log.bot_response:
                            operation_type = "Archive Operation"
                        elif "Delete Preview" in log.bot_response:
                            operation_type = "Delete Operation"
                        
                        # Extract table name from stored table_name or from response
                        table_name = preview_operation.table_name
                        if not table_name and preview_operation.bot_response:
                            # Try to extract from response text
                            import re
                            table_match = re.search(r'From Table: (\w+)', preview_operation.bot_response)
                            if table_match:
                                table_name = table_match.group(1)
                        break
                
                # Build response with table information if available
                if table_name:
                    response = f"{operation_type} Cancelled\n\nThe {operation_type.lower()} for table '{table_name}' has been cancelled and will not proceed.\nNo changes have been made to the database."
                    details = [
                        f"Cancelled: {operation_type} for {table_name}",
                        "No changes have been made to the database"
                    ]
                else:
                    response = "Operation Cancelled\n\nThe operation has been cancelled and will not proceed.\nNo changes have been made to the database."
                    details = ["No changes have been made to the database"]
                
                structured_content = {
                    "type": "cancelled_card",
                    "title": f"{operation_type} Cancelled",
                    "icon": "",
                    "region": region.upper(),
                    "table": table_name,  # Add table name for frontend display
                    "message": "The operation has been cancelled and will not proceed.",
                    "details": details,
                    "context": {
                        "response_type": "cancelled",
                        "operation_type": operation_type,
                        "table_name": table_name,
                        "timestamp": datetime.now().isoformat()
                    }
                }
                
                chat_log.bot_response = response
                chat_log.operation_status = "cancelled"
                db.commit()
                
                return ChatResponse(
                    response=response,
                    response_type="cancelled",
                    structured_content=structured_content,
                    context={"cancelled": True, "table": table_name, "operation_type": operation_type}
                )
            
            # Use LLM with conversation context to understand and execute the confirmation
            if "CONFIRM ARCHIVE" in message_upper or "CONFIRM DELETE" in message_upper:
                # Get the most recent operation from conversation history to extract details
                recent_logs = db.query(ChatOpsLog).filter(
                    ChatOpsLog.session_id == chat_log.session_id
                ).order_by(ChatOpsLog.timestamp.desc()).limit(5).all()
                
                # Find the most recent preview operation
                preview_operation = None
                for log in recent_logs:
                    if log.bot_response and ("Archive Preview" in log.bot_response or "Delete Preview" in log.bot_response):
                        preview_operation = log
                        break
                
                if not preview_operation:
                    # Try to find any archive/delete related message in recent history
                    for log in recent_logs:
                        if log.user_message and any(keyword in log.user_message.lower() for keyword in ['archive', 'delete']):
                            preview_operation = log
                            break
                
                if preview_operation:
                    # Direct execution based on stored operation details
                    llm_result = await self._execute_stored_confirmation(
                        message_upper, preview_operation, conversation_history
                    )
                else:
                    # Last resort: Try to parse from conversation history using LLM
                    
                    # CRITICAL : Don't hardcode table names in fallback - this causes wrong table targeting
                    if "CONFIRM ARCHIVE" in message_upper:
                        return ChatResponse(
                            response="Archive Confirmation Failed\n\nCannot determine which table to archive. Please start a new archive operation by saying something like:\n• 'archive transactions older than 7 days'\n• 'archive activities older than 7 days'",
                            response_type="error",
                            structured_content=self._create_error_structured_content(
                                "Cannot determine target table for archive operation. Please start a new operation.",
                                region
                            )
                        )
                    elif "CONFIRM DELETE" in message_upper:
                        return ChatResponse(
                            response="Delete Confirmation Failed\n\nCannot determine which archived table to delete from. Please start a new delete operation by saying something like:\n• 'delete archived transactions older than 30 days'\n• 'delete archived activities older than 30 days'",
                            response_type="error",
                            structured_content=self._create_error_structured_content(
                                "Cannot determine target table for delete operation. Please start a new operation.",
                                region
                            )
                        )
                    else:
                        confirmation_prompt = f"The user is confirming an operation: {user_message}"
                    
                    # Use enhanced LLM parsing with conversation context
                    llm_result = await self.llm_service.parse_with_enhanced_tools(
                        user_message=confirmation_prompt, 
                        conversation_context=conversation_history
                    )
                
                if llm_result and llm_result.mcp_result:
                    # Format the response based on the operation type
                    mcp_result = llm_result.mcp_result
                    
                    if llm_result.tool_used == "archive_records":
                        if mcp_result.get("success"):
                            archived_count = mcp_result.get("archived_count", 0)
                            table_name = llm_result.table_used
                            user_id = user_info.get("username", "admin")
                            
                            response = f"Archive Operation Completed - {region.upper()} Region\n\n"
                            response += f"Successfully archived {archived_count:,} records\n"
                            response += f"From: {table_name}\n"
                            response += f"To: {self._get_archive_table_name(table_name)}\n"
                            response += f"Executed by: {user_id}\n\n"
                            response += "Records have been moved from the main table to the archive table."
                            
                            structured_content = {
                                "type": "success_card",
                                "title": "Archive Completed",
                                "region": region.upper(),
                                "details": [
                                    f"Successfully archived {archived_count:,} records",
                                    f"From: {table_name}",
                                    f"To: {self._get_archive_table_name(table_name)}",
                                    f"Executed by: {user_id}"
                                ]
                            }
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "archive_completed"
                            chat_log.records_affected = archived_count
                            chat_log.filters_applied = getattr(llm_result, 'filters', None)
                            db.commit()
                            
                            return ChatResponse(
                                response=response,
                                response_type="archive_completed",
                                structured_content=structured_content,
                                context={
                                    "operation": "archive",
                                    "archived_count": archived_count,
                                    "table": table_name,
                                    "confirmed": True
                                }
                            )
                        else:
                            error_msg = mcp_result.get("error", "Archive operation failed")
                            response = f"Archive Operation Failed\n\n{error_msg}"
                            
                            structured_content = self._create_error_structured_content(error_msg, region)
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "archive_failed"
                            chat_log.filters_applied = getattr(llm_result, 'filters', None)
                            db.commit()
                            
                            return ChatResponse(
                                response=response,
                                response_type="error",
                                structured_content=structured_content
                            )
                            
                    elif llm_result.tool_used == "delete_archived_records":
                        if mcp_result.get("success"):
                            deleted_count = mcp_result.get("deleted_count", 0)
                            table_name = llm_result.table_used
                            user_id = user_info.get("username", "admin")
                            
                            response = f"Delete Operation Completed - {region.upper()} Region\n\n"
                            response += f"Successfully deleted {deleted_count:,} records\n"
                            response += f"From: {table_name}\n"
                            response += f"Executed by: {user_id}\n\n"
                            response += "Records have been permanently removed from the archive table."
                            
                            structured_content = {
                                "type": "success_card",
                                "title": "Delete Completed",
                                "region": region.upper(),
                                "details": [
                                    f"Successfully deleted {deleted_count:,} records",
                                    f"From: {table_name}",
                                    f"Executed by: {user_id}",
                                    "Records have been permanently removed"
                                ]
                            }
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "delete_completed"
                            chat_log.records_affected = deleted_count
                            chat_log.filters_applied = getattr(llm_result, 'filters', None)
                            db.commit()
                            
                            return ChatResponse(
                                response=response,
                                response_type="delete_completed",
                                structured_content=structured_content,
                                context={
                                    "operation": "delete",
                                    "deleted_count": deleted_count,
                                    "table": table_name,
                                    "confirmed": True
                                }
                            )
                        else:
                            error_msg = mcp_result.get("error", "Delete operation failed")
                            response = f"Delete Operation Failed\n\n{error_msg}"
                            
                            structured_content = self._create_error_structured_content(error_msg, region)
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "delete_failed"
                            chat_log.filters_applied = getattr(llm_result, 'filters', None)
                            db.commit()
                            
                            return ChatResponse(
                                response=response,
                                response_type="error",
                                structured_content=structured_content
                            )
                else:
                    # LLM failed to process the confirmation - use direct fallback
                    logger.error(f"Confirmation processing failed: llm_result={llm_result}, conversation_history length={len(conversation_history)}")
                    
                    # Direct fallback execution without LLM
                    try:
                        fallback_result = await self._execute_direct_confirmation_fallback(
                            message_upper, user_info, region
                        )
                        
                        if fallback_result:
                            return fallback_result
                        
                    except Exception as fallback_error:
                        logger.error(f"Direct confirmation fallback also failed: {fallback_error}")
                    
                    # If everything fails, return error
                    error_message = "Confirmation Processing Failed\n\nThe system failed to process your confirmation. Please try again.\n\nTip: Try saying 'archive activities' or 'delete archived activities' to start a new operation."
                    return ChatResponse(
                        response=error_message,
                        response_type="error",
                        structured_content=self._create_error_structured_content(
                            "The system failed to process your confirmation. Please try again.",
                            region
                        )
                    )
            
            # If we get here, the confirmation was not understood
            error_message = "Invalid Confirmation\n\nPlease type 'CONFIRM ARCHIVE', 'CONFIRM DELETE', or 'CANCEL' to proceed."
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(
                    "Invalid confirmation command. Please type 'CONFIRM ARCHIVE', 'CONFIRM DELETE', or 'CANCEL' to proceed.",
                    region
                )
            )
            
        except Exception as e:
            logger.error(f"Confirmation handling error: {e}")
            error_message = f"Error processing confirmation: {str(e)}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region)
            )

    async def _execute_stored_confirmation(
        self, 
        message_upper: str, 
        preview_operation: ChatOpsLog, 
        conversation_history: str
    ) -> Any:
        """Execute confirmation based on stored preview operation details"""
        try:
            from cloud_mcp.server import archive_records, delete_archived_records
            
            # Extract operation details from the preview operation user message
            # This is more reliable than parsing LLM responses
            user_message = preview_operation.user_message.lower()
            
            # Determine operation type and table
            if "CONFIRM ARCHIVE" in message_upper:
                # CRITICAL FIX: Use the stored table name from the preview operation
                table_name = preview_operation.table_name
                
                # Fallback logic if table_name is not stored (backward compatibility)
                if not table_name:
                    if "activities" in user_message or "activity" in user_message:
                        table_name = "dsiactivities"
                    elif "transaction" in user_message:
                        table_name = "dsitransactionlog"
                    else:
                        # Try to extract from bot response if available
                        if preview_operation.bot_response and "dsitransactionlog" in preview_operation.bot_response.lower():
                            table_name = "dsitransactionlog"
                        elif preview_operation.bot_response and "dsiactivities" in preview_operation.bot_response.lower():
                            table_name = "dsiactivities"
                        else:
                            # Last resort fallback
                            table_name = "dsiactivities"
                
                # Extract filters from original user message
                filters = self._extract_filters_from_message(user_message)
                filters["confirmed"] = True
                
                # Execute archive operation
                mcp_result = await archive_records(table_name, filters, "system")
                
                # Create mock LLM result structure
                class MockLLMResult:
                    def __init__(self, tool, table, filters, result):
                        self.tool_used = tool
                        self.table_used = table
                        self.filters = filters
                        self.mcp_result = result
                
                return MockLLMResult("archive_records", table_name, filters, mcp_result)
                
            elif "CONFIRM DELETE" in message_upper:
                # CRITICAL FIX: Use the stored table name from the preview operation
                table_name = preview_operation.table_name
                
                # Fallback logic if table_name is not stored (backward compatibility)
                if not table_name:
                    if "activities" in user_message or "activity" in user_message:
                        table_name = "dsiactivities"
                    elif "transaction" in user_message:
                        table_name = "dsitransactionlog"
                    else:
                        # Try to extract from bot response if available
                        if preview_operation.bot_response and "dsitransactionlog" in preview_operation.bot_response.lower():
                            table_name = "dsitransactionlog"
                        elif preview_operation.bot_response and "dsiactivities" in preview_operation.bot_response.lower():
                            table_name = "dsiactivities"
                        else:
                            # Last resort fallback
                            table_name = "dsiactivities"
                
                # Extract filters from original user message
                filters = self._extract_filters_from_message(user_message)
                filters["confirmed"] = True
                
                # Execute delete operation
                mcp_result = await delete_archived_records(table_name, filters, "system")
                
                # Create mock LLM result structure
                class MockLLMResult:
                    def __init__(self, tool, table, filters, result):
                        self.tool_used = tool
                        self.table_used = table
                        self.filters = filters
                        self.mcp_result = result
                
                return MockLLMResult("delete_archived_records", table_name, filters, mcp_result)
                
            else:
                logger.warning(f"Unknown confirmation type: {message_upper}")
                return None
            
        except Exception as e:
            logger.error(f"Error in stored confirmation execution: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    def _extract_filters_from_message(self, message: str) -> dict:
        """Extract date filters from user message"""
        filters = {}
        
        import re
        
        # Common date filter patterns
        if "older than" in message:
            # Extract "older than X days/months/years"
            match = re.search(r'older than (\d+)\s*(day|month|year)', message)
            if match:
                number = match.group(1)
                unit = match.group(2)
                # Ensure plural form
                if not unit.endswith('s'):
                    unit += 's'
                filters["date_filter"] = f"older_than_{number}_{unit}"
        
        # Handle simple archive/delete without specific dates
        elif any(keyword in message for keyword in ['archive', 'delete']) and 'older than' not in message:
            # Let the system apply default filters - don't set anything
            pass
        
        return filters

    async def _execute_direct_confirmation_fallback(
        self, 
        message_upper: str, 
        user_info: dict, 
        region: str
    ) -> ChatResponse:
        """Direct confirmation fallback when all parsing fails"""
        try:
            from cloud_mcp.server import archive_records, delete_archived_records
            
            # Use default operations with system safety filters
            if "CONFIRM ARCHIVE" in message_upper:
                # This fallback should not be used as it can't reliably determine the intended table
                return ChatResponse(
                    response="Archive Confirmation Failed\n\nCannot determine which table to archive. Please start a new archive operation by saying something like:\n• 'archive transactions older than 7 days'\n• 'archive activities older than 7 days'",
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        "Cannot determine target table for archive operation. Please start a new operation.",
                        region
                    )
                )
                    
            elif "CONFIRM DELETE" in message_upper:
                # This fallback should not be used as it can't reliably determine the intended table
                return ChatResponse(
                    response="Delete Confirmation Failed\n\nCannot determine which archived table to delete from. Please start a new delete operation by saying something like:\n• 'delete archived transactions older than 60 days'\n• 'delete archived activities older than 60 days'",
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        "Cannot determine target table for delete operation. Please start a new operation.",
                        region
                    )
                )
            
            return None
            
        except Exception as e:
            logger.error(f"Direct confirmation fallback error: {e}")
            return None

    def _get_conversation_history(self, session_id: str, db: Session, limit: int = 5) -> str:
        """Get recent conversation history for LLM context"""
        try:
            # Get recent chat logs for this session (last 5 exchanges)
            recent_logs = db.query(ChatOpsLog).filter(
                ChatOpsLog.session_id == session_id
            ).order_by(ChatOpsLog.timestamp.desc()).limit(limit * 2).all()  # *2 to get both user and bot messages
            
            if not recent_logs:
                return "No previous conversation history."
            
            # Build conversation context
            conversation = []
            recent_logs.reverse()  # Order chronologically
            
            for log in recent_logs:
                if log.user_message:
                    conversation.append(f"User: {log.user_message}")
                    # Add table context information for better LLM understanding
                    if log.table_name:
                        conversation.append(f"[Context: Previous operation on table: {log.table_name}]")
                if log.bot_response:
                    conversation.append(f"Assistant: {log.bot_response}")
            
            # Limit total context length to avoid token limits
            context = "\n".join(conversation[-10:])  # Last 10 messages (5 exchanges)
            
            if len(context) > 2000:  # Truncate if too long
                context = context[-2000:]
                context = "...[conversation truncated]...\n" + context
            
            return f"Previous conversation:\n{context}\n\nCurrent message:"
            
        except Exception as e:
            logger.error(f"Error getting conversation history: {e}")
            return "No previous conversation history."

    async def _handle_general_stats_request(self, user_info: dict, db: Session, region: str) -> ChatResponse:
        """Handle general table statistics request showing all tables"""
        try:
            from services.database_service import DatabaseService
            from services.region_service import get_region_service
            
            # Get regional database session
            region_service = get_region_service()
            
            # Ensure region is connected
            if not region_service.is_connected(region):
                connected, message = await region_service.connect_to_region(region)
                if not connected:
                    error_msg = f"Failed to connect to region {region}: {message}"
                    return ChatResponse(
                        response=f"Connection Error - {region.upper()} Region\n\n{error_msg}",
                        response_type="error",
                        structured_content=self._create_error_structured_content(error_msg, region)
                    )
            
            region_db_session = region_service.get_session(region)
            
            try:
                # Use database service to get detailed stats
                db_service = DatabaseService(region_db_session)
                stats_result = await db_service.get_detailed_table_stats()
                
                if not stats_result.get("success"):
                    error_msg = stats_result.get("error", "Failed to get statistics")
                    return ChatResponse(
                        response=f"Statistics Error - {region.upper()} Region\n\n{error_msg}",
                        response_type="error",
                        structured_content=self._create_error_structured_content(error_msg, region)
                    )
                
                # Format structured response
                return self._format_general_stats_response(stats_result, region)
                
            finally:
                region_db_session.close()
                
        except Exception as e:
            logger.error(f"Error handling general stats request: {e}")
            error_msg = f"Failed to retrieve table statistics: {str(e)}"
            return ChatResponse(
                response=f"Statistics Error - {region.upper()} Region\n\n{error_msg}",
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )

    async def _handle_region_status_request(self, user_info: dict, db: Session, region: str) -> ChatResponse:
        """Handle region connection status request directly"""
        try:
            from cloud_mcp.server import region_status
            
            # Execute region status MCP tool
            mcp_result = await region_status()
            
            # Format the response
            return self._format_region_status_response(mcp_result, region)
                
        except Exception as e:
            logger.error(f"Error handling region status request: {e}")
            error_msg = f"Failed to retrieve region status: {str(e)}"
            return ChatResponse(
                response=f"Region Status Error\n\n{error_msg}",
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )

    def _format_stats_response(self, mcp_result: dict, table_name: str, region: str) -> ChatResponse:
        """Format table statistics response with structured content"""
        if not mcp_result.get("success"):
            error_msg = mcp_result.get("error", "Unknown error")
            error_message = f"Stats Error - {region.upper()} Region\n\n{error_msg}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        # Build statistics display - MCP result returns data at root level
        filtered_count = mcp_result.get("record_count", 0)  # This is the filtered count (or total if no filter)
        filter_description = mcp_result.get("filter_description")
        filter_applied = mcp_result.get("filter_applied")
        
        # Determine if filters were applied
        has_filter = bool(filter_applied or filter_description)
        
        # Plain text response for backward compatibility
        is_activity_transaction_archive = table_name in ['dsiactivities', 'dsitransactionlog', 'dsiactivitiesarchive', 'dsitransactionlogarchive']
        
        response = f"Table Statistics - {region.upper()} Region\n\n"
        response += f"Table: {table_name}\n"
        
        if has_filter:
            # Show filtered count as primary when filters are applied
            if is_activity_transaction_archive:
                response += f"Records: **{filtered_count:,}**\n"
            else:
                response += f"Records: **{filtered_count:,}**\n"
            response += f"Filter: Records {filter_description}\n"
        else:
            # Show total count when no filters are applied
            if is_activity_transaction_archive:
                response += f"Total Records: **{filtered_count:,}**\n"
            else:
                response += f"Total Records: **{filtered_count:,}**\n"

        # Structured content for rich rendering
        structured_content = {
            "type": "stats_card",
            "title": f"Table Statistics",
            "icon": "",
            "table_name": table_name,
            "region": region.upper(),
            "stats": []
        }
        
        # Add appropriate stats based on whether filters are applied
        if has_filter:
            # Primary stat is the filtered count only
            structured_content["stats"].append({
                "label": f"{filter_description}", 
                "value": f"{filtered_count:,}", 
                "type": "number", 
                "highlight": True
            })
        else:
            # Primary stat is the total count
            structured_content["stats"].append({
                "label": "Total Records", 
                "value": f"{filtered_count:,}", 
                "type": "number", 
                "highlight": True
            })
        
        return ChatResponse(
            response=response,
            response_type="stats",
            structured_content=structured_content,
            context={"count": filtered_count, "table": table_name, "tool": "get_table_stats", "has_filter": has_filter}
        )

    def _format_query_response(self, mcp_result: dict, table_name: str, region: str) -> ChatResponse:
        """Format query results response with structured content"""
        if not mcp_result.get("success"):
            error_msg = mcp_result.get("error", "Unknown error")
            error_message = f"Query Error - {region.upper()} Region\n\n{error_msg}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        records = mcp_result.get("records", [])
        total_found = mcp_result.get("total_records", len(records))
        
        # Plain text response for backward compatibility
        response = f"Table Statistics"
        response += f"Table: {table_name}\n"
        response += f"**Total Records Found: {total_found:,}**\n\n"

        if total_found > 0:
            response += f"Found **{total_found:,}** records\n"
        else:
            response += "No matching records found.\n"
        
        # Structured content as stats card (no table data shown)
        structured_content = {
            "type": "stats_card",
            "title": f"Table Statistics",
            "icon": "",
            "table_name": table_name,
            "region": region.upper(),
            "stats": [
                {"label": "Total Records Found", "value": f"{total_found:,}", "type": "number", "highlight": True},
            ]
        }
        
        return ChatResponse(
            response=response,
            response_type="query_results",
            structured_content=structured_content,
            context={"count": total_found, "records_shown": len(records), "table": table_name}
        )

    def _format_general_stats_response(self, stats_result: dict, region: str) -> ChatResponse:
        """Format general table statistics response showing all tables"""
        detailed_stats = stats_result.get("detailed_stats", {})
        
        # Separate main and archive tables
        main_tables = []
        archive_tables = []
        
        for table_name, stats in detailed_stats.items():
            table_data = {
                "name": table_name,
                "table_name": table_name,
                "total_records": stats.get("total_count", 0),
                "age_based_count": stats.get("older_count", 0),
                "age_days": stats.get("older_than_days", 0),
                "error": stats.get("error")
            }
            
            if stats.get("type") == "main":
                main_tables.append(table_data)
            else:
                archive_tables.append(table_data)
        
        # Build plain text response
        response = f"Database Statistics - {region.upper()} Region\n\n"
        
        # Main Tables Section
        response += "Main Tables:\n"
        for table in main_tables:
            if table["error"]:
                response += f"• {table['name']}: Error - {table['error']}\n"
            else:
                response += f"• {table['name']}: **{table['total_records']:,}** total records"
                if table['age_based_count'] > 0:
                    response += f", **{table['age_based_count']:,}** records older than {table['age_days']} days\n"
                else:
                    response += "\n"
        
        response += "\n📦 Archive Tables:\n"
        for table in archive_tables:
            if table["error"]:
                response += f"• {table['name']}: Error - {table['error']}\n"
            else:
                response += f"• {table['name']}: **{table['total_records']:,}** total records"
                if table['age_based_count'] > 0:
                    response += f", **{table['age_based_count']:,}** records older than {table['age_days']} days\n"
                else:
                    response += "\n"
        
        # Build structured content
        structured_content = {
            "type": "database_overview",
            "title": f"Database Statistics",
            "region": region.upper(),
            "main_tables": main_tables,
            "archive_tables": archive_tables,
            "summary": {
                "total_main_records": sum(t["total_records"] for t in main_tables if not t["error"]),
                "total_archive_records": sum(t["total_records"] for t in archive_tables if not t["error"]),
                "main_tables_count": len([t for t in main_tables if not t["error"]]),
                "archive_tables_count": len([t for t in archive_tables if not t["error"]])
            }
        }
        
        return ChatResponse(
            response=response,
            response_type="database_overview",
            structured_content=structured_content,
            context={"region": region, "tool": "general_stats", "table_count": len(detailed_stats)}
        )

    def _format_archive_response(self, mcp_result: dict, table_name: str, region: str, session_id: str = None) -> ChatResponse:
        """Format archive operation response with confirmation if needed"""
        count = mcp_result.get('archived_count', 0)
        
        # Check if this is a preview (confirmation needed)
        if mcp_result.get('requires_confirmation', False):
            response = f"Archive Preview - {region.upper()} Region\n\n"
            response += f"Ready to Archive: **{count:,} records** \n"
            response += f"From Table: {table_name}\n"
            response += f"To Table: {self._get_archive_table_name(table_name)}\n\n"
            response += f"This will move records from main table to archive table.\n"
            
            # Add safety information about default filters if no specific date filters were provided
            if not mcp_result.get('filters', {}).get('date_filter'):
                response += "Safety Filter Applied: Only records older than 7 days will be archived.\n"

            response += "Click 'CONFIRM ARCHIVE' to proceed or 'CANCEL' to abort."
            
            # Structured content for confirmation
            structured_content = {
                "type": "confirmation_card",
                "title": "Archive Preview",
                "region": region.upper(),
                "count": count,  # Add count for frontend display
                "table": table_name,  # Add table name
                "details": [
                    f"Ready to Archive: {count:,} records",
                    f"From Table: {table_name}",
                    f"To Table: {self._get_archive_table_name(table_name)}",
                    "This will move records from main table to archive table.",
                    "Click 'CONFIRM ARCHIVE' to proceed or 'CANCEL' to abort."
                ]
            }
            
            return ChatResponse(
                response=response,
                response_type="archive_confirmation",
                structured_content=structured_content,
                requires_confirmation=True,
                operation_data={
                    "confirmation_id": f"archive_{table_name}_{count}",
                    "operation": "ARCHIVE",
                    "details": f"Ready to Archive: {count:,} records from {table_name}",
                    "count": count,
                    "table": table_name
                },
                context={"count": count, "tool": "archive_records", "table": table_name}
            )
        
        # Handle case where there are no records to archive
        if count == 0:
            response = f"Archive Result - {region.upper()} Region\n\n"
            response += f"No records found matching the criteria (Older than 7 days)\n"
            response += f"Table: {table_name}\n\n"
            response += "No archive operation was needed."
            
            # Structured content for no records
            structured_content = {
                "type": "success_card",
                "title": "Archive Result",
                "region": region.upper(),
                "details": [
                    f"Table: {table_name}",
                    "No records found matching the criteria (Older than 7 days)",
                    "No archive operation was needed"
                ]
            }
            
            return ChatResponse(
                response=response,
                response_type="archive_info",
                structured_content=structured_content,
                requires_confirmation=False,
                context={"count": 0, "tool": "archive_records", "table": table_name}
            )
        
        # This is the actual result
        if mcp_result.get("success"):
            response = f"Archive Operation Completed - {region.upper()} Region\n\n"
            response += f"Successfully archived {count:,} records\n"
            response += f"From: {table_name}\n"
            response += f"To: {self._get_archive_table_name(table_name)}\n\n"
            response += "Records have been moved from the main table to the archive table."
            
            # Structured content for success
            structured_content = {
                "type": "success_card",
                "title": "Archive Completed",
                "region": region.upper(),
                "details": [
                    f"Successfully archived {count:,} records",
                    f"From: {table_name}",
                    f"To: {self._get_archive_table_name(table_name)}"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Archive failed")
            response = f"Archive Error - {region.upper()} Region\n\n{error_msg}"
            structured_content = self._create_error_structured_content(error_msg, region)
        
        return ChatResponse(
            response=response,
            response_type="archive_result",
            structured_content=structured_content,
            context={"count": count, "tool": "archive_records", "table": table_name}
        )

    def _format_delete_response(self, mcp_result: dict, table_name: str, region: str, session_id: str = None) -> ChatResponse:
        """Format delete operation response with confirmation if needed"""
        count = mcp_result.get('deleted_count', 0)
        
        # Check if this is a preview (confirmation needed)
        if mcp_result.get('requires_confirmation', False):
            response = f"Delete Preview - {region.upper()} Region\n\n"
            response += f"Ready to Delete: **{count:,} records**\n"
            response += f"From Table: {table_name}\n\n"
            response += "WARNING: THIS WILL PERMANENTLY DELETE RECORDS\n"
            
            # Add safety information about default filters if no specific date filters were provided
            if not mcp_result.get('filters', {}).get('date_filter'):
                response += "🛡️ Safety Filter Applied: Only archived records older than 30 days will be deleted.\n"
            
            response += "\nType 'CONFIRM DELETE' to proceed or 'CANCEL' to abort."
            
            # Structured content for confirmation
            structured_content = {
                "type": "confirmation_card",
                "title": "Delete Preview",
                "region": region.upper(),
                "count": count,  # Add count for frontend display
                "table": table_name,  # Add table name
                "details": [
                    f"Ready to Delete: {count:,} records",
                    f"From Table: {table_name}",
                    "WARNING: THIS WILL PERMANENTLY DELETE RECORDS",
                    "Type 'CONFIRM DELETE' to proceed or 'CANCEL' to abort."
                ]
            }
            
            return ChatResponse(
                response=response,
                response_type="delete_confirmation",
                structured_content=structured_content,
                requires_confirmation=True,
                operation_data={
                    "confirmation_id": f"delete_{table_name}_{count}",
                    "operation": "DELETE",
                    "details": f"Ready to Delete: {count:,} records from {table_name}",
                    "count": count,
                    "table": table_name
                },
                context={"count": count, "tool": "delete_archived_records", "table": table_name}
            )
        
        # Handle case where there are no records to delete
        if count == 0:
            response = f"Delete Result - {region.upper()} Region\n\n"
            response += f"No records found matching the criteria (Older than 30 days)\n"
            response += f"Table: {table_name}\n\n"
            response += "No delete operation was needed."
            
            # Structured content for no records
            structured_content = {
                "type": "success_card",
                "title": "Delete Result",
                "region": region.upper(),
                "details": [
                    f"Table: {table_name}",
                    "No records found matching the criteria (Older than 30 days)",
                    "No delete operation was needed"
                ]
            }
            
            return ChatResponse(
                response=response,
                response_type="delete_info",
                structured_content=structured_content,
                requires_confirmation=False,
                context={"count": 0, "tool": "delete_archived_records", "table": table_name}
            )
        
        # This is the actual result
        if mcp_result.get("success"):
            response = f"Delete Operation Completed - {region.upper()} Region\n\n"
            response += f"Successfully deleted {count:,} records\n"
            response += f"From: {table_name}\n\n"
            response += "Records have been permanently removed."
            
            # Structured content for success
            structured_content = {
                "type": "success_card",
                "title": "Delete Completed",
                "region": region.upper(),
                "details": [
                    f"Successfully deleted {count:,} records",
                    f"From: {table_name}",
                    "Records have been permanently removed"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Delete failed")
            response = f"Delete Error - {region.upper()} Region\n\n{error_msg}"
            structured_content = self._create_error_structured_content(error_msg, region)
        
        return ChatResponse(
            response=response,
            response_type="delete_result",
            structured_content=structured_content,
            context={"count": count, "tool": "delete_archived_records", "table": table_name}
        )

    def _format_health_response(self, mcp_result: dict, region: str) -> ChatResponse:
        """Format health check response"""
        if mcp_result.get("success"):
            response = f"System Health Check - {region.upper()} Region\n\n"
            response += "All database connections and services are operational."
            
            structured_content = {
                "type": "success_card",
                "title": "System Health Check",
                "region": region.upper(),
                "details": [
                    "All database connections are operational",
                    "Services are running normally",
                    "System is ready for operations"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Health check failed")
            response = f"System Health Issues - {region.upper()} Region\n\n{error_msg}"
            structured_content = self._create_error_structured_content(error_msg, region)
        
        return ChatResponse(
            response=response,
            response_type="health_check",
            structured_content=structured_content,
            context={"tool": "health_check"}
        )

    def _format_region_status_response(self, mcp_result: dict, region: str) -> ChatResponse:
        """Format region status response"""
        if not mcp_result.get("success"):
            error_msg = mcp_result.get("error", "Failed to get region status")
            response = f"Region Status Error\n\n{error_msg}"
            return ChatResponse(
                response=response,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        current_region = mcp_result.get("current_region")
        default_region = mcp_result.get("default_region")
        available_regions = mcp_result.get("available_regions", [])
        connection_status = mcp_result.get("connection_status", {})
        connected_regions = mcp_result.get("connected_regions", [])
        
        # Build response with natural sentence format
        total_regions = len(available_regions)
        connected_count = len(connected_regions)
        
        response = f"Region Status Information\n\n"
        
        # Main summary sentence
        if connected_count == 0:
            response += f"There are **{total_regions} regions** available ({', '.join([r.upper() for r in available_regions])}), of which currently **none is connected**.\n\n"
        elif connected_count == 1:
            connected_region = connected_regions[0]
            response += f"There are **{total_regions} regions** available ({', '.join([r.upper() for r in available_regions])}), of which currently **{connected_region.upper()}** is connected.\n\n"
        else:
            connected_list = ', '.join([r.upper() for r in connected_regions])
            response += f"There are **{total_regions} regions** available ({', '.join([r.upper() for r in available_regions])}), of which currently **{connected_list}** are connected.\n\n"
        
        # Current region info
        if current_region:
            is_connected = connection_status.get(current_region, False)
            connection_text = "Connected" if is_connected else "Disconnected"
            response += f"**Active Region:** {current_region.upper()} ({connection_text})\n"
        else:
            response += f"**Active Region:** None (using default: {default_region.upper()})\n"
        
        # Default region
        if default_region and default_region != current_region:
            response += f"**Default Region:** {default_region.upper()}"
        
        # Structured content
        structured_content = {
            "type": "region_status_card",
            "title": "Region Status",
            "icon": "",
            "current_region": current_region,
            "default_region": default_region,
            "available_regions": available_regions,
            "connection_status": connection_status,
            "connected_regions": connected_regions,
            "summary": {
                "total_regions": total_regions,
                "connected_count": connected_count,
                "all_connected": connected_count == total_regions,
                "none_connected": connected_count == 0
            },
            "details": [
                f"Current Region: {current_region.upper() if current_region else 'None'}",
                f"Connected Regions: {connected_count}/{total_regions}",
                f"Available Regions: {', '.join([r.upper() for r in available_regions])}"
            ]
        }
        
        return ChatResponse(
            response=response,
            response_type="region_status",
            structured_content=structured_content,
            context={
                "tool": "region_status",
                "current_region": current_region,
                "connected_count": connected_count,
                "total_regions": total_regions
            }
        )

    def _format_job_logs_response(self, mcp_result: dict, region: str) -> ChatResponse:
        """Format job logs query response"""
        if not mcp_result.get("success", True):  # Structured responses don't have success field
            error_msg = mcp_result.get("error_message", "Failed to retrieve job logs")
            response = f"Job Logs Error\n\n{error_msg}"
            return ChatResponse(
                response=response,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        # For structured responses, pass them directly through
        if mcp_result.get("type"):
            # This is already a structured response, pass it through
            structured_content = mcp_result
            
            # Create a text response based on the structured content
            if mcp_result.get("type") == "job_logs_table":
                records = mcp_result.get("records", [])
                total_count = mcp_result.get("total_count", 0)
                response = f"Job Logs Table\n\nFound {len(records)} job logs"
                if total_count > len(records):
                    response += f" (showing {len(records)} of {total_count} total)"
                response += "\n\nView the detailed table below for complete information."
                
            elif mcp_result.get("type") == "conversational_card":
                content = mcp_result.get('content', 'No content available')
                title = mcp_result.get('title', 'Job Logs Results')
                
                # For reason-only responses, extract just the reason text
                if title == "Job Status" and content and '\n\nTable:' in content:
                    # Extract just the reason part (everything before the table info)
                    reason_part = content.split('\n\nTable:')[0].strip()
                    if reason_part:
                        response = reason_part
                    else:
                        response = content
                else:
                    response = f"{title}\n\n{content}"
                
            else:
                response = "Job Logs Results\n\nView the detailed information below."
            
            return ChatResponse(
                response=response,
                response_type="job_logs",
                structured_content=structured_content,
                context={
                    "tool": "query_job_logs",
                    "region": region,
                    "record_count": len(mcp_result.get("records", [])),
                    "total_count": mcp_result.get("total_count", 0)
                }
            )
        
        # Fallback for old-style responses
        response = "Job Logs Query Results\n\nNo data available."
        return ChatResponse(
            response=response,
            response_type="job_logs",
            structured_content=None
        )

    def _format_job_summary_response(self, mcp_result: dict, region: str) -> ChatResponse:
        """Format job summary statistics response"""
        if not mcp_result.get("success", True):  # Structured responses don't have success field
            error_msg = mcp_result.get("error_message", "Failed to retrieve job statistics")
            response = f"Job Statistics Error\n\n{error_msg}"
            return ChatResponse(
                response=response,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        # For structured responses, pass them directly through
        if mcp_result.get("type") == "stats_card":
            # This is already a structured response, pass it through
            structured_content = mcp_result
            
            # Create a text response based on the stats
            stats = mcp_result.get("stats", [])
            details = mcp_result.get("details", [])
            
            response = f"Job Statistics Summary\n\n"
            
            # Add key stats to text response
            for stat in stats[:4]:  # First 4 stats for text
                response += f"• {stat.get('label', 'Unknown')}: {stat.get('value', 'N/A')}\n"
            
            if details:
                response += f"\nAdditional Details:\n"
                for detail in details[:3]:  # First 3 details
                    response += f"• {detail}\n"
            
            response += f"\nView the statistics card below for complete visual breakdown."
            
            return ChatResponse(
                response=response,
                response_type="job_statistics",
                structured_content=structured_content,
                context={
                    "tool": "get_job_summary_stats",
                    "region": region,
                    "stats_count": len(stats)
                }
            )
        
        # Fallback for old-style responses
        response = "Job Statistics\n\nNo statistics available."
        return ChatResponse(
            response=response,
            response_type="job_statistics",
            structured_content=None
        )

    def _create_welcome_response(self, user_id: str, user_role: str, region: str) -> ChatResponse:
        """Create a welcome card response for greeting messages"""
        # Determine connection status
        region_service = get_region_service()
        connection_status = "no_region"
        region_backend_connected = None
        region_display = "No Region"
        
        if region:
            region_display = region.upper()
            if region_service.is_connected(region):
                connection_status = "connected"
                region_backend_connected = True
            else:
                connection_status = "disconnected" 
                region_backend_connected = False
        
        # Create role-specific welcome message
        if user_role == "Admin":
            title = f"Welcome {user_id}"
            content = f"Hello {user_id}! I'm your Cloud Inventory assistant. As a Admin, you have access to all operations including archiving and deletion."
            suggestions = []
        else:
            title = f"Welcome {user_id}"
            content = f"Hello {user_id}! I'm your Cloud Inventory assistant. As a User, you have read-only access for viewing data."
            suggestions = []
        
        # Create welcome card structured content
        structured_content = {
            "type": "welcome_card",
            "title": title,
            "region": region_display,
            "user_role": user_role,
            "content": content,
            "suggestions": suggestions,
            "connection_status": connection_status,
            "context": {
                "response_type": "initialization",
                "timestamp": datetime.now().isoformat(),
                "region_provided": region is not None,
                "region_backend_connected": region_backend_connected,
                "message_type": connection_status
            }
        }
        
        return ChatResponse(
            response=content,
            suggestions=suggestions,
            response_type="welcome",
            structured_content=structured_content
        )

    def _create_conversational_structured_content(self, response_text: str, user_role: str, region: str, suggestions: List[str]) -> Dict[str, Any]:
        """Create structured content for conversational responses"""
        return {
            "type": "conversational_card",
            "title": "AI Assistant Response",
            "icon": "",
            "region": region.upper(),
            "user_role": user_role,
            "content": response_text,
            "suggestions": suggestions,
            "context": {
                "response_type": "conversational",
                "timestamp": datetime.now().isoformat()
            }
        }

    def _get_archive_table_name(self, table_name: str) -> str:
        """Get the correct archive table name for a given main table name"""
        if table_name == "dsiactivities":
            return "dsiactivitiesarchive"
        elif table_name == "dsitransactionlog":
            return "dsitransactionlogarchive" 
        elif table_name in ["dsiactivitiesarchive", "dsitransactionlogarchive"]:
            return table_name  # Already an archive table
        else:
            return f"{table_name}archive"  # Fallback for other tables

    def _create_error_structured_content(self, error_message: str, region: str) -> Dict[str, Any]:
        """Create structured content for error responses"""
        return {
            "type": "error_card",
            "title": "System Error",
            "icon": "",
            "region": region.upper() if region else "UNKNOWN",
            "error_message": error_message,
            "suggestions": [
                "Check system status"
            ],
            "context": {
                "response_type": "error",
                "timestamp": datetime.now().isoformat()
            }
        }