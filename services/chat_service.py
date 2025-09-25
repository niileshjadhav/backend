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
from .prompt_parser import ParsedOperation
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
                logger.info(f"No region specified, using default: {region}")
            elif not region_service.is_region_valid(region):
                logger.error(f"Invalid region: {region}")
                error_message = f"❌ Invalid Region\n\nRegion '{region}' is not valid. Available regions: {', '.join(region_service.get_valid_regions())}"
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(error_message, "UNKNOWN")
                )
            
            # Ensure region is set on the service
            region_service.set_current_region(region)
            logger.info(f"Current region set to: {region}")

            # Create and save chat log first
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
            
            logger.info(f"Chat logged: session_id={final_session_id}, user_id={final_user_id}, role={user_role}, region={region}")
            
            # Step 0: Handle confirmations for archive/delete operations (security critical)
            if self._is_confirmation_message(user_message):
                return await self._handle_operation_confirmation(
                    user_message, user_info, db, chat_log, region
                )
            
            # Step 0.5: Handle general table statistics requests directly (bypass LLM for reliability)
            if self._is_general_stats_request(user_message):
                logger.info(f"Direct general statistics request detected: {user_message}")
                return await self._handle_general_stats_request(user_info, db, region)
            
            # Step 1: Let LLM decide everything in one intelligent call
            conversation_history = self._get_conversation_history(chat_log.session_id, db)
            
            try:
                llm_result = await self.llm_service.parse_with_enhanced_tools(
                    user_message=user_message, 
                    conversation_context=conversation_history
                )
                
                if llm_result and llm_result.mcp_result:
                    # Database operation - format response based on tool used
                    logger.info(f"LLM selected MCP tool: {llm_result.tool_used} on {llm_result.table_used}")
                    return self._format_response_by_tool(llm_result, region, chat_log.session_id)
                else:
                    # Conversational response
                    logger.info(f"LLM classified as conversational: {user_message}")
                    return await self._handle_conversational(
                        user_message, user_info, db, chat_log, region
                    )
            except Exception as e:
                logger.error(f"LLM processing failed: {e}")
                # Fallback to conversational
                return await self._handle_conversational(
                    user_message, user_info, db, chat_log, region
                )
                
        except Exception as e:
            logger.error(f"Error in process_chat: {e}")
            error_message = f"❌ System Error: {str(e)}\n\nPlease contact your administrator if this problem persists."
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region if 'region' in locals() else "UNKNOWN")
            )

    async def _handle_conversational(
        self, 
        user_message: str, 
        user_info: dict, 
        db: Session, 
        chat_log: ChatOpsLog,
        region: str
    ) -> ChatResponse:
        """Handle conversational messages using LLM without database operations"""
        try:
            # Check if this is a capabilities query
            if self._is_capabilities_query(user_message):
                return self._format_capabilities_response(user_info, region)
            
            conversation_history = self._get_conversation_history(chat_log.session_id, db)
            
            user_id = user_info.get("username", "anonymous") if user_info else "anonymous"
            user_role = user_info.get("role", "Monitor") if user_info else "Monitor"
            
            # Generate conversational response using OpenAI
            llm_response = await self.llm_service.generate_response(
                user_message=user_message,
                user_id=user_id,
                conversation_context=conversation_history
            )
            
            response_text = llm_response.get("response", "I'm here to help with your Cloud Inventory log management questions!")
            suggestions = llm_response.get("suggestions", ["Show table statistics", "Help with archiving", "Explain safety rules"])
            
            # Create structured content for conversational responses
            structured_content = self._create_conversational_structured_content(
                response_text, user_role, region, suggestions
            )
            
            # Update chat log
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
            
            # Handle special case for general stats requests (all tables)
            if tool_used == "get_table_stats" and not table_used:
                # This is a general database statistics request
                logger.info(f"General database statistics request detected")
                return self._format_general_stats_response(mcp_result, region)
            
            # Format response based on tool used
            if tool_used == "get_table_stats":
                return self._format_stats_response(mcp_result, table_used, region)
                
            elif tool_used == "query_logs":
                return self._format_query_response(mcp_result, table_used, region)
                
            elif tool_used == "archive_records":
                return self._format_archive_response(mcp_result, table_used, region, session_id)
                
            elif tool_used == "delete_archived_records":
                return self._format_delete_response(mcp_result, table_used, region, session_id)
                
            elif tool_used == "health_check":
                return self._format_health_response(mcp_result, region)
                
            else:
                # Unknown tool
                logger.warning(f"Unknown MCP tool: {tool_used}")
                error_message = f"❌ Unknown Operation\n\nThe system tried to use an unknown operation: {tool_used}. Please try rephrasing your request."
                return ChatResponse(
                    response=error_message,
                    response_type="error",
                    structured_content=self._create_error_structured_content(
                        f"Unknown operation: {tool_used}",
                        region
                    )
                )
                
        except Exception as e:
            logger.error(f"Response formatting error: {e}")
            error_message = f"❌ Processing Error: {str(e)}\n\nPlease try rephrasing your request."
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region)
            )

    def _is_confirmation_message(self, message: str) -> bool:
        """Check if message is a confirmation for archive/delete operations"""
        message_upper = message.upper().strip()
        confirmation_patterns = [
            'CONFIRM ARCHIVE', 'CONFIRM DELETE', 'YES', 'PROCEED', 'EXECUTE',
            'CANCEL', 'ABORT', 'NO'
        ]
        return any(pattern in message_upper for pattern in confirmation_patterns)

    def _is_general_stats_request(self, message: str) -> bool:
        """Check if message is asking for general table statistics"""
        message_lower = message.lower().strip()
        general_stats_patterns = [
            'show table statistics', 'table statistics', 'database statistics',
            'show database stats', 'show table stats', 'database stats',
            'show all table stats', 'show stats for all tables', 'table summary',
            'database summary', 'show all tables', 'list all tables'
        ]
        return any(pattern in message_lower for pattern in general_stats_patterns)

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
                error_message = "❌ Access Denied\n\nArchive and delete operations require Admin privileges."
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
                response = "❌ Operation Cancelled\n\nThe operation has been cancelled and will not proceed.\nNo changes have been made to the database."
                
                structured_content = {
                    "type": "cancelled_card",
                    "title": "Operation Cancelled",
                    "icon": "❌",
                    "region": region.upper(),
                    "message": "The operation has been cancelled and will not proceed.",
                    "details": ["No changes have been made to the database"],
                    "context": {
                        "response_type": "cancelled",
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
                    context={"cancelled": True}
                )
            
            # Use LLM with conversation context to understand and execute the confirmation
            if "CONFIRM ARCHIVE" in message_upper or "CONFIRM DELETE" in message_upper:
                confirmation_prompt = f"""
                The user is confirming an operation. Based on the conversation history, 
                parse and execute the appropriate archive or delete operation with the EXACT same filters 
                that were used in the preview.
                
                Current confirmation: "{user_message}"
                {conversation_history}
                
                Rules:
                - Use the EXACT same filters from the preview operation in the conversation history
                - For "CONFIRM ARCHIVE": use archive_records MCP tool with confirmed=True
                - For "CONFIRM DELETE": use delete_archived_records MCP tool with confirmed=True
                - Execute with the same table and filters that showed the preview count
                """
                
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
                            
                            response = f"📦 Archive Operation Completed - {region.upper()} Region\n\n"
                            response += f"✅ Successfully archived **{archived_count:,}** records\n"
                            response += f"From: {table_name}\n"
                            response += f"To: {table_name}_archive\n"
                            response += f"Executed by: {user_id}\n\n"
                            response += "Records have been moved from the main table to the archive table."
                            
                            structured_content = {
                                "type": "success_card",
                                "title": f"Archive Completed - {region.upper()} Region",
                                "count": archived_count,
                                "operation": "archive",
                                "details": [
                                    f"Successfully archived **{archived_count:,}** records",
                                    f"From: {table_name}",
                                    f"To: {table_name}_archive",
                                    f"Executed by: {user_id}"
                                ]
                            }
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "archive_completed"
                            chat_log.records_affected = archived_count
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
                            response = f"❌ Archive Operation Failed\n\n{error_msg}"
                            
                            structured_content = self._create_error_structured_content(error_msg, region)
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "archive_failed"
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
                            
                            response = f"🗑️ Delete Operation Completed - {region.upper()} Region\n\n"
                            response += f"✅ Successfully deleted **{deleted_count:,}** records\n"
                            response += f"From: {table_name}\n"
                            response += f"Executed by: {user_id}\n\n"
                            response += "⚠️ Records have been permanently removed from the archive table."
                            
                            structured_content = {
                                "type": "success_card",
                                "title": f"Delete Completed - {region.upper()} Region",
                                "count": deleted_count,
                                "operation": "delete",
                                "details": [
                                    f"Successfully deleted **{deleted_count:,}** records",
                                    f"From: {table_name}",
                                    f"Executed by: {user_id}",
                                    "⚠️ Records have been permanently removed"
                                ]
                            }
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "delete_completed"
                            chat_log.records_affected = deleted_count
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
                            response = f"❌ Delete Operation Failed\n\n{error_msg}"
                            
                            structured_content = self._create_error_structured_content(error_msg, region)
                            
                            chat_log.bot_response = response
                            chat_log.operation_status = "delete_failed"
                            db.commit()
                            
                            return ChatResponse(
                                response=response,
                                response_type="error",
                                structured_content=structured_content
                            )
                else:
                    # LLM failed to process the confirmation
                    error_message = "❌ Confirmation Processing Failed\n\nThe system failed to process your confirmation. Please try again or contact support."
                    return ChatResponse(
                        response=error_message,
                        response_type="error",
                        structured_content=self._create_error_structured_content(
                            "The system failed to process your confirmation. Please try again or contact support.",
                            region
                        )
                    )
            
            # If we get here, the confirmation was not understood
            error_message = "❌ Invalid Confirmation\n\nPlease type 'CONFIRM ARCHIVE', 'CONFIRM DELETE', or 'CANCEL' to proceed."
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
            error_message = f"❌ Error processing confirmation: {str(e)}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(str(e), region)
            )

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

    def _is_capabilities_query(self, message: str) -> bool:
        """Check if the message is asking about bot capabilities"""
        capabilities_patterns = [
            'what can you do', 'what are your capabilities', 'help me', 'what do you do',
            'how can you help', 'what functions', 'what features', 'what services',
            'tell me about yourself', 'what can you help with', 'capabilities',
            'what are you capable of', 'what tasks can you perform'
        ]
        
        message_lower = message.lower().strip()
        
        # Only treat as capabilities query if it explicitly asks about capabilities
        # Not just any greeting message
        return any(pattern in message_lower for pattern in capabilities_patterns)

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
                        response=f"❌ Connection Error - {region.upper()} Region\n\n{error_msg}",
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
                        response=f"❌ Statistics Error - {region.upper()} Region\n\n{error_msg}",
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
                response=f"❌ Statistics Error - {region.upper()} Region\n\n{error_msg}",
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )

    def _format_capabilities_response(self, user_info: dict, region: str) -> ChatResponse:
        """Format a structured capabilities response"""
        user_role = user_info.get("role", "Monitor") if user_info else "Monitor"
        user_name = user_info.get("username", "there") if user_info else "there"
        
        # Base capabilities for all users
        capabilities = [
            {
                "icon": "📊",
                "title": "Database Statistics",
                "description": "Get comprehensive statistics for any table including record counts, date ranges, and data insights.",
                "examples": ["Show activities stats", "Count transactions", "Table statistics"]
            },
            {
                "icon": "🔍",
                "title": "Data Querying",
                "description": "Query and search through log records with filters, date ranges, and specific criteria.",
                "examples": ["Find recent activities", "Find recent transactions"]
            },
            {
                "icon": "💬",
                "title": "Conversational AI",
                "description": "Answer questions about your cloud inventory system and provide guidance on best practices.",
                "examples": ["How does archiving work?", "Explain safety rules"]
            }
        ]
        
        # Add admin-only capabilities
        if user_role == "Admin":
            capabilities.extend([
                {
                    "icon": "📦",
                    "title": "Archive Operations",
                    "description": "Safely move old records to archive tables with confirmation workflows.",
                    "examples": ["Archive activities older than 30 days", "Archive transactions older than 7 days", "Archive logs older than 2 weeks"]
                },
                {
                    "icon": "🗑️",
                    "title": "Delete Operations",
                    "description": "Permanently remove archived records with strict safety confirmations.",
                    "examples": ["Delete archived logs older than 30 days", "Delete archived data older than 60 days", "Clean old archived records"]
                }
            ])
        
        quick_tips = [
            "Use natural language - I understand conversational requests",
            f"Currently working in {region.upper()} region",
            "I provide safety confirmations for destructive operations",
            "Type specific table names or let me suggest the right one"
        ]
        
        if user_role != "Admin":
            quick_tips.append("Archive/delete operations require Admin privileges")
        
        # Plain text response for backward compatibility
        response = f"👋 Hello {user_name}! I'm your Cloud Inventory Assistant\n\n"
        response += f" Current Region: {region.upper()}\n"
        response += f" Your Role: {user_role}\n\n"
        response += "🚀 What I can help you with:\n\n"
        
        for cap in capabilities:
            response += f"{cap['icon']}  {cap['title']}\n"
            response += f"   {cap['description']}\n"
            if cap.get('examples'):
                response += f"   *Examples: {', '.join(cap['examples'])}*\n"
            response += "\n"
        
        response += "💡 Quick Tips:\n"
        for tip in quick_tips:
            response += f"• {tip}\n"
        
        # Structured content for rich rendering
        structured_content = {
            "type": "capabilities_card",
            "title": f"Hello {user_name}! 👋",
            "subtitle": "Cloud Inventory Management Assistant",
            "region": region.upper(),
            "user_role": user_role,
            "capabilities": capabilities,
            "quick_tips": quick_tips
        }
        
        # Enhanced suggestions based on user role
        suggestions = ["Show table statistics", "Help with queries", "Explain regions"]
        if user_role == "Admin":
            suggestions.extend(["Archive old data", "Safety procedures"])
        
        return ChatResponse(
            response=response,
            response_type="capabilities",
            structured_content=structured_content,
            suggestions=suggestions,
            context={"user_role": user_role, "region": region}
        )

    def _format_stats_response(self, mcp_result: dict, table_name: str, region: str) -> ChatResponse:
        """Format table statistics response with structured content"""
        if not mcp_result.get("success"):
            error_msg = mcp_result.get("error", "Unknown error")
            error_message = f"❌ Stats Error - {region.upper()} Region\n\n{error_msg}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        # Build statistics display - MCP result returns data at root level
        total_count = mcp_result.get("record_count", 0)
        earliest_date = mcp_result.get("earliest_date")
        latest_date = mcp_result.get("latest_date")
        filter_description = mcp_result.get("filter_description")
        
        # Plain text response for backward compatibility
        response = f"📊 Table Statistics - {region.upper()} Region\n\n"
        response += f"Table: {table_name}\n"
        response += f"Total Records: **{total_count:,}**\n"
        
        # Add filter information if available
        if filter_description:
            response += f"Filter: Records {filter_description}\n"

        # Structured content for rich rendering
        structured_content = {
            "type": "stats_card",
            "title": f"Table Statistics - {region.upper()} Region",
            "icon": "📊",
            "table_name": table_name,
            "region": region.upper(),
            "stats": [
                {"label": "Total Records", "value": f"{total_count:,}", "type": "number", "highlight": True},
            ]
        }
        
        # Add additional stats to structured content from MCP result
        if filter_description:
            structured_content["stats"].append({
                "label": f"Filtered ({filter_description})", 
                "value": f"{total_count:,}", 
                "type": "number"
            })
        
        return ChatResponse(
            response=response,
            response_type="stats",
            structured_content=structured_content,
            context={"count": total_count, "table": table_name, "tool": "get_table_stats"}
        )

    def _format_query_response(self, mcp_result: dict, table_name: str, region: str) -> ChatResponse:
        """Format query results response with structured content"""
        if not mcp_result.get("success"):
            error_msg = mcp_result.get("error", "Unknown error")
            error_message = f"❌ Query Error - {region.upper()} Region\n\n{error_msg}"
            return ChatResponse(
                response=error_message,
                response_type="error",
                structured_content=self._create_error_structured_content(error_msg, region)
            )
        
        records = mcp_result.get("records", [])
        total_found = mcp_result.get("total_records", len(records))
        
        # Plain text response for backward compatibility
        response = f"📋 Query Results - {region.upper()} Region\n\n"
        response += f"Table: {table_name}\n"
        response += f"**Total Records Found: {total_found:,}**\n"
        
        if len(records) != total_found:
            response += f"Showing: **{len(records)}** records\n\n"
        else:
            response += f"Showing: All **{len(records)}** records\n\n"

        if records:
            # Show first few records in a readable format
            for i, record in enumerate(records[:5], 1):
                response += f"Record {i}:\n"
                for key, value in record.items():
                    response += f"  • {key}: {value}\n"
                response += "\n"
            
            if len(records) > 5:
                response += f"... and {len(records) - 5} more records\n"
        else:
            response += "No matching records found.\n"
        
        # Structured content for table rendering
        structured_content = {
            "type": "data_table",
            "title": f"Query Results - {region.upper()} Region",
            "icon": "📋",
            "table_name": table_name,
            "region": region.upper(),
            "total_count": total_found,
            "showing_count": len(records),
            "columns": list(records[0].keys()) if records else [],
            "data": records[:10],  # Limit to first 10 records for display
            "has_more": len(records) > 10
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
                "name": stats.get("display_name", table_name),
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
        response = f"📊 Database Statistics - {region.upper()} Region\n\n"
        
        # Main Tables Section
        response += "🗂️ Main Tables:\n"
        for table in main_tables:
            if table["error"]:
                response += f"• {table['name']}: ❌ Error - {table['error']}\n"
            else:
                response += f"• {table['name']}: **{table['total_records']:,}** total records"
                if table['age_based_count'] > 0:
                    response += f", **{table['age_based_count']:,}** records older than {table['age_days']} days\n"
                else:
                    response += "\n"
        
        response += "\n📦 Archive Tables:\n"
        for table in archive_tables:
            if table["error"]:
                response += f"• {table['name']}: ❌ Error - {table['error']}\n"
            else:
                response += f"• {table['name']}: **{table['total_records']:,}** total records"
                if table['age_based_count'] > 0:
                    response += f", **{table['age_based_count']:,}** records older than {table['age_days']} days\n"
                else:
                    response += "\n"
        
        # Build structured content
        structured_content = {
            "type": "database_overview",
            "title": f"Database Statistics - {region.upper()} Region",
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
            response = f"📦 Archive Preview - {region.upper()} Region\n\n"
            response += f"Ready to Archive: 🟠 **{count:,} records** 🟠\n"
            response += f"From Table: {table_name}\n"
            response += f"To Table: {table_name}_archive\n\n"
            response += "⚠️ This will move records from main table to archive table.\n\n"
            response += "Type 'CONFIRM ARCHIVE' to proceed or 'CANCEL' to abort."
            
            # Structured content for confirmation
            structured_content = {
                "type": "confirmation_card",
                "title": f"Archive Preview - {region.upper()} Region",
                "operation": "archive",
                "table_name": table_name,
                "count": count,
                "warning": "This will move records from main table to archive table.",
                "instructions": "Type 'CONFIRM ARCHIVE' to proceed or 'CANCEL' to abort."
            }
            
            return ChatResponse(
                response=response,
                response_type="archive_confirmation",
                structured_content=structured_content,
                requires_confirmation=True,
                context={"count": count, "tool": "archive_records", "table": table_name}
            )
        
        # Handle case where there are no records to archive
        if count == 0:
            response = f"📦 Archive Result - {region.upper()} Region\n\n"
            response += f"✅ No records found matching the criteria\n"
            response += f"Table: {table_name}\n\n"
            response += "No archive operation was needed."
            
            # Structured content for no records
            structured_content = {
                "type": "success_card",
                "title": f"Archive Result - {region.upper()} Region",
                "operation": "archive",
                "table_name": table_name,
                "count": 0,
                "message": "No records found matching the criteria",
                "details": ["No archive operation was needed"]
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
            response = f"📦 Archive Operation Completed - {region.upper()} Region\n\n"
            response += f"✅ Successfully archived **{count:,}** records\n"
            response += f"From: {table_name}\n"
            response += f"To: {table_name}_archive\n\n"
            response += "Records have been moved from the main table to the archive table."
            
            # Structured content for success
            structured_content = {
                "type": "success_card",
                "title": f"Archive Completed - {region.upper()} Region",
                "count": count,
                "operation": "archive",
                "details": [
                    f"Successfully archived **{count:,}** records",
                    f"From: {table_name}",
                    f"To: {table_name}_archive"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Archive failed")
            response = f"❌ Archive Error - {region.upper()} Region\n\n{error_msg}"
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
            response = f"🗑️ Delete Preview - {region.upper()} Region\n\n"
            response += f"Ready to Delete: 🔴 **{count:,} records** 🔴\n"
            response += f"From Table: {table_name}\n\n"
            response += "🚨 WARNING: THIS WILL PERMANENTLY DELETE RECORDS 🚨\n\n"
            response += "Type 'CONFIRM DELETE' to proceed or 'CANCEL' to abort."
            
            # Structured content for confirmation
            structured_content = {
                "type": "confirmation_card",
                "title": f"Delete Preview - {region.upper()} Region",
                "operation": "delete",
                "table_name": table_name,
                "count": count,
                "warning": "🚨 WARNING: THIS WILL PERMANENTLY DELETE RECORDS 🚨",
                "instructions": "Type 'CONFIRM DELETE' to proceed or 'CANCEL' to abort."
            }
            
            return ChatResponse(
                response=response,
                response_type="delete_confirmation",
                structured_content=structured_content,
                requires_confirmation=True,
                context={"count": count, "tool": "delete_archived_records", "table": table_name}
            )
        
        # Handle case where there are no records to delete
        if count == 0:
            response = f"🗑️ Delete Result - {region.upper()} Region\n\n"
            response += f"✅ No records found matching the criteria\n"
            response += f"Table: {table_name}\n\n"
            response += "No delete operation was needed."
            
            # Structured content for no records
            structured_content = {
                "type": "success_card",
                "title": f"Delete Result - {region.upper()} Region",
                "operation": "delete",
                "table_name": table_name,
                "count": 0,
                "message": "No records found matching the criteria",
                "details": ["No delete operation was needed"]
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
            response = f"🗑️ Delete Operation Completed - {region.upper()} Region\n\n"
            response += f"✅ Successfully deleted **{count:,}** records\n"
            response += f"From: {table_name}\n\n"
            response += "⚠️ Records have been permanently removed."
            
            # Structured content for success
            structured_content = {
                "type": "success_card",
                "title": f"Delete Completed - {region.upper()} Region",
                "count": count,
                "operation": "delete",
                "details": [
                    f"Successfully deleted **{count:,}** records",
                    f"From: {table_name}",
                    "⚠️ Records have been permanently removed"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Delete failed")
            response = f"❌ Delete Error - {region.upper()} Region\n\n{error_msg}"
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
            response = f"✅ System Health Check - {region.upper()} Region\n\n"
            response += "All database connections and services are operational."
            
            structured_content = {
                "type": "success_card",
                "title": f"System Health Check - {region.upper()} Region",
                "details": [
                    "All database connections are operational",
                    "Services are running normally",
                    "System is ready for operations"
                ]
            }
        else:
            error_msg = mcp_result.get("error", "Health check failed")
            response = f"❌ System Health Issues - {region.upper()} Region\n\n{error_msg}"
            structured_content = self._create_error_structured_content(error_msg, region)
        
        return ChatResponse(
            response=response,
            response_type="health_check",
            structured_content=structured_content,
            context={"tool": "health_check"}
        )

    def _create_conversational_structured_content(self, response_text: str, user_role: str, region: str, suggestions: List[str]) -> Dict[str, Any]:
        """Create structured content for conversational responses"""
        return {
            "type": "conversational_card",
            "title": "AI Assistant Response",
            "icon": "🤖",
            "region": region.upper(),
            "user_role": user_role,
            "content": response_text,
            "suggestions": suggestions,
            "context": {
                "response_type": "conversational",
                "timestamp": datetime.now().isoformat()
            }
        }

    def _create_error_structured_content(self, error_message: str, region: str) -> Dict[str, Any]:
        """Create structured content for error responses"""
        return {
            "type": "error_card",
            "title": "System Error",
            "icon": "❌",
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