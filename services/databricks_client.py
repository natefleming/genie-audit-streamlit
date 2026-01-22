"""
Databricks Client Service

Provides a wrapper around the Databricks SDK WorkspaceClient for:
- Executing SQL queries against system.query.history
- Listing and retrieving Genie spaces
- Caching results for performance
"""

import os
import re
from typing import Optional, Any
from dataclasses import dataclass
from functools import lru_cache
import time

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


@dataclass
class GenieSpace:
    """Represents a Genie space/room."""
    id: str
    name: str
    description: str
    created_at: str
    warehouse_id: Optional[str] = None
    owner: Optional[str] = None


@dataclass
class GenieConversation:
    """Represents a Genie conversation."""
    conversation_id: str
    title: str = ""
    created_time: str = ""
    last_updated_time: str = ""


@dataclass
class GenieMessageAttachment:
    """Represents an attachment in a Genie message."""
    attachment_type: str = ""
    statement_id: str = ""
    sql_content: str = ""


@dataclass
class GenieMessage:
    """Represents a message in a Genie conversation."""
    message_id: str
    content: str = ""
    status: str = ""
    created_timestamp: int = 0  # Epoch milliseconds from Genie API
    attachments: list[GenieMessageAttachment] = None
    
    def __post_init__(self):
        if self.attachments is None:
            self.attachments = []


@dataclass
class QueryMetrics:
    """SQL query with full performance metrics."""
    statement_id: str
    query_text: str = ""
    start_time: str = ""
    total_duration_ms: int = 0
    compilation_ms: int = 0
    execution_ms: int = 0
    queue_wait_ms: int = 0
    compute_wait_ms: int = 0
    result_fetch_ms: int = 0
    ai_overhead_sec: float = 0.0
    bytes_scanned: int = 0
    rows_scanned: int = 0
    rows_returned: int = 0
    execution_status: str = ""
    bottleneck: str = "NORMAL"
    speed_category: str = "FAST"
    # Fields for correlation matching
    genie_conversation_id: str = ""  # From query_source.genie_conversation_id
    executed_by: str = ""  # User email who executed the query
    # Concurrency metrics at query start time
    genie_concurrent: int = 0  # Concurrent Genie queries at start time
    warehouse_concurrent: int = 0  # Concurrent warehouse queries at start time


@dataclass
class MessageWithQueries:
    """A message (prompt) with its linked SQL queries."""
    message_id: str
    content: str = ""
    status: str = ""
    timestamp: str = ""
    queries: list[QueryMetrics] = None
    # Aggregated metrics for this message
    query_count: int = 0
    total_duration_ms: int = 0
    # AI overhead: time from message submission to first SQL query (Genie model inference)
    ai_overhead_sec: float = 0.0
    # Total response time: AI overhead + SQL execution time
    total_response_sec: float = 0.0
    # Performance issue flags
    has_performance_issue: bool = False
    has_slow_ai: bool = False  # AI overhead > 10s
    has_slow_query: bool = False  # Any query > 10s
    # Message source: "API" (genieCreateConversationMessage, genieStartConversationMessage)
    #                 "Space" (createConversationMessage) 
    #                 "Unknown" if not determined
    message_source: str = "Unknown"
    
    def __post_init__(self):
        if self.queries is None:
            self.queries = []
        # Compute aggregates if queries are provided
        if self.queries:
            self.query_count = len(self.queries)
            self.total_duration_ms = sum(q.total_duration_ms for q in self.queries)
        # Compute total response time (AI overhead + SQL duration)
        sql_duration_sec = self.total_duration_ms / 1000.0
        self.total_response_sec = self.ai_overhead_sec + sql_duration_sec
        # Detect performance issues
        self.has_slow_ai = self.ai_overhead_sec > 10.0
        self.has_slow_query = any(q.total_duration_ms >= 10000 for q in self.queries)
        self.has_performance_issue = self.has_slow_ai or self.has_slow_query


@dataclass
class ConversationWithMessages:
    """Conversation with its messages and linked SQL queries."""
    conversation_id: str
    title: str = ""
    created_time: str = ""
    last_updated_time: str = ""
    user_email: str = ""
    messages: list[MessageWithQueries] = None
    # Aggregated metrics
    total_queries: int = 0
    total_duration_ms: int = 0
    avg_duration_ms: float = 0.0
    slowest_query_ms: int = 0
    success_rate: float = 100.0
    # Conversation source: "API" if started via genieStartConversationMessage
    #                      "Space" if started via createConversation in UI
    #                      Derived from first message's source
    conversation_source: str = "Unknown"
    # Performance metrics (computed)
    total_ai_overhead_sec: float = 0.0  # Sum of AI overhead across all messages
    avg_response_sec: float = 0.0  # Average total response time (AI + SQL)
    slowest_response_sec: float = 0.0  # Slowest message response (max)
    fastest_response_sec: float = 0.0  # Fastest message response (min)
    # Performance issue counts
    slow_ai_count: int = 0  # Messages with AI overhead > 10s
    slow_query_count: int = 0  # Queries > 10s
    has_performance_issues: bool = False
    
    def __post_init__(self):
        if self.messages is None:
            self.messages = []
        # Compute aggregates if messages are provided
        if self.messages:
            all_queries = [q for m in self.messages for q in m.queries]
            self.total_queries = len(all_queries)
            if all_queries:
                self.total_duration_ms = sum(q.total_duration_ms for q in all_queries)
                self.avg_duration_ms = self.total_duration_ms / len(all_queries)
                self.slowest_query_ms = max(q.total_duration_ms for q in all_queries)
                successful = sum(1 for q in all_queries if q.execution_status == "FINISHED")
                self.success_rate = (successful / len(all_queries)) * 100.0
            # Derive conversation source from first message
            if self.messages and self.messages[0].message_source != "Unknown":
                self.conversation_source = self.messages[0].message_source
            # Compute AI overhead and response time metrics
            self.total_ai_overhead_sec = sum(m.ai_overhead_sec for m in self.messages)
            response_times = [m.total_response_sec for m in self.messages if m.total_response_sec > 0]
            if response_times:
                self.avg_response_sec = sum(response_times) / len(response_times)
                self.slowest_response_sec = max(response_times)
                self.fastest_response_sec = min(response_times)
            # Count performance issues
            self.slow_ai_count = sum(1 for m in self.messages if m.has_slow_ai)
            self.slow_query_count = sum(1 for q in all_queries if q.total_duration_ms >= 10000)
            self.has_performance_issues = self.slow_ai_count > 0 or self.slow_query_count > 0


class DatabricksClient:
    """
    Client for interacting with Databricks APIs.
    
    Uses WorkspaceClient which auto-discovers credentials when running
    in Databricks Apps or uses environment variables locally.
    """
    
    def __init__(self, warehouse_id: Optional[str] = None):
        """
        Initialize the Databricks client.
        
        Args:
            warehouse_id: SQL warehouse ID to use for queries.
                         Falls back to DATABRICKS_WAREHOUSE_ID env var.
        """
        self._client = WorkspaceClient()
        self._warehouse_id = warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID")
        self._cache: dict[str, tuple[float, Any]] = {}
        self._cache_ttl = 60  # 1 minute cache TTL
    
    @property
    def workspace_client(self) -> WorkspaceClient:
        """Get the underlying WorkspaceClient."""
        return self._client
    
    def get_current_user(self) -> Optional[str]:
        """Get the current user's email/username."""
        cache_key = "current_user"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            me = self._client.current_user.me()
            user_email = me.user_name or me.display_name or ""
            self._set_cached(cache_key, user_email)
            return user_email
        except Exception as e:
            print(f"Could not get current user: {e}")
            return None
    
    def get_query_profile_url(self, statement_id: str) -> Optional[str]:
        """
        Build URL to view query profile in Databricks Query History UI.
        
        The URL opens the query details page where users can view the full
        query profile and use the built-in "Download as JSON" feature.
        
        Args:
            statement_id: The SQL statement ID from query history
            
        Returns:
            URL string to the query profile page, or None if URL cannot be built
        """
        try:
            # Get workspace host - try multiple sources (Databricks App deployment)
            # 1. WorkspaceClient config (auto-discovered in Databricks Apps)
            # 2. Environment variables (DATABRICKS_HOST)
            host = self._client.config.host
            if not host:
                host = os.getenv("DATABRICKS_HOST")
            if not host:
                print("Could not get workspace host from client config or environment")
                return None
            
            # Normalize host (remove trailing slash, ensure https)
            host = host.rstrip('/')
            if not host.startswith('http'):
                host = f"https://{host}"
            
            # Get workspace ID - try multiple sources
            workspace_id: Optional[str] = None
            
            # 1. Try SDK's get_workspace_id() method first (most reliable)
            try:
                workspace_id = str(self._client.get_workspace_id())
            except Exception:
                pass
            
            # 2. Try environment variable DATABRICKS_WORKSPACE_ID
            if not workspace_id:
                workspace_id = os.getenv("DATABRICKS_WORKSPACE_ID")
            
            # 3. Extract from host URL as fallback
            if not workspace_id:
                # Azure pattern: adb-{workspace_id}.{region}.azuredatabricks.net
                azure_match = re.search(r'adb-(\d+)\.', host)
                if azure_match:
                    workspace_id = azure_match.group(1)
                else:
                    # AWS/GCP pattern or /?o={workspace_id} in URL
                    aws_match = re.search(r'[/\?]o=(\d+)', host)
                    if aws_match:
                        workspace_id = aws_match.group(1)
            
            if not workspace_id:
                print(f"Could not determine workspace ID from SDK, env, or host: {host}")
                return None
            
            # Build the Query History URL
            # Format: {host}/sql/history?o={workspace_id}&queryId={statement_id}
            url = f"{host}/sql/history?o={workspace_id}&queryId={statement_id}"
            
            return url
            
        except Exception as e:
            print(f"Error building query profile URL: {e}")
            return None
    
    def _get_cached(self, key: str) -> Optional[Any]:
        """Get a cached value if not expired."""
        if key in self._cache:
            timestamp, value = self._cache[key]
            if time.time() - timestamp < self._cache_ttl:
                return value
            del self._cache[key]
        return None
    
    def _set_cached(self, key: str, value: Any) -> None:
        """Cache a value with current timestamp."""
        self._cache[key] = (time.time(), value)
    
    def clear_cache(self) -> None:
        """Clear all cached data."""
        self._cache.clear()
    
    def execute_sql(self, sql: str, use_cache: bool = True) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.
        
        Args:
            sql: SQL query to execute
            use_cache: Whether to use cached results
            
        Returns:
            pandas DataFrame with query results
        """
        if not self._warehouse_id:
            raise ValueError(
                "No warehouse ID configured. Set DATABRICKS_WAREHOUSE_ID environment variable "
                "or pass warehouse_id to DatabricksClient constructor."
            )
        
        # Check cache
        cache_key = f"sql:{hash(sql)}"
        if use_cache:
            cached = self._get_cached(cache_key)
            if cached is not None:
                return cached
        
        # Execute query (wait_timeout max is 50s)
        response = self._client.statement_execution.execute_statement(
            warehouse_id=self._warehouse_id,
            statement=sql,
            wait_timeout="50s",
        )
        
        # Check for errors
        if response.status.state == StatementState.FAILED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"SQL execution failed: {error_msg}")
        
        # Parse results into DataFrame
        if response.result is None or response.manifest is None:
            return pd.DataFrame()
        
        columns = [col.name for col in response.manifest.schema.columns]
        data = []
        
        if response.result.data_array:
            for row in response.result.data_array:
                data.append(dict(zip(columns, row)))
        
        df = pd.DataFrame(data)
        
        # Cache the result
        if use_cache:
            self._set_cached(cache_key, df)
        
        return df
    
    def list_genie_spaces(self, progress_callback: Optional[callable] = None) -> list[GenieSpace]:
        """
        List all Genie spaces in the workspace using the Genie API.
        
        First tries the SDK's list_spaces method, then falls back to direct REST API
        for older SDK versions.
        
        Args:
            progress_callback: Optional callback function that receives (count, has_more, total=None) 
                               to report loading progress.
        
        Returns:
            List of GenieSpace objects
        """
        cache_key = "genie_spaces"
        cached = self._get_cached(cache_key)
        if cached is not None:
            if progress_callback:
                progress_callback(len(cached), False, None)
            return cached
        
        spaces = []
        max_pages = 30  # Support up to 3000 spaces
        
        # Try SDK list_spaces method first (newer SDK versions)
        if hasattr(self._client.genie, 'list_spaces'):
            try:
                page_token = None
                page_count = 0
                
                while page_count < max_pages:
                    page_count += 1
                    response = self._client.genie.list_spaces(page_size=100, page_token=page_token)
                    spaces_in_page = response.spaces or []
                    
                    for space in spaces_in_page:
                        owner = getattr(space, 'creator_name', None) or getattr(space, 'creator', None)
                        create_time = getattr(space, 'create_time', None)
                        spaces.append(GenieSpace(
                            id=space.space_id or "",
                            name=space.title or space.space_id or "Unnamed Space",
                            description=space.description or "",
                            created_at=str(create_time) if create_time else "",
                            warehouse_id=getattr(space, 'warehouse_id', None),
                            owner=owner,
                        ))
                    
                    page_token = getattr(response, 'next_page_token', None)
                    
                    if progress_callback:
                        progress_callback(len(spaces), page_token is not None, None)
                    
                    if not page_token:
                        break
                
                if spaces:
                    self._set_cached(cache_key, spaces)
                    return spaces
                    
            except Exception as e:
                print(f"SDK list_spaces failed, trying REST API: {e}")
                spaces = []  # Reset for fallback
        
        # Fallback: Use REST API directly (works in all SDK versions)
        try:
            page_token = None
            page_count = 0
            
            while page_count < max_pages:
                page_count += 1
                
                # Build query params
                query_params = {'page_size': 100}
                if page_token:
                    query_params['page_token'] = page_token
                
                # Direct REST API call
                response = self._client.api_client.do('GET', '/api/2.0/genie/spaces', query=query_params)
                spaces_in_page = response.get('spaces', [])
                
                for space in spaces_in_page:
                    spaces.append(GenieSpace(
                        id=space.get('space_id', ''),
                        name=space.get('title') or space.get('space_id', 'Unnamed Space'),
                        description=space.get('description', ''),
                        created_at=space.get('create_time', ''),
                        warehouse_id=space.get('warehouse_id'),
                        owner=space.get('creator_name') or space.get('creator'),
                    ))
                
                page_token = response.get('next_page_token')
                
                if progress_callback:
                    progress_callback(len(spaces), page_token is not None, None)
                
                if not page_token:
                    break
            
            if spaces:
                self._set_cached(cache_key, spaces)
            return spaces
            
        except Exception as e:
            print(f"REST API list spaces failed: {e}")
            return []
    
    def get_genie_space(self, space_id: str) -> Optional[GenieSpace]:
        """
        Get a specific Genie space by ID.
        
        Args:
            space_id: The Genie space ID
            
        Returns:
            GenieSpace object or None if not found
        """
        cache_key = f"genie_space:{space_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            space = self._client.genie.get_space(space_id=space_id)
            
            create_time = getattr(space, 'create_time', None)
            result = GenieSpace(
                id=space.space_id or space_id,
                name=space.title or space.space_id or "Unnamed Space",
                description=space.description or "",
                created_at=str(create_time) if create_time else "",
                warehouse_id=getattr(space, 'warehouse_id', None),
            )
            
            self._set_cached(cache_key, result)
            return result
            
        except Exception as e:
            print(f"Warning: Failed to get Genie space {space_id}: {e}")
            return None
    
    def get_genie_message_content(
        self, 
        space_id: str, 
        conversation_id: str, 
        message_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Retrieve the user's prompt from a Genie conversation.
        
        Uses REST API: GET /api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages
        
        Args:
            space_id: The Genie space ID
            conversation_id: The conversation ID
            message_id: Optional specific message ID (if None, returns first user message)
            
        Returns:
            The user's prompt text, or None if not found
        """
        if not conversation_id or conversation_id == "N/A":
            return None
            
        cache_key = f"genie_message:{space_id}:{conversation_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            # Try REST API to get conversation messages
            response = self._client.api_client.do(
                'GET',
                f'/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages'
            )
            
            messages = response.get('messages', [])
            
            # Debug: log the response structure
            print(f"Genie API response for {conversation_id}: {len(messages)} messages found")
            if messages and len(messages) > 0:
                first_msg = messages[0]
                print(f"First message keys: {list(first_msg.keys()) if isinstance(first_msg, dict) else type(first_msg)}")
            
            # If message_id provided, find that specific message
            if message_id:
                for msg in messages:
                    if msg.get('id') == message_id or msg.get('message_id') == message_id:
                        content = msg.get('content', '')
                        if content:
                            self._set_cached(cache_key, content)
                            return content
            
            # Otherwise, find the first user message (the original prompt)
            for msg in messages:
                role = msg.get('role', '').lower()
                if role in ('user', 'human'):
                    content = msg.get('content', '')
                    if content:
                        self._set_cached(cache_key, content)
                        return content
            
            # If no user message found, try getting from message text/query field
            for msg in messages:
                # Some APIs use 'text' or 'query' instead of 'content'
                content = msg.get('text') or msg.get('query') or msg.get('content', '')
                if content and msg.get('role', '').lower() in ('user', 'human', ''):
                    self._set_cached(cache_key, content)
                    return content
            
            return None
            
        except Exception as e:
            print(f"Could not retrieve Genie message for conversation {conversation_id}: {e}")
            return None
    
    def list_conversations(
        self, 
        space_id: str, 
        max_conversations: int = 500
    ) -> list[GenieConversation]:
        """
        List recent conversations for a Genie space using the SDK with pagination.
        
        Args:
            space_id: The Genie space ID
            max_conversations: Maximum number of conversations to return (default 500)
            
        Returns:
            List of GenieConversation objects
        """
        cache_key = f"conversations:{space_id}"
        cached = self._get_cached(cache_key)
        # Only use cache if it has conversations (don't cache empty results)
        if cached is not None and len(cached) > 0:
            print(f"[DEBUG] Returning {len(cached)} cached conversations for space {space_id}")
            return cached
        
        conversations: list[GenieConversation] = []
        page_size = min(100, max_conversations)  # API typically supports up to 100 per page
        max_pages = 20  # Safety limit to prevent infinite loops
        
        # Use SDK genie.list_conversations with pagination
        # Returns GenieListConversationsResponse with .conversations list and .next_page_token
        try:
            print(f"[DEBUG] Calling genie.list_conversations for space_id={space_id} (with pagination)")
            
            # Try without include_all first (most common case - user's own conversations)
            page_token = None
            page_count = 0
            
            while page_count < max_pages and len(conversations) < max_conversations:
                page_count += 1
                
                # Build kwargs for API call
                kwargs = {
                    "space_id": space_id,
                    "page_size": page_size,
                }
                if page_token:
                    kwargs["page_token"] = page_token
                
                response = self._client.genie.list_conversations(**kwargs)
                
                if page_count == 1:
                    print(f"[DEBUG] SDK response type: {type(response)}")
                    print(f"[DEBUG] SDK response has conversations: {response.conversations is not None if response else 'None response'}")
                
                # Response is GenieListConversationsResponse with .conversations list
                if response and response.conversations:
                    print(f"[DEBUG] Page {page_count}: {len(response.conversations)} conversations")
                    for conv in response.conversations:
                        if len(conversations) >= max_conversations:
                            break
                        # GenieConversationSummary has: conversation_id, title, created_timestamp
                        conversations.append(GenieConversation(
                            conversation_id=conv.conversation_id or "",
                            title=conv.title or "",
                            created_time=str(conv.created_timestamp) if conv.created_timestamp else "",
                            last_updated_time="",  # Not available in summary
                        ))
                
                # Check for next page
                page_token = getattr(response, 'next_page_token', None)
                if not page_token:
                    print(f"[DEBUG] No more pages (fetched {page_count} page(s))")
                    break
                else:
                    print(f"[DEBUG] Has next_page_token, fetching page {page_count + 1}...")
            
            # If no conversations, also try with include_all=True (admin view)
            if not conversations:
                print(f"[DEBUG] No conversations found, trying with include_all=True...")
                try:
                    page_token = None
                    page_count = 0
                    
                    while page_count < max_pages and len(conversations) < max_conversations:
                        page_count += 1
                        
                        kwargs = {
                            "space_id": space_id,
                            "include_all": True,
                            "page_size": page_size,
                        }
                        if page_token:
                            kwargs["page_token"] = page_token
                        
                        response = self._client.genie.list_conversations(**kwargs)
                        
                        if response and response.conversations:
                            for conv in response.conversations:
                                if len(conversations) >= max_conversations:
                                    break
                                conversations.append(GenieConversation(
                                    conversation_id=conv.conversation_id or "",
                                    title=conv.title or "",
                                    created_time=str(conv.created_timestamp) if conv.created_timestamp else "",
                                    last_updated_time="",
                                ))
                        
                        page_token = getattr(response, 'next_page_token', None)
                        if not page_token:
                            break
                            
                except Exception as admin_err:
                    print(f"[DEBUG] include_all=True failed (expected for non-admins): {admin_err}")
            
            print(f"[DEBUG] SDK list_conversations: {len(conversations)} conversations for space {space_id}")
            if conversations:
                print(f"[DEBUG] First conversation: id={conversations[0].conversation_id}, title={conversations[0].title[:50] if conversations[0].title else 'No title'}")
            
            # Only cache if we got results
            if conversations:
                self._set_cached(cache_key, conversations)
            return conversations
            
        except Exception as e:
            print(f"[DEBUG] SDK list_conversations failed: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def get_conversation_messages(
        self, 
        space_id: str, 
        conversation_id: str
    ) -> list[GenieMessage]:
        """
        Get all messages for a conversation including attachments using the SDK.
        
        Args:
            space_id: The Genie space ID
            conversation_id: The conversation ID
            
        Returns:
            List of GenieMessage objects with content and attachments
        """
        cache_key = f"messages:{space_id}:{conversation_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        messages: list[GenieMessage] = []
        
        # Use SDK genie.list_conversation_messages
        try:
            response = self._client.genie.list_conversation_messages(
                space_id=space_id, 
                conversation_id=conversation_id
            )
            
            # Response is GenieListConversationMessagesResponse with .messages list
            # Based on API: GenieMessage has message_id (not id), content, attachments, status
            # Attachments: GenieAttachment has attachment_id, query (QueryAttachment), text (TextAttachment)
            if response and response.messages:
                print(f"[DEBUG] Processing {len(response.messages)} messages")
                for msg in response.messages:
                    # Extract attachments
                    attachments: list[GenieMessageAttachment] = []
                    if msg.attachments:
                        for att in msg.attachments:
                            att_obj = GenieMessageAttachment(
                                attachment_type="query" if att.query else ("text" if att.text else "other"),
                            )
                            # Check for query attachment which contains SQL
                            if att.query:
                                att_obj.statement_id = att.query.statement_id or ""
                                att_obj.sql_content = att.query.description or ""
                                print(f"[DEBUG] Found query attachment with statement_id: {att_obj.statement_id[:20] if att_obj.statement_id else 'NONE'}...")
                            attachments.append(att_obj)
                    
                    # Use message_id (not id!) - id is None in the API response
                    # Also capture created_timestamp for AI overhead calculation
                    msg_obj = GenieMessage(
                        message_id=msg.message_id or "",
                        content=msg.content or "",
                        status=str(msg.status) if msg.status else "",
                        created_timestamp=msg.created_timestamp or 0,
                        attachments=attachments,
                    )
                    messages.append(msg_obj)
                    print(f"[DEBUG] Message: id={msg_obj.message_id[:12] if msg_obj.message_id else 'NONE'}..., created_ts={msg_obj.created_timestamp}, content='{msg_obj.content[:30] if msg_obj.content else 'EMPTY'}...', attachments={len(attachments)}")
            
            if messages:
                print(f"[DEBUG] SDK get_messages: {len(messages)} messages for conv {conversation_id[:8]}...")
                first_content = messages[0].content[:50] if messages[0].content else "EMPTY"
                print(f"[DEBUG] First message content: '{first_content}'")
                if messages[0].attachments:
                    print(f"[DEBUG] First message has {len(messages[0].attachments)} attachments")
            
            self._set_cached(cache_key, messages)
            return messages
            
        except Exception as e:
            print(f"[DEBUG] SDK get_messages failed for {conversation_id}: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL for comparison by removing whitespace and lowercasing."""
        if not sql:
            return ""
        # Remove comments, normalize whitespace, lowercase
        sql = re.sub(r'--.*$', '', sql, flags=re.MULTILINE)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        sql = re.sub(r'\s+', ' ', sql)
        return sql.strip().lower()
    
    def find_prompt_for_query(
        self,
        space_id: str,
        statement_id: str,
        statement_text: str,
    ) -> Optional[dict]:
        """
        Find the Genie prompt that generated a given SQL query using reverse lookup.
        
        Lists conversations for the space, checks message attachments for matching SQL.
        
        Args:
            space_id: The Genie space ID
            statement_id: The SQL statement ID to match
            statement_text: The SQL query text to match
            
        Returns:
            dict with {conversation_id, message_id, prompt, matched_by} or None
        """
        cache_key = f"prompt_lookup:{statement_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        try:
            conversations = self.list_conversations(space_id, max_conversations=50)
            normalized_target = self._normalize_sql(statement_text)
            
            for conv in conversations:
                conv_id = conv.conversation_id
                if not conv_id:
                    continue
                
                messages = self.get_conversation_messages(space_id, conv_id)
                
                # Find user prompt (first message content that's not SQL)
                user_prompt: Optional[str] = None
                for msg in messages:
                    content = msg.content
                    if content and not user_prompt:
                        # Check if it looks like a question (not SQL)
                        if not content.strip().upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')):
                            user_prompt = content
                    
                    # Check attachments for SQL match
                    for att in msg.attachments:
                        # Check for direct statement_id match
                        if att.statement_id and att.statement_id == statement_id:
                            result = {
                                'conversation_id': conv_id,
                                'message_id': msg.message_id,
                                'prompt': user_prompt,
                                'matched_by': 'statement_id'
                            }
                            self._set_cached(cache_key, result)
                            return result
                        
                        # Check for SQL text match
                        att_sql = att.sql_content
                        if att_sql:
                            normalized_att = self._normalize_sql(att_sql)
                            # Use substring match for partial SQL (query_text may be truncated)
                            if normalized_target and normalized_att:
                                if normalized_target[:200] in normalized_att or normalized_att[:200] in normalized_target:
                                    result = {
                                        'conversation_id': conv_id,
                                        'message_id': msg.message_id,
                                        'prompt': user_prompt,
                                        'matched_by': 'sql_text'
                                    }
                                    self._set_cached(cache_key, result)
                                    return result
            
            return None
            
        except Exception as e:
            print(f"Error in find_prompt_for_query: {e}")
            return None
    
    def get_prompts_for_queries(
        self,
        space_id: str,
        queries_df: pd.DataFrame,
        progress_callback: Optional[callable] = None
    ) -> dict[str, str]:
        """
        Get prompts for multiple queries at once using reverse lookup.
        
        Uses strongly typed SDK objects to list conversations and messages,
        then builds an index mapping statement_id -> prompt text.
        
        Args:
            space_id: The Genie space ID
            queries_df: DataFrame with statement_id and query_text columns
            progress_callback: Optional callback(current, total) for progress
            
        Returns:
            dict mapping statement_id to prompt text
        """
        prompts: dict[str, str] = {}
        
        if queries_df.empty:
            return prompts
        
        try:
            # Get all conversations for the space using strongly typed SDK
            conversations = self.list_conversations(space_id, max_conversations=100)
            print(f"[DEBUG] Found {len(conversations)} conversations for space {space_id}")
            
            # Build index: statement_id -> prompt AND normalized_sql -> prompt
            statement_to_prompt: dict[str, str] = {}
            sql_to_prompt: dict[str, str] = {}
            
            for i, conv in enumerate(conversations):
                conv_id = conv.conversation_id
                if not conv_id:
                    continue
                
                # Get messages using strongly typed SDK
                messages = self.get_conversation_messages(space_id, conv_id)
                
                # Debug first conversation
                if i == 0 and messages:
                    first_msg = messages[0]
                    print(f"[DEBUG] First message: id={first_msg.message_id}, content='{first_msg.content[:50] if first_msg.content else 'EMPTY'}', attachments={len(first_msg.attachments)}")
                
                # Find user prompt (first message that's not SQL)
                user_prompt: Optional[str] = None
                for msg in messages:
                    content = msg.content
                    if content and not user_prompt:
                        # Check if it looks like a question (not SQL)
                        if not content.strip().upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE', 'CREATE')):
                            user_prompt = content
                    
                    # Index all SQL attachments
                    for att in msg.attachments:
                        # Index by statement_id
                        if att.statement_id and user_prompt:
                            statement_to_prompt[att.statement_id] = user_prompt
                        
                        # Index by normalized SQL
                        if att.sql_content and user_prompt:
                            normalized = self._normalize_sql(att.sql_content)
                            if normalized:
                                sql_to_prompt[normalized[:300]] = user_prompt
            
            print(f"[DEBUG] Built index: {len(statement_to_prompt)} statement_ids, {len(sql_to_prompt)} SQL texts")
            
            # Now match each query
            total = len(queries_df)
            matched = 0
            for idx, row in queries_df.iterrows():
                statement_id = str(row.get('statement_id', ''))
                query_text = str(row.get('query_text', '') or '')
                
                # Try statement_id match first
                if statement_id in statement_to_prompt:
                    prompts[statement_id] = statement_to_prompt[statement_id]
                    matched += 1
                else:
                    # Try SQL text match
                    normalized = self._normalize_sql(query_text)
                    if normalized[:300] in sql_to_prompt:
                        prompts[statement_id] = sql_to_prompt[normalized[:300]]
                        matched += 1
                
                if progress_callback:
                    progress_callback(idx + 1, total)
            
            print(f"[DEBUG] Matched {matched}/{total} queries to prompts")
            
            return prompts
            
        except Exception as e:
            print(f"Error getting prompts for queries: {e}")
            import traceback
            traceback.print_exc()
            return prompts
    
    def get_spaces_with_metrics(self, days: int = 30) -> pd.DataFrame:
        """
        Get all Genie spaces with aggregated query metrics.
        
        Args:
            days: Number of days to look back for metrics
            
        Returns:
            DataFrame with space info and metrics
        """
        # Get spaces from Genie API
        spaces = self.list_genie_spaces()
        
        if not spaces:
            return pd.DataFrame()
        
        # Get metrics from query history
        sql = f"""
        SELECT 
            query_source.genie_space_id as space_id,
            COUNT(*) as query_count,
            AVG(total_duration_ms) as avg_duration_ms,
            SUM(CASE WHEN total_duration_ms > 30000 THEN 1 ELSE 0 END) as slow_query_count,
            ROUND(100.0 * SUM(CASE WHEN execution_status = 'FINISHED' THEN 1 ELSE 0 END) / COUNT(*), 1) as success_rate
        FROM system.query.history
        WHERE start_time >= current_date() - INTERVAL {days} DAYS
          AND query_source.genie_space_id IS NOT NULL
        GROUP BY query_source.genie_space_id
        """
        
        try:
            metrics_df = self.execute_sql(sql)
        except Exception:
            metrics_df = pd.DataFrame()
        
        # Merge spaces with metrics
        spaces_data = []
        for space in spaces:
            row = {
                "id": space.id,
                "name": space.name,
                "description": space.description,
                "created_at": space.created_at,
                "warehouse_id": space.warehouse_id,
                "query_count": 0,
                "avg_duration_ms": 0.0,
                "slow_query_count": 0,
                "success_rate": 100.0,
            }
            
            if not metrics_df.empty:
                space_metrics = metrics_df[metrics_df["space_id"] == space.id]
                if not space_metrics.empty:
                    row["query_count"] = int(space_metrics["query_count"].iloc[0] or 0)
                    row["avg_duration_ms"] = float(space_metrics["avg_duration_ms"].iloc[0] or 0)
                    row["slow_query_count"] = int(space_metrics["slow_query_count"].iloc[0] or 0)
                    row["success_rate"] = float(space_metrics["success_rate"].iloc[0] or 100)
            
            spaces_data.append(row)
        
        return pd.DataFrame(spaces_data)
    
    def get_conversations_with_query_metrics(
        self,
        space_id: str,
        max_conversations: int = 50,
        progress_callback: Optional[callable] = None
    ) -> list[ConversationWithMessages]:
        """
        Get conversations with their messages and linked SQL query metrics.
        
        This method implements a conversation-first data model:
        1. Lists conversations via Genie API
        2. Gets messages for each conversation
        3. Extracts statement_ids from message attachments
        4. Batches queries to system.query.history for metrics
        5. Assembles hierarchical structure with computed aggregates
        
        Args:
            space_id: The Genie space ID
            max_conversations: Maximum number of conversations to fetch
            progress_callback: Optional callback(current, total, status_text) for progress
            
        Returns:
            List of ConversationWithMessages with full hierarchy
        """
        from queries.sql import (
            QUERIES_BY_STATEMENT_IDS, 
            build_statement_ids_filter,
            get_conversation_sources_query,
            get_message_ai_overhead_query,
            get_queries_by_space_and_time,
        )
        from datetime import datetime, timedelta
        
        cache_key = f"conversations_with_metrics:{space_id}:{max_conversations}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        result: list[ConversationWithMessages] = []
        
        try:
            # Step 1: List conversations
            if progress_callback:
                progress_callback(0, max_conversations, "Fetching conversations...")
            
            conversations = self.list_conversations(space_id, max_conversations)
            print(f"[DEBUG] get_conversations_with_query_metrics: {len(conversations)} conversations")
            
            if not conversations:
                return result
            
            # Step 1b: Fetch conversation sources from audit logs
            # This tells us if each conversation was initiated via API or Genie Space UI
            conversation_source_map: dict[str, str] = {}
            try:
                source_sql = get_conversation_sources_query(space_id, hours=720)
                source_df = self.execute_sql(source_sql, use_cache=True)
                
                if not source_df.empty:
                    # Group by conversation_id and take the first (earliest) action
                    for _, row in source_df.iterrows():
                        conv_id = str(row.get("conversation_id", "") or "")
                        if conv_id and conv_id not in conversation_source_map:
                            conversation_source_map[conv_id] = str(row.get("message_source", "Unknown") or "Unknown")
                    
                    print(f"[DEBUG] Found sources for {len(conversation_source_map)} conversations from audit logs")
            except Exception as e:
                print(f"[DEBUG] Could not fetch conversation sources from audit: {e}")
            
            # Step 1c: Fetch AI overhead per message from audit logs
            message_ai_overhead_map: dict[str, float] = {}  # message_id -> ai_overhead_sec
            message_timestamp_map: dict[str, str] = {}  # message_id -> timestamp
            message_user_map: dict[str, str] = {}  # message_id -> user_email (for correlation)
            try:
                overhead_sql = get_message_ai_overhead_query(space_id, hours=720)
                overhead_df = self.execute_sql(overhead_sql, use_cache=True)
                
                if not overhead_df.empty:
                    for _, row in overhead_df.iterrows():
                        msg_id = str(row.get("message_id", "") or "")
                        if msg_id:
                            message_ai_overhead_map[msg_id] = float(row.get("ai_overhead_sec", 0) or 0)
                            message_timestamp_map[msg_id] = str(row.get("message_time", "") or "")
                            # Store user email for correlation matching
                            message_user_map[msg_id] = str(row.get("user_email", "") or "")
                            # Also update conversation source if not already set
                            conv_id = str(row.get("conversation_id", "") or "")
                            if conv_id and conv_id not in conversation_source_map:
                                conversation_source_map[conv_id] = str(row.get("message_source", "Unknown") or "Unknown")
                    
                    print(f"[DEBUG] Found AI overhead for {len(message_ai_overhead_map)} messages, user emails for {len(message_user_map)}")
            except Exception as e:
                print(f"[DEBUG] Could not fetch AI overhead from audit: {e}")
            
            # Step 1d: Fetch ALL queries for this space from query history (for time-based correlation)
            # This is needed because Genie Space UI interactions don't populate query.statement_id in attachments
            all_space_queries: list[QueryMetrics] = []
            space_query_by_id: dict[str, QueryMetrics] = {}
            try:
                if progress_callback:
                    progress_callback(0, max_conversations, "Fetching SQL queries for space...")
                
                space_queries_sql = get_queries_by_space_and_time(space_id, hours=720)
                space_queries_df = self.execute_sql(space_queries_sql, use_cache=True)
                
                if not space_queries_df.empty:
                    for _, row in space_queries_df.iterrows():
                        stmt_id = str(row.get("statement_id", "") or "")
                        if stmt_id:
                            qm = QueryMetrics(
                                statement_id=stmt_id,
                                query_text=str(row.get("query_text", "") or ""),
                                start_time=str(row.get("start_time", "") or ""),
                                total_duration_ms=int(row.get("total_duration_ms", 0) or 0),
                                compilation_ms=int(row.get("compilation_ms", 0) or 0),
                                execution_ms=int(row.get("execution_ms", 0) or 0),
                                queue_wait_ms=int(row.get("queue_wait_ms", 0) or 0),
                                compute_wait_ms=int(row.get("compute_wait_ms", 0) or 0),
                                result_fetch_ms=int(row.get("result_fetch_ms", 0) or 0),
                                ai_overhead_sec=0.0,
                                bytes_scanned=int(row.get("bytes_scanned", 0) or 0),
                                rows_scanned=int(row.get("rows_scanned", 0) or 0),
                                rows_returned=int(row.get("rows_returned", 0) or 0),
                                execution_status=str(row.get("execution_status", "") or ""),
                                bottleneck=str(row.get("bottleneck", "NORMAL") or "NORMAL"),
                                speed_category=str(row.get("speed_category", "FAST") or "FAST"),
                                # Correlation fields
                                genie_conversation_id=str(row.get("genie_conversation_id", "") or ""),
                                executed_by=str(row.get("executed_by", "") or ""),
                            )
                            all_space_queries.append(qm)
                            space_query_by_id[stmt_id] = qm
                    
                    print(f"[DEBUG] Found {len(all_space_queries)} total queries for space from query history")
            except Exception as e:
                print(f"[DEBUG] Could not fetch space queries: {e}")
                import traceback
                traceback.print_exc()
            
            # Step 1e: Get warehouse_id from the Genie space for concurrency calculation
            space_warehouse_id = ""
            try:
                space_info = self.get_genie_space(space_id)
                if space_info and space_info.warehouse_id:
                    space_warehouse_id = space_info.warehouse_id
                    print(f"[DEBUG] Got warehouse_id from Genie space: {space_warehouse_id}")
                else:
                    print(f"[DEBUG] Could not get warehouse_id from Genie space")
            except Exception as e:
                print(f"[DEBUG] Error getting Genie space info: {e}")
            
            # Step 2: Collect all statement_ids across all conversations
            all_statement_ids: list[str] = []
            conversation_messages: dict[str, list[GenieMessage]] = {}
            
            for i, conv in enumerate(conversations):
                if progress_callback:
                    progress_callback(i + 1, len(conversations), f"Loading messages for conversation {i + 1}...")
                
                messages = self.get_conversation_messages(space_id, conv.conversation_id)
                conversation_messages[conv.conversation_id] = messages
                
                # Extract statement_ids from attachments
                for msg in messages:
                    for att in msg.attachments:
                        if att.statement_id:
                            all_statement_ids.append(att.statement_id)
            
            print(f"[DEBUG] Found {len(all_statement_ids)} statement_ids across all conversations")
            
            # Step 3: Batch query for all statement metrics
            query_metrics_map: dict[str, QueryMetrics] = {}
            
            if all_statement_ids:
                if progress_callback:
                    progress_callback(len(conversations), len(conversations), "Fetching SQL query metrics...")
                
                # Deduplicate statement_ids
                unique_statement_ids = list(set(all_statement_ids))
                
                # Query in batches of 100 to avoid query size limits
                batch_size = 100
                for batch_start in range(0, len(unique_statement_ids), batch_size):
                    batch_ids = unique_statement_ids[batch_start:batch_start + batch_size]
                    statement_ids_str = build_statement_ids_filter(batch_ids)
                    sql = QUERIES_BY_STATEMENT_IDS.format(statement_ids=statement_ids_str)
                    
                    try:
                        metrics_df = self.execute_sql(sql, use_cache=False)
                        
                        for _, row in metrics_df.iterrows():
                            stmt_id = str(row.get("statement_id", ""))
                            if stmt_id:
                                query_metrics_map[stmt_id] = QueryMetrics(
                                    statement_id=stmt_id,
                                    query_text=str(row.get("query_text", "") or ""),
                                    start_time=str(row.get("start_time", "") or ""),
                                    total_duration_ms=int(row.get("total_duration_ms", 0) or 0),
                                    compilation_ms=int(row.get("compilation_ms", 0) or 0),
                                    execution_ms=int(row.get("execution_ms", 0) or 0),
                                    queue_wait_ms=int(row.get("queue_wait_ms", 0) or 0),
                                    compute_wait_ms=int(row.get("compute_wait_ms", 0) or 0),
                                    result_fetch_ms=int(row.get("result_fetch_ms", 0) or 0),
                                    ai_overhead_sec=0.0,  # Will be computed per message
                                    bytes_scanned=int(row.get("bytes_scanned", 0) or 0),
                                    rows_scanned=int(row.get("rows_scanned", 0) or 0),
                                    rows_returned=int(row.get("rows_returned", 0) or 0),
                                    execution_status=str(row.get("execution_status", "") or ""),
                                    bottleneck=str(row.get("bottleneck", "NORMAL") or "NORMAL"),
                                    speed_category=str(row.get("speed_category", "FAST") or "FAST"),
                                )
                    except Exception as e:
                        print(f"[DEBUG] Error fetching metrics for batch: {e}")
            
            # NOTE: Concurrency metrics (genie_concurrent, warehouse_concurrent) are calculated
            # on-demand when a user selects a specific query for detailed view via load_query_concurrency().
            # This avoids N+M SQL calls during initial load which was causing significant delays.
            
            print(f"[DEBUG] Fetched metrics for {len(query_metrics_map)} queries")
            
            # Step 4: Assemble hierarchical structure
            for conv in conversations:
                messages_raw = conversation_messages.get(conv.conversation_id, [])
                
                # Determine conversation source from audit logs
                # API = genieStartConversationMessage, genieCreateConversationMessage
                # Space = createConversation, createConversationMessage
                conv_source = conversation_source_map.get(conv.conversation_id, "Unknown")
                
                # Find user prompts and build MessageWithQueries
                messages_with_queries: list[MessageWithQueries] = []
                user_email = ""
                
                # Get user email from the first message in the conversation (who started it)
                # This comes from audit logs via message_user_map
                if messages_raw:
                    for msg_check in messages_raw:
                        first_msg_user = message_user_map.get(msg_check.message_id, "")
                        if first_msg_user:
                            user_email = first_msg_user
                            break
                
                # Track which queries have been assigned to avoid duplicates
                assigned_query_ids: set[str] = set()
                
                for msg in messages_raw:
                    # Extract queries for this message
                    msg_queries: list[QueryMetrics] = []
                    
                    # Method 1: Direct statement_id from attachments (API-initiated queries)
                    for att in msg.attachments:
                        if att.statement_id:
                            # Try query_metrics_map first (from attachment batch query)
                            if att.statement_id in query_metrics_map:
                                msg_queries.append(query_metrics_map[att.statement_id])
                                assigned_query_ids.add(att.statement_id)
                            # Fall back to space_query_by_id (from space-wide query)
                            elif att.statement_id in space_query_by_id:
                                msg_queries.append(space_query_by_id[att.statement_id])
                                assigned_query_ids.add(att.statement_id)
                    
                    # Method 2: Correlation for Space UI interactions (no attachment statement_id)
                    # Priority: 1) conversation_id match, 2) user + time window match
                    if not msg_queries and all_space_queries:
                        msg_timestamp_str = message_timestamp_map.get(msg.message_id, "")
                        msg_user_email = message_user_map.get(msg.message_id, "")
                        
                        # Method 2a: Try matching by genie_conversation_id (most accurate)
                        for qm in all_space_queries:
                            if qm.statement_id in assigned_query_ids:
                                continue
                            
                            # If query has genie_conversation_id and it matches this conversation
                            if qm.genie_conversation_id and qm.genie_conversation_id == conv.conversation_id:
                                # Also verify time proximity (query should be after message)
                                if msg_timestamp_str:
                                    try:
                                        msg_time = pd.to_datetime(msg_timestamp_str)
                                        query_time = pd.to_datetime(qm.start_time)
                                        time_diff = (query_time - msg_time).total_seconds()
                                        if 0 <= time_diff <= 120:
                                            msg_queries.append(qm)
                                            assigned_query_ids.add(qm.statement_id)
                                            print(f"[DEBUG] Conv-matched query {qm.statement_id[:12]}... to message (conv_id match + time)")
                                    except Exception:
                                        pass
                                else:
                                    # No timestamp, but conversation_id matches - still use it
                                    msg_queries.append(qm)
                                    assigned_query_ids.add(qm.statement_id)
                                    print(f"[DEBUG] Conv-matched query {qm.statement_id[:12]}... to message (conv_id match only)")
                        
                        # Method 2b: Fall back to user + time window matching
                        if not msg_queries and msg_timestamp_str:
                            try:
                                msg_time = pd.to_datetime(msg_timestamp_str)
                                
                                for qm in all_space_queries:
                                    if qm.statement_id in assigned_query_ids:
                                        continue
                                    
                                    try:
                                        query_time = pd.to_datetime(qm.start_time)
                                        time_diff = (query_time - msg_time).total_seconds()
                                        
                                        # Must be within 2 minute window
                                        if 0 <= time_diff <= 120:
                                            # Prefer queries from the same user
                                            if msg_user_email and qm.executed_by:
                                                if qm.executed_by == msg_user_email:
                                                    msg_queries.append(qm)
                                                    assigned_query_ids.add(qm.statement_id)
                                                    print(f"[DEBUG] User+time matched query {qm.statement_id[:12]}... to message (user: {msg_user_email[:20]}..., diff: {time_diff:.1f}s)")
                                            else:
                                                # No user info available, fall back to time-only matching
                                                msg_queries.append(qm)
                                                assigned_query_ids.add(qm.statement_id)
                                                print(f"[DEBUG] Time-only matched query {qm.statement_id[:12]}... to message (diff: {time_diff:.1f}s)")
                                    except Exception:
                                        pass
                            except Exception as e:
                                print(f"[DEBUG] Could not parse message timestamp: {e}")
                    
                    # Only include messages that have content or queries
                    if msg.content or msg_queries:
                        # Primary: Compute AI overhead from Genie API timestamp to first query start
                        ai_overhead = 0.0
                        
                        # Method 1: If we have linked queries, compute AI overhead from message timestamp to earliest query
                        if msg_queries and msg.created_timestamp:
                            try:
                                msg_time = pd.to_datetime(msg.created_timestamp, unit='ms', utc=True)
                                candidate_queries: list[tuple[QueryMetrics, float]] = []
                                
                                for q in msg_queries:
                                    if q.start_time:
                                        try:
                                            q_time = pd.to_datetime(q.start_time)
                                            if msg_time.tzinfo is not None and q_time.tzinfo is None:
                                                q_time = q_time.tz_localize('UTC')
                                            diff = (q_time - msg_time).total_seconds()
                                            # Accept any positive diff (query started after message)
                                            if diff > 0:
                                                candidate_queries.append((q, diff))
                                        except Exception:
                                            pass
                                
                                if candidate_queries:
                                    earliest = min(candidate_queries, key=lambda x: x[1])
                                    ai_overhead = earliest[1]
                                    print(f"[DEBUG] AI overhead: {ai_overhead:.1f}s from {len(msg_queries)} linked queries (msg_ts={msg.created_timestamp})")
                            except Exception as e:
                                print(f"[DEBUG] Could not compute AI overhead from linked queries: {e}")
                        
                        # Method 2: No linked queries but have message timestamp - search all space queries
                        elif msg.created_timestamp and all_space_queries:
                            try:
                                msg_time = pd.to_datetime(msg.created_timestamp, unit='ms', utc=True)
                                candidate_queries = []
                                
                                for q in all_space_queries:
                                    # Don't filter by assigned_query_ids - we just want to find AI overhead
                                    if q.start_time:
                                        try:
                                            q_time = pd.to_datetime(q.start_time)
                                            if msg_time.tzinfo is not None and q_time.tzinfo is None:
                                                q_time = q_time.tz_localize('UTC')
                                            diff = (q_time - msg_time).total_seconds()
                                            # Query must start after message and within 5 minutes
                                            if 0 < diff < 300:
                                                candidate_queries.append((q, diff))
                                        except Exception:
                                            pass
                                
                                if candidate_queries:
                                    earliest = min(candidate_queries, key=lambda x: x[1])
                                    ai_overhead = earliest[1]
                                    print(f"[DEBUG] AI overhead: {ai_overhead:.1f}s from space queries (msg_ts={msg.created_timestamp})")
                            except Exception as e:
                                print(f"[DEBUG] Could not compute AI overhead from space queries: {e}")
                        
                        # Method 3: Fallback to audit log value if still 0
                        if ai_overhead == 0.0:
                            ai_overhead = message_ai_overhead_map.get(msg.message_id, 0.0)
                            if ai_overhead > 0:
                                print(f"[DEBUG] AI overhead from audit logs: {ai_overhead:.1f}s (msg_id={msg.message_id[:12]}...)")
                            else:
                                # Debug: explain why AI overhead is 0
                                has_queries = len(msg_queries) > 0
                                has_msg_ts = msg.created_timestamp > 0
                                has_audit = msg.message_id in message_ai_overhead_map
                                print(f"[DEBUG] AI overhead=0: msg_id={msg.message_id[:12]}..., has_queries={has_queries}, has_msg_ts={has_msg_ts}, in_audit_map={has_audit}")
                        
                        timestamp = message_timestamp_map.get(msg.message_id, "")
                        
                        msg_with_queries = MessageWithQueries(
                            message_id=msg.message_id,
                            content=msg.content,
                            status=msg.status,
                            timestamp=timestamp,
                            queries=msg_queries,
                            ai_overhead_sec=ai_overhead,
                            # Use conversation source for all messages in that conversation
                            message_source=conv_source,
                        )
                        messages_with_queries.append(msg_with_queries)
                
                # Build conversation with messages
                conv_with_msgs = ConversationWithMessages(
                    conversation_id=conv.conversation_id,
                    title=conv.title or "Untitled Conversation",
                    created_time=conv.created_time,
                    last_updated_time=conv.last_updated_time,
                    user_email=user_email,
                    messages=messages_with_queries,
                    conversation_source=conv_source,
                )
                
                # Include all conversations with at least one message
                # (even if they don't have queries - user may want to see conversation context)
                if messages_with_queries:
                    result.append(conv_with_msgs)
            
            print(f"[DEBUG] Built {len(result)} conversations with query metrics")
            
            # Sort by most recent first
            result.sort(key=lambda c: c.created_time, reverse=True)
            
            self._set_cached(cache_key, result)
            return result
            
        except Exception as e:
            print(f"Error in get_conversations_with_query_metrics: {e}")
            import traceback
            traceback.print_exc()
            return result


# Singleton instance
_client: Optional[DatabricksClient] = None


def get_client(warehouse_id: Optional[str] = None) -> DatabricksClient:
    """
    Get the singleton DatabricksClient instance.
    
    Args:
        warehouse_id: Optional warehouse ID to use
        
    Returns:
        DatabricksClient instance
    """
    global _client
    if _client is None:
        _client = DatabricksClient(warehouse_id=warehouse_id)
    return _client
