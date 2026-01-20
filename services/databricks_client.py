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
    attachments: list[GenieMessageAttachment] = None
    
    def __post_init__(self):
        if self.attachments is None:
            self.attachments = []


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
        max_conversations: int = 100
    ) -> list[GenieConversation]:
        """
        List recent conversations for a Genie space using the SDK.
        
        Args:
            space_id: The Genie space ID
            max_conversations: Maximum number of conversations to return
            
        Returns:
            List of GenieConversation objects
        """
        cache_key = f"conversations:{space_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached
        
        conversations: list[GenieConversation] = []
        
        # Use SDK genie.list_conversations with include_all=True
        try:
            response = self._client.genie.list_conversations(
                space_id=space_id,
                include_all=True,
                page_size=max_conversations
            )
            
            # Response is GenieListConversationsResponse with .conversations list
            if response and response.conversations:
                for conv in response.conversations:
                    conversations.append(GenieConversation(
                        conversation_id=conv.conversation_id or "",
                        title=conv.title or "",
                        created_time=str(conv.created_timestamp) if conv.created_timestamp else "",
                        last_updated_time=str(conv.last_updated_timestamp) if conv.last_updated_timestamp else "",
                    ))
            
            print(f"[DEBUG] SDK list_conversations: {len(conversations)} conversations for space {space_id}")
            if conversations:
                print(f"[DEBUG] First conversation: id={conversations[0].conversation_id}, title={conversations[0].title[:50] if conversations[0].title else 'No title'}")
            
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
            if response and response.messages:
                for msg in response.messages:
                    # Extract attachments
                    attachments: list[GenieMessageAttachment] = []
                    if msg.attachments:
                        for att in msg.attachments:
                            att_obj = GenieMessageAttachment(
                                attachment_type=str(att.type) if att.type else "",
                            )
                            # Check for query attachment which contains SQL
                            if att.query:
                                att_obj.statement_id = att.query.statement_id or ""
                                att_obj.sql_content = att.query.description or ""
                            attachments.append(att_obj)
                    
                    messages.append(GenieMessage(
                        message_id=msg.id or "",
                        content=msg.content or "",
                        status=str(msg.status) if msg.status else "",
                        attachments=attachments,
                    ))
            
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
