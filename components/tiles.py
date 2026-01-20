"""
Room Tile Components

Streamlit components for displaying Genie rooms as interactive tiles.
"""

import streamlit as st
import pandas as pd
from typing import Optional, Callable

from utils.formatters import format_duration, format_number


# Tile CSS styles
TILE_CSS = """
<style>
.room-tile-grid {
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
    gap: 16px;
    padding: 10px 0;
}
.room-tile {
    background: linear-gradient(135deg, rgba(18,18,26,0.95) 0%, rgba(26,26,38,0.95) 100%);
    border: 1px solid rgba(255,255,255,0.1);
    border-radius: 16px;
    padding: 20px;
    cursor: pointer;
    transition: all 0.3s ease;
    height: 100%;
}
.room-tile:hover {
    border-color: rgba(124,58,237,0.6);
    box-shadow: 0 8px 32px rgba(124,58,237,0.15);
    transform: translateY(-2px);
}
.room-tile-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    margin-bottom: 16px;
}
.room-tile-icon {
    width: 48px;
    height: 48px;
    border-radius: 12px;
    background: linear-gradient(135deg, rgba(124,58,237,0.2) 0%, rgba(6,182,212,0.2) 100%);
    display: flex;
    align-items: center;
    justify-content: center;
    font-size: 24px;
}
.room-tile-arrow {
    color: #666666;
    font-size: 20px;
    transition: all 0.3s ease;
}
.room-tile:hover .room-tile-arrow {
    color: #ffffff;
    transform: translateX(4px);
}
.room-tile-name {
    color: #ffffff;
    font-size: 18px;
    font-weight: 600;
    margin-bottom: 4px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.room-tile:hover .room-tile-name {
    color: #06b6d4;
}
.room-tile-description {
    color: #888888;
    font-size: 13px;
    margin-bottom: 16px;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
    min-height: 36px;
}
.room-tile-stats {
    display: flex;
    flex-direction: column;
    gap: 8px;
    margin-bottom: 16px;
}
.room-tile-stat {
    display: flex;
    justify-content: space-between;
    font-size: 13px;
}
.room-tile-stat-label {
    color: #888888;
}
.room-tile-stat-value {
    color: #ffffff;
    font-family: 'Monaco', 'Menlo', monospace;
}
.room-tile-stat-value.orange {
    color: #f97316;
}
.room-tile-health {
    padding-top: 12px;
    border-top: 1px solid rgba(255,255,255,0.05);
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
}
.health-good { color: #10b981; }
.health-warning { color: #f97316; }
.health-critical { color: #ef4444; }
</style>
"""


def render_room_card(room: dict) -> str:
    """
    Generate HTML for a single room tile card.
    
    Args:
        room: Dict with room data (id, name, description, query_count, avg_duration_ms, slow_query_count)
        
    Returns:
        HTML string for the card
    """
    name = room.get("name", "Unnamed Room")
    description = room.get("description", "") or "No description"
    query_count = room.get("query_count", 0)
    avg_duration_ms = room.get("avg_duration_ms", 0)
    slow_query_count = room.get("slow_query_count", 0)
    
    # Calculate health
    if query_count > 0:
        health_pct = round(((query_count - slow_query_count) / query_count) * 100)
    else:
        health_pct = 100
    
    if slow_query_count == 0:
        health_class = "health-good"
        health_icon = "✅"
        health_text = "Healthy"
    elif health_pct >= 90:
        health_class = "health-warning"
        health_icon = "⚠️"
        health_text = f"{health_pct}% healthy"
    else:
        health_class = "health-critical"
        health_icon = "⚠️"
        health_text = f"{health_pct}% healthy"
    
    slow_class = "orange" if slow_query_count > 0 else ""
    
    return f"""
    <div class="room-tile" onclick="selectRoom('{room.get('id', '')}')">
        <div class="room-tile-header">
            <div class="room-tile-icon">🗄️</div>
            <div class="room-tile-arrow">→</div>
        </div>
        <div class="room-tile-name">{name}</div>
        <div class="room-tile-description">{description[:100]}</div>
        <div class="room-tile-stats">
            <div class="room-tile-stat">
                <span class="room-tile-stat-label">Queries</span>
                <span class="room-tile-stat-value">{format_number(query_count)}</span>
            </div>
            <div class="room-tile-stat">
                <span class="room-tile-stat-label">Avg Duration</span>
                <span class="room-tile-stat-value">{format_duration(avg_duration_ms)}</span>
            </div>
            {f'''<div class="room-tile-stat">
                <span class="room-tile-stat-label">Slow Queries</span>
                <span class="room-tile-stat-value {slow_class}">{format_number(slow_query_count)}</span>
            </div>''' if slow_query_count > 0 else ''}
        </div>
        <div class="room-tile-health">
            <span>{health_icon}</span>
            <span class="{health_class}">{health_text}</span>
        </div>
    </div>
    """


def render_room_tiles(rooms_df: pd.DataFrame, on_select: Optional[Callable] = None) -> Optional[str]:
    """
    Render a grid of room tiles using Streamlit columns.
    
    Args:
        rooms_df: DataFrame with room data
        on_select: Optional callback when a room is selected
        
    Returns:
        Selected room ID if any
    """
    if rooms_df.empty:
        st.info("No Genie rooms found. Create a Genie room in Databricks to get started.")
        return None
    
    # Inject CSS
    st.markdown(TILE_CSS, unsafe_allow_html=True)
    
    selected_room = None
    
    # Create grid using columns (4 per row)
    rooms_list = rooms_df.to_dict("records")
    
    for i in range(0, len(rooms_list), 4):
        cols = st.columns(4)
        for j, col in enumerate(cols):
            if i + j < len(rooms_list):
                room = rooms_list[i + j]
                with col:
                    # Use a button styled as the tile
                    if st.button(
                        f"🗄️ {room.get('name', 'Room')[:20]}...",
                        key=f"room_{room.get('id', i+j)}",
                        width="stretch",
                        help=f"Click to view {room.get('name', 'Room')} insights"
                    ):
                        selected_room = room.get("id")
                        if on_select:
                            on_select(room)
                    
                    # Display metrics under button
                    query_count = room.get("query_count", 0)
                    avg_duration = format_duration(room.get("avg_duration_ms", 0))
                    slow_count = room.get("slow_query_count", 0)
                    
                    st.caption(f"📊 {format_number(query_count)} queries | ⏱️ {avg_duration}")
                    if slow_count > 0:
                        st.caption(f"⚠️ {slow_count} slow queries")
    
    return selected_room


def render_room_selector(rooms_df: pd.DataFrame) -> Optional[str]:
    """
    Render a selectbox for choosing a room.
    
    Args:
        rooms_df: DataFrame with room data
        
    Returns:
        Selected room ID
    """
    if rooms_df.empty:
        st.warning("No Genie rooms available")
        return None
    
    options = ["-- Select a Room --"] + [
        f"{r['name']} ({format_number(r['query_count'])} queries)"
        for r in rooms_df.to_dict("records")
    ]
    
    room_ids = [None] + rooms_df["id"].tolist()
    
    selected_idx = st.selectbox(
        "Select Genie Room",
        range(len(options)),
        format_func=lambda x: options[x],
    )
    
    return room_ids[selected_idx] if selected_idx > 0 else None
