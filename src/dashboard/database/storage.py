"""SQLite storage for metrics data."""

import aiosqlite
import time
from typing import Dict, Any, Optional
from pathlib import Path


class MetricsStorage:
    """Handles persistent storage of metrics data."""
    
    def __init__(self, db_path: str = "data/metrics.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(exist_ok=True)
    
    async def initialize(self):
        """Initialize the database schema."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS node_metrics (
                    timestamp INTEGER,
                    node_name TEXT,
                    slurm_status TEXT,
                    cpu_percent REAL,
                    memory_percent REAL,
                    load_avg REAL,
                    uptime_seconds REAL,
                    health_reachable INTEGER,
                    PRIMARY KEY (timestamp, node_name)
                )
            """)
            
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_node_time 
                ON node_metrics(node_name, timestamp)
            """)
            
            await db.commit()
    
    async def store_node_data(self, node_name: str, node_data: Dict[str, Any]):
        """Store node data to database."""
        timestamp = int(time.time())
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO node_metrics 
                (timestamp, node_name, slurm_status, cpu_percent, memory_percent, 
                 load_avg, uptime_seconds, health_reachable)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                timestamp,
                node_name,
                node_data.get('slurm_status'),
                node_data.get('cpu_percent'),
                node_data.get('memory_percent'),
                node_data.get('load_avg'),
                node_data.get('uptime_seconds'),
                1 if node_data.get('health_reachable', False) else 0
            ))
            await db.commit()
    
    async def get_recent_data(self, node_name: str, hours: int = 24) -> list[Dict[str, Any]]:
        """Get recent data for a node."""
        since_timestamp = int(time.time() - (hours * 3600))
        
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT * FROM node_metrics 
                WHERE node_name = ? AND timestamp >= ?
                ORDER BY timestamp
            """, (node_name, since_timestamp)) as cursor:
                
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                return [dict(zip(columns, row)) for row in rows]
    
    async def cleanup_old_data(self, days_to_keep: int = 30):
        """Remove old data beyond retention period."""
        cutoff_timestamp = int(time.time() - (days_to_keep * 24 * 3600))
        
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                DELETE FROM node_metrics WHERE timestamp < ?
            """, (cutoff_timestamp,))
            await db.commit()