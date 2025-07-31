"""Cluster state management."""

import asyncio
from typing import Dict, Any
from datetime import datetime


class ClusterState:
    """Manages the current state of the cluster."""
    
    def __init__(self):
        self.nodes: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.last_slurm_update = None
        self.last_health_update = None
    
    async def update_slurm_status(self, node: str, status: str):
        """Update Slurm status for a node."""
        async with self.lock:
            if node not in self.nodes:
                self.nodes[node] = {}
            self.nodes[node]['slurm_status'] = status
            self.nodes[node]['slurm_updated'] = datetime.now()
            self.last_slurm_update = datetime.now()
    
    async def update_health_data(self, node: str, metrics: Dict[str, Any]):
        """Update health metrics for a node."""
        async with self.lock:
            if node not in self.nodes:
                self.nodes[node] = {}
            self.nodes[node].update({
                'cpu_percent': metrics.get('cpu_percent'),
                'memory_percent': metrics.get('memory_percent'),
                'load_avg': metrics.get('load_avg'),
                'uptime': metrics.get('uptime'),
                'health_reachable': True,
                'health_updated': datetime.now()
            })
            self.last_health_update = datetime.now()
    
    async def mark_node_unreachable(self, node: str):
        """Mark a node as unreachable for health data."""
        async with self.lock:
            if node not in self.nodes:
                self.nodes[node] = {}
            self.nodes[node].update({
                'cpu_percent': None,
                'memory_percent': None,
                'load_avg': None,
                'uptime': None,
                'health_reachable': False,
                'health_updated': datetime.now()
            })
    
    async def get_current_nodes(self) -> Dict[str, Dict[str, Any]]:
        """Get current node data."""
        async with self.lock:
            return self.nodes.copy()
    
    async def get_node_list(self) -> set[str]:
        """Get list of known node names."""
        async with self.lock:
            return set(self.nodes.keys())