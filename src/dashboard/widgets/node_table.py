"""Node table widget for displaying cluster status."""

from typing import Dict, Any
from textual.widgets import DataTable
from textual.reactive import reactive


class NodeTable(DataTable):
    """Table displaying node status and health metrics."""
    
    nodes_data: reactive[Dict[str, Dict[str, Any]]] = reactive({})
    
    def on_mount(self):
        """Initialize the table columns."""
        self.add_columns("Node", "Slurm Status", "CPU", "Memory", "Load", "Uptime")
        self.cursor_type = "row"
    
    async def update_nodes(self, nodes: Dict[str, Dict[str, Any]]):
        """Update the table with new node data."""
        self.nodes_data = nodes
        self._refresh_table()
    
    def _refresh_table(self):
        """Refresh the table display with current data."""
        self.clear()
        
        for node_name, node_data in self.nodes_data.items():
            slurm_status = node_data.get('slurm_status', 'unknown')
            
            if node_data.get('health_reachable', False):
                cpu = f"{node_data.get('cpu_percent', 0):.1f}%"
                memory = f"{node_data.get('memory_percent', 0):.1f}%"
                load = f"{node_data.get('load_avg', 0):.1f}"
                uptime = self._format_uptime(node_data.get('uptime_seconds', 0))
            else:
                cpu = memory = load = uptime = "--"
            
            self.add_row(
                node_name,
                slurm_status,
                cpu,
                memory,
                load,
                uptime
            )
    
    def _format_uptime(self, uptime_seconds: float) -> str:
        """Format uptime in a human-readable format."""
        if uptime_seconds == 0:
            return "--"
        
        days = int(uptime_seconds // 86400)
        hours = int((uptime_seconds % 86400) // 3600)
        
        if days > 0:
            return f"{days}d"
        elif hours > 0:
            return f"{hours}h"
        else:
            minutes = int((uptime_seconds % 3600) // 60)
            return f"{minutes}m"