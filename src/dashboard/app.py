"""Main Textual application class."""

import asyncio
from textual.app import App
from textual.containers import Container
from textual.widgets import Header, Footer

from dashboard.widgets.node_table import NodeTable
from dashboard.models.cluster_state import ClusterState
from dashboard.collectors.slurm_collector import SlurmCollector
from dashboard.collectors.health_collector import HealthCollector
from dashboard.database.storage import MetricsStorage


class DashboardApp(App):
    """Main dashboard application."""
    
    CSS_PATH = "dashboard.tcss"
    TITLE = "Cluster Monitor"
    
    def __init__(self, slurm_controller_url: str = "http://localhost:8080"):
        super().__init__()
        self.cluster_state = ClusterState()
        self.node_table = NodeTable()
        self.storage = MetricsStorage()
        
        # Initialize collectors
        self.slurm_collector = SlurmCollector(slurm_controller_url, self.cluster_state)
        self.health_collector = HealthCollector(self.cluster_state)
        
        self._collection_tasks = []
    
    def compose(self):
        """Create child widgets for the app."""
        yield Header()
        yield Container(
            self.node_table,
            id="main-container"
        )
        yield Footer()
    
    async def on_mount(self):
        """Initialize the app."""
        # Initialize database
        await self.storage.initialize()
        
        # Start background data collection tasks
        self._collection_tasks = [
            asyncio.create_task(self.slurm_collector.start_collection()),
            asyncio.create_task(self.health_collector.start_collection()),
            asyncio.create_task(self._storage_loop())
        ]
        
        # Start UI refresh
        self.set_interval(2.0, self.refresh_display)
    
    async def refresh_display(self):
        """Update the display with current data."""
        nodes = await self.cluster_state.get_current_nodes()
        await self.node_table.update_nodes(nodes)
    
    async def _storage_loop(self):
        """Periodically store data to database."""
        while True:
            await asyncio.sleep(30)  # Store every 30 seconds
            nodes = await self.cluster_state.get_current_nodes()
            for node_name, node_data in nodes.items():
                await self.storage.store_node_data(node_name, node_data)
    
    async def on_unmount(self):
        """Clean up when app shuts down."""
        for task in self._collection_tasks:
            task.cancel()
        
        # Wait for tasks to finish cancellation
        await asyncio.gather(*self._collection_tasks, return_exceptions=True)