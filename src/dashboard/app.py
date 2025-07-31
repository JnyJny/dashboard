"""Main Textual application class."""

from textual.app import App
from textual.containers import Container
from textual.widgets import Header, Footer

from dashboard.widgets.node_table import NodeTable
from dashboard.models.cluster_state import ClusterState


class DashboardApp(App):
    """Main dashboard application."""
    
    CSS_PATH = "dashboard.tcss"
    TITLE = "Cluster Monitor"
    
    def __init__(self):
        super().__init__()
        self.cluster_state = ClusterState()
        self.node_table = NodeTable()
    
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
        # Start background data collection tasks
        self.set_interval(2.0, self.refresh_display)
    
    async def refresh_display(self):
        """Update the display with current data."""
        nodes = await self.cluster_state.get_current_nodes()
        await self.node_table.update_nodes(nodes)