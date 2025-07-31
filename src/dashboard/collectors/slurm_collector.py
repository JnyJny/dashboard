"""Slurm metrics collector."""

import asyncio
import aiohttp
from typing import Dict
from prometheus_client.parser import text_string_to_metric_families
from dashboard.models.cluster_state import ClusterState


class SlurmCollector:
    """Collects node status from Slurm controller."""
    
    def __init__(self, controller_url: str, cluster_state: ClusterState):
        self.controller_url = controller_url
        self.cluster_state = cluster_state
    
    async def fetch_slurm_metrics(self) -> Dict[str, str]:
        """Fetch and parse Slurm node metrics."""
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(5)) as session:
                async with session.get(f"{self.controller_url}/metrics") as response:
                    response.raise_for_status()
                    text = await response.text()
                    return self._parse_slurm_metrics(text)
        except Exception as error:
            print(f"Failed to fetch Slurm metrics: {error}")
            return {}
    
    def _parse_slurm_metrics(self, metrics_text: str) -> Dict[str, str]:
        """Parse Slurm node status from Prometheus metrics."""
        nodes = {}
        
        for family in text_string_to_metric_families(metrics_text):
            if family.name == 'slurm_node_cpu_total':
                for sample in family.samples:
                    labels = sample.labels
                    node_name = labels.get('node')
                    state = labels.get('state')
                    
                    if node_name and state:
                        nodes[node_name] = state
        
        return nodes
    
    async def start_collection(self):
        """Start the Slurm metrics collection loop."""
        while True:
            node_statuses = await self.fetch_slurm_metrics()
            for node, status in node_statuses.items():
                await self.cluster_state.update_slurm_status(node, status)
            await asyncio.sleep(10)