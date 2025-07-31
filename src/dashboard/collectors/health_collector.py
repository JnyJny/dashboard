"""Node health metrics collector."""

import asyncio
import aiohttp
from typing import Dict, Any
from prometheus_client.parser import text_string_to_metric_families
from dashboard.models.cluster_state import ClusterState


class HealthCollector:
    """Collects health metrics from node exporters."""
    
    def __init__(self, cluster_state: ClusterState, node_port: int = 9100):
        self.cluster_state = cluster_state
        self.node_port = node_port
    
    async def fetch_node_health(self, node: str) -> Dict[str, Any]:
        """Fetch health metrics from a single node."""
        try:
            url = f"http://{node}:{self.node_port}/metrics"
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(5)) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    text = await response.text()
                    return self._parse_health_metrics(text)
        except Exception as error:
            print(f"Failed to fetch health from {node}: {error}")
            return {}
    
    def _parse_health_metrics(self, metrics_text: str) -> Dict[str, Any]:
        """Parse node health metrics from Prometheus format."""
        metrics = {}
        
        for family in text_string_to_metric_families(metrics_text):
            if family.name == 'node_cpu_seconds_total':
                for sample in family.samples:
                    if sample.labels.get('mode') == 'idle':
                        metrics['cpu_idle'] = sample.value
            
            elif family.name == 'node_memory_MemTotal_bytes':
                for sample in family.samples:
                    metrics['memory_total'] = sample.value
            
            elif family.name == 'node_memory_MemAvailable_bytes':
                for sample in family.samples:
                    metrics['memory_available'] = sample.value
            
            elif family.name == 'node_load1':
                for sample in family.samples:
                    metrics['load_avg'] = sample.value
            
            elif family.name == 'node_time_seconds':
                for sample in family.samples:
                    metrics['uptime_seconds'] = sample.value
        
        # Calculate derived metrics
        if 'memory_total' in metrics and 'memory_available' in metrics:
            used = metrics['memory_total'] - metrics['memory_available']
            metrics['memory_percent'] = (used / metrics['memory_total']) * 100
        
        # CPU percentage would need more complex calculation from multiple samples
        # For now, using a placeholder
        metrics['cpu_percent'] = 0.0  # TODO: Implement proper CPU calculation
        
        return metrics
    
    async def collect_all_health(self, nodes: set[str]) -> Dict[str, Dict[str, Any]]:
        """Collect health metrics from all nodes concurrently."""
        tasks = [self.fetch_node_health(node) for node in nodes]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        health_data = {}
        for node, result in zip(nodes, results):
            if isinstance(result, Exception):
                await self.cluster_state.mark_node_unreachable(node)
            else:
                health_data[node] = result
        
        return health_data
    
    async def start_collection(self):
        """Start the health metrics collection loop."""
        while True:
            nodes = await self.cluster_state.get_node_list()
            if nodes:
                health_data = await self.collect_all_health(nodes)
                for node, data in health_data.items():
                    await self.cluster_state.update_health_data(node, data)
            await asyncio.sleep(30)