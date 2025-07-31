"""Main entry point for the dashboard application."""

import asyncio
from dashboard.app import DashboardApp


async def main():
    """Run the dashboard application."""
    app = DashboardApp()
    await app.run_async()


if __name__ == "__main__":
    asyncio.run(main())