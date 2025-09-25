#!/usr/bin/env python3
"""
Simple startup script for Cloud Inventory MCP Server
"""

import asyncio
import sys
import argparse
from pathlib import Path

# Add the backend directory to the path
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

async def start_mcp_server():
    """Start the MCP server"""
    print("🚀 Starting Cloud Inventory MCP Server...")
    
    try:
        from cloud_mcp.server import main
        main()
        
    except KeyboardInterrupt:
        print("\n⏹️ MCP Server stopped by user")
    except Exception as e:
        print(f"❌ MCP Server failed to start: {e}")
        sys.exit(1)

async def test_mcp_server():
    """Test the MCP server"""
    print("🧪 Running MCP Server Tests...")
    
    try:
        # Import the MCP instance to test basic functionality
        from cloud_mcp.server import mcp
        
        # Check that we can import the main functions
        from cloud_mcp.server import query_logs, get_table_stats, archive_records
        
        # Count registered tools using the correct FastMCP API (async)
        tools = await mcp.get_tools()
        tools_count = len(tools)
        
        if tools_count >= 5:  # We should have at least 5 tools registered
            print(f"✅ Health check passed! MCP server initialized with {tools_count} tools")
            print(f"✅ Registered tools: {list(tools.keys())}")
            return True
        else:
            print(f"❌ Health check failed: Expected 5+ tools, found {tools_count}")
            print(f"Tools found: {list(tools.keys()) if tools else 'None'}")
            return False
            
    except Exception as e:
        print(f"❌ Test execution failed: {e}")
        return False

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description="Cloud Inventory MCP Server")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Start server command
    start_parser = subparsers.add_parser('start', help='Start the MCP server')
    
    # Test command
    test_parser = subparsers.add_parser('test', help='Run MCP server health check')
    
    args = parser.parse_args()
    
    if args.command == 'start':
        asyncio.run(start_mcp_server())
        
    elif args.command == 'test':
        success = asyncio.run(test_mcp_server())
        sys.exit(0 if success else 1)
    
    else:
        parser.print_help()
        print(f"\nExample usage:")
        print(f"  python mcp_server.py start     # Start MCP server")
        print(f"  python mcp_server.py test      # Run health check")

if __name__ == "__main__":
    main()