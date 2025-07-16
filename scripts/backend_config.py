#!/usr/bin/env python3
"""
Simple Backend Configuration Script

This script shows how to configure the simplified messaging backend system.
No more complex dual-mode configuration - just a single environment variable!

Usage:
    # Use CIF files (default)
    python scripts/backend_config.py cif
    
    # Use database
    python scripts/backend_config.py database
    
    # Show current status
    python scripts/backend_config.py status
"""

import os
import sys
from pathlib import Path

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def set_backend_mode(mode: str):
    """Set the backend mode via environment variable"""
    if mode.lower() not in ["cif", "database"]:
        print(f"❌ Invalid mode '{mode}'. Must be 'cif' or 'database'")
        return False
    
    if mode.lower() == "cif":
        # Remove the environment variable to use default CIF mode
        if "WWPDB_MESSAGING_BACKEND" in os.environ:
            del os.environ["WWPDB_MESSAGING_BACKEND"]
        print("✅ Backend set to CIF mode (default)")
        print("   - Messages will be stored in CIF files")
        print("   - This is the traditional legacy mode")
    else:
        # Set environment variable for database mode
        os.environ["WWPDB_MESSAGING_BACKEND"] = "database"
        print("✅ Backend set to database mode")
        print("   - Messages will be stored in the database")
        print("   - Requires database configuration")
    
    return True


def show_status():
    """Show current backend configuration"""
    current_mode = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif")
    
    print("📊 Current Backend Configuration")
    print("=" * 40)
    print(f"Backend Mode: {current_mode.upper()}")
    print(f"Environment Variable: WWPDB_MESSAGING_BACKEND={os.environ.get('WWPDB_MESSAGING_BACKEND', 'not set')}")
    
    if current_mode.lower() == "database":
        print("\n🗃️ Database Mode Active")
        print("   - Messages stored in database")
        print("   - Requires MSGDB_* environment variables")
        
        # Check database configuration
        db_vars = ["MSGDB_HOST", "MSGDB_NAME", "MSGDB_USER", "MSGDB_PASS"]
        print("\n📋 Database Configuration:")
        for var in db_vars:
            value = os.environ.get(var, "not set")
            if var == "MSGDB_PASS" and value != "not set":
                value = "***"  # Hide password
            print(f"   {var}: {value}")
    else:
        print("\n📄 CIF Mode Active")
        print("   - Messages stored in CIF files")
        print("   - Traditional legacy mode")
        print("   - No additional configuration required")


def test_backend_selection():
    """Test the simplified backend selection"""
    try:
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Create a mock request object
        class MockReqObj:
            def getValue(self, key):
                return None
        
        req_obj = MockReqObj()
        
        # Test backend selection
        info = MessagingFactory.get_backend_info(req_obj)
        
        print("\n🧪 Backend Selection Test")
        print("=" * 30)
        print(f"Selected Backend: {info['selected_backend']}")
        print(f"Backend Class: {info['backend_class']}")
        print(f"Reason: {info['reason']}")
        
    except Exception as e:
        print(f"\n❌ Backend test failed: {e}")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        return
    
    command = sys.argv[1].lower()
    
    if command in ["cif", "database"]:
        if set_backend_mode(command):
            print(f"\n💡 To persist this setting, add to your environment:")
            if command == "database":
                print(f"   export WWPDB_MESSAGING_BACKEND=database")
            else:
                print(f"   unset WWPDB_MESSAGING_BACKEND  # (uses CIF default)")
    
    elif command == "status":
        show_status()
        test_backend_selection()
    
    else:
        print(f"❌ Unknown command: {command}")
        print(__doc__)


if __name__ == "__main__":
    main()
