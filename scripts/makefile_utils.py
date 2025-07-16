#!/usr/bin/env python3
"""
Quick utility scripts for Makefile commands
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def show_backend_status():
    """Show current backend configuration status"""
    try:
        print("🔧 Backend Configuration Status:")
        print("=" * 35)
        
        backend = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif")
        print(f"  WWPDB_MESSAGING_BACKEND: {backend}")
        
        if backend == "database":
            mode = "🗃️ Database Backend"
            print(f"\n  Current Mode: {mode}")
            print("  Storage: Relational database")
        else:
            mode = "📄 CIF Backend"
            print(f"\n  Current Mode: {mode}")
            print("  Storage: CIF files")

    except Exception as e:
        print(f"Error: {e}")
        return False
    return True


def check_health():
    """Perform basic health checks"""
    try:
        print("🏥 System Health Check")
        print("=" * 22)
        
        # Check Python environment
        print(f"  Python: ✅ OK ({sys.version.split()[0]})")
        
        # Check package import
        try:
            import wwpdb.apps.msgmodule
            print("  Package Import: ✅ OK")
        except ImportError as e:
            print(f"  Package Import: ❌ FAILED - {e}")
            return False
        
        # Check backend configuration
        backend = os.environ.get("WWPDB_MESSAGING_BACKEND", "cif")
        print(f"  Backend Config: ✅ OK ({backend})")
        
        # Test MessagingFactory
        try:
            from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
            factory = MessagingFactory()
            # Just test that we can get backend info without requiring a real req_obj
            info = factory.get_backend_info()
            print("  MessagingFactory: ✅ OK")
        except Exception as e:
            print(f"  MessagingFactory: ⚠️  WARNING - {e}")
        
        return True

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: makefile_utils.py <command> [args...]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "backend-status":
        success = show_backend_status()
    elif command == "health":
        success = check_health()
    else:
        print(f"Unknown command: {command}")
        success = False

    sys.exit(0 if success else 1)
