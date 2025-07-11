#!/usr/bin/env python3
"""
Backend configuration utility for dual-mode messaging system.
"""

import os
import sys

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def set_cif_only_mode():
    """Configure CIF-only mode"""
    print("🔧 Configuring CIF-only mode...")
    
    # Clear all dual-mode flags
    for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                 "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
        if flag in os.environ:
            del os.environ[flag]
    
    # Set CIF-only flags
    os.environ["MSGCIF_WRITES_ENABLED"] = "true"
    os.environ["MSGCIF_READS_ENABLED"] = "true"
    
    print("✅ CIF-only mode configured")
    show_current_config()


def set_db_only_mode():
    """Configure database-only mode"""
    print("🔧 Configuring database-only mode...")
    
    # Clear all dual-mode flags
    for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                 "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
        if flag in os.environ:
            del os.environ[flag]
    
    # Set database-only flags
    os.environ["MSGDB_WRITES_ENABLED"] = "true"
    os.environ["MSGDB_READS_ENABLED"] = "true"
    
    print("✅ Database-only mode configured")
    show_current_config()


def set_dual_write_cif_read():
    """Configure dual-write, CIF-read mode (Migration Phase 1)"""
    print("🔧 Configuring dual-write, CIF-read mode (Migration Phase 1)...")
    
    # Clear all dual-mode flags
    for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                 "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
        if flag in os.environ:
            del os.environ[flag]
    
    # Set dual-write, CIF-read flags
    os.environ["MSGDB_WRITES_ENABLED"] = "true"
    os.environ["MSGCIF_WRITES_ENABLED"] = "true"
    os.environ["MSGCIF_READS_ENABLED"] = "true"
    
    print("✅ Dual-write, CIF-read mode configured")
    show_current_config()


def set_dual_write_db_read():
    """Configure dual-write, DB-read mode (Migration Phase 2)"""
    print("🔧 Configuring dual-write, DB-read mode (Migration Phase 2)...")
    
    # Clear all dual-mode flags
    for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                 "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
        if flag in os.environ:
            del os.environ[flag]
    
    # Set dual-write, DB-read flags
    os.environ["MSGDB_WRITES_ENABLED"] = "true"
    os.environ["MSGDB_READS_ENABLED"] = "true"
    os.environ["MSGCIF_WRITES_ENABLED"] = "true"
    
    print("✅ Dual-write, DB-read mode configured")
    show_current_config()


def show_current_config():
    """Show current backend configuration"""
    try:
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        config = MessagingDatabaseConfig()
        
        db_writes = config.is_database_writes_enabled()
        db_reads = config.is_database_reads_enabled()
        cif_writes = config.is_cif_writes_enabled()
        cif_reads = config.is_cif_reads_enabled()
        
        print("\n📋 Current Configuration:")
        print(f"   DB writes:  {db_writes}")
        print(f"   DB reads:   {db_reads}")
        print(f"   CIF writes: {cif_writes}")
        print(f"   CIF reads:  {cif_reads}")
        
        # Determine mode
        if (db_writes or db_reads) and (cif_writes or cif_reads):
            mode = "🔄 Dual-mode"
        elif db_writes or db_reads:
            mode = "🗃️ Database-only"
        else:
            mode = "📄 CIF-only"
            
        print(f"   Mode: {mode}")
        
        # Show environment variables to set
        print("\n🔧 Environment Variables:")
        for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                     "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
            value = os.environ.get(flag, "unset")
            print(f"   {flag}: {value}")
        
        print("\n💡 To persist these settings, add to your shell profile:")
        for flag in ["MSGDB_WRITES_ENABLED", "MSGDB_READS_ENABLED", 
                     "MSGCIF_WRITES_ENABLED", "MSGCIF_READS_ENABLED"]:
            value = os.environ.get(flag)
            if value:
                print(f"   export {flag}={value}")
        
    except Exception as e:
        print(f"❌ Error showing configuration: {e}")


def show_migration_guide():
    """Show migration guide"""
    print("🛤️ Migration Guide:")
    print("=" * 50)
    print()
    print("📋 Phase 1: Dual-write, CIF-read")
    print("   python backend_config.py dual-write-cif-read")
    print("   → Write to both backends, read from CIF")
    print("   → Validate database writes while maintaining current reads")
    print()
    print("📋 Phase 2: Dual-write, DB-read") 
    print("   python backend_config.py dual-write-db-read")
    print("   → Write to both backends, read from database")
    print("   → Test database reads while maintaining CIF backup")
    print()
    print("📋 Phase 3: Database-only")
    print("   python backend_config.py db-only")
    print("   → Write and read from database only")
    print("   → Migration complete")
    print()
    print("🔄 Rollback: CIF-only")
    print("   python backend_config.py cif-only")
    print("   → Revert to traditional CIF-only operation")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: backend_config.py <command>")
        print("Commands:")
        print("  cif-only              - Configure CIF-only mode")
        print("  db-only               - Configure database-only mode")
        print("  dual-write-cif-read   - Configure dual-write, CIF-read")
        print("  dual-write-db-read    - Configure dual-write, DB-read")
        print("  show                  - Show current configuration")
        print("  migration-guide       - Show migration guide")
        sys.exit(1)

    command = sys.argv[1]

    if command == "cif-only":
        set_cif_only_mode()
    elif command == "db-only":
        set_db_only_mode()
    elif command == "dual-write-cif-read":
        set_dual_write_cif_read()
    elif command == "dual-write-db-read":
        set_dual_write_db_read()
    elif command == "show":
        show_current_config()
    elif command == "migration-guide":
        show_migration_guide()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
