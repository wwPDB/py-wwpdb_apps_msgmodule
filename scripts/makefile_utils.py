#!/usr/bin/env python3
"""
Quick utility scripts for Makefile commands
"""

import sys
import os

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def show_feature_flags():
    """Show current feature flag status"""
    try:
        # Show new dual-mode feature flags
        print("🔧 Dual-Mode Backend Feature Flags:")
        print("=" * 40)
        
        from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
        config = MessagingDatabaseConfig()
        
        db_writes = config.is_database_writes_enabled()
        db_reads = config.is_database_reads_enabled()
        cif_writes = config.is_cif_writes_enabled()
        cif_reads = config.is_cif_reads_enabled()
        
        print(f"  MSGDB_WRITES_ENABLED:  {db_writes}")
        print(f"  MSGDB_READS_ENABLED:   {db_reads}")
        print(f"  MSGCIF_WRITES_ENABLED: {cif_writes}")
        print(f"  MSGCIF_READS_ENABLED:  {cif_reads}")
        
        # Determine mode
        if (db_writes or db_reads) and (cif_writes or cif_reads):
            mode = "🔄 Dual-mode"
            write_targets = []
            if db_writes:
                write_targets.append("database")
            if cif_writes:
                write_targets.append("CIF")
            read_source = "database" if db_reads else "CIF"
            
            print(f"\n  Current Mode: {mode}")
            print(f"  Writes to: {', '.join(write_targets) if write_targets else 'none'}")
            print(f"  Reads from: {read_source}")
        elif db_writes or db_reads:
            mode = "🗃️ Database-only"
            print(f"\n  Current Mode: {mode}")
        else:
            mode = "📄 CIF-only"
            print(f"\n  Current Mode: {mode}")
        
        # Show legacy feature flags if available
        print("\n🏗️ Legacy Feature Flags:")
        print("=" * 25)
        try:
            from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager
            fm = FeatureFlagManager()
            flags = fm.get_all_flags()

            if not flags:
                print("  No legacy feature flags configured")
            else:
                for flag_name, flag_info in flags.items():
                    enabled = fm.is_enabled(flag_name)
                    rollout = flag_info.get("rollout_percentage", 100.0)
                    print(f"  {flag_name}: {enabled} (rollout: {rollout}%)")
        except Exception as e:
            print(f"  Legacy feature flags unavailable: {e}")

    except Exception as e:
        print(f"Error: {e}")
        return False
    return True


def enable_feature_flag(flag_name):
    """Enable a feature flag"""
    try:
        from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager

        fm = FeatureFlagManager()
        fm.enable_flag(flag_name)
        print(f"Flag '{flag_name}' enabled")

    except Exception as e:
        print(f"Error enabling flag '{flag_name}': {e}")
        return False
    return True


def disable_feature_flag(flag_name):
    """Disable a feature flag"""
    try:
        from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager

        fm = FeatureFlagManager()
        fm.disable_flag(flag_name)
        print(f"Flag '{flag_name}' disabled")

    except Exception as e:
        print(f"Error disabling flag '{flag_name}': {e}")
        return False
    return True


def check_health():
    """Check system health"""
    try:
        print("🏥 System Health Check:")
        print("=" * 25)
        
        # Check dual-mode feature flags
        try:
            from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
            config = MessagingDatabaseConfig()
            
            db_writes = config.is_database_writes_enabled()
            db_reads = config.is_database_reads_enabled()
            cif_writes = config.is_cif_writes_enabled()
            cif_reads = config.is_cif_reads_enabled()
            
            # Determine if configuration is valid
            has_writes = db_writes or cif_writes
            has_reads = db_reads or cif_reads
            
            if has_writes and has_reads:
                print("  Backend Configuration: ✅ OK")
                if (db_writes or db_reads) and (cif_writes or cif_reads):
                    print("    Mode: Dual-mode")
                elif db_writes or db_reads:
                    print("    Mode: Database-only")
                else:
                    print("    Mode: CIF-only")
            else:
                print("  Backend Configuration: ⚠️  WARNING - No read or write backends enabled")
                return False
                
        except Exception as e:
            print(f"  Backend Configuration: ❌ ERROR - {e}")
            return False
        
        # Check database connection if database operations are enabled
        try:
            from wwpdb.apps.msgmodule.db.config import MessagingDatabaseConfig
            config = MessagingDatabaseConfig()
            
            if config.is_database_writes_enabled() or config.is_database_reads_enabled():
                from wwpdb.apps.msgmodule.db import get_messaging_database_config
                db_config = get_messaging_database_config()
                print("  Database Connection: ✅ OK")
                print(f"    Host: {db_config.get('host', 'unknown')}")
                print(f"    Database: {db_config.get('database', 'unknown')}")
            else:
                print("  Database Connection: ⏭️  Skipped (database not enabled)")
        except Exception as e:
            print(f"  Database Connection: ⚠️  WARNING - {e}")
        
        # Check legacy feature flags
        try:
            from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager
            fm = FeatureFlagManager()
            flags = fm.get_all_flags()
            print(f"  Legacy Feature Flags: ✅ OK ({len(flags)} flags active)")
        except Exception as e:
            print(f"  Legacy Feature Flags: ⚠️  WARNING - {e}")
        
        # Check circuit breaker
        try:
            from wwpdb.apps.msgmodule.util.CircuitBreaker import get_database_circuit_breaker
            breaker = get_database_circuit_breaker()
            print(f"  Circuit Breaker: ✅ OK (state: {breaker.state.name})")
        except Exception as e:
            print(f"  Circuit Breaker: ⚠️  WARNING - {e}")
        
        return True

    except Exception as e:
        print(f"❌ Health check failed: {e}")
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: makefile_utils.py <command> [args...]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "feature-flags":
        success = show_feature_flags()
    elif command == "enable-flag" and len(sys.argv) > 2:
        success = enable_feature_flag(sys.argv[2])
    elif command == "disable-flag" and len(sys.argv) > 2:
        success = disable_feature_flag(sys.argv[2])
    elif command == "health":
        success = check_health()
    else:
        print(f"Unknown command: {command}")
        success = False

    sys.exit(0 if success else 1)
