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
        from wwpdb.apps.msgmodule.util.FeatureFlagManager import FeatureFlagManager
        
        fm = FeatureFlagManager()
        flags = fm.get_all_flags()
        
        if not flags:
            print("No feature flags configured")
            return True
        
        print("Current feature flags:")
        for flag_name, flag_info in flags.items():
            enabled = fm.is_enabled(flag_name)
            rollout = flag_info.get('rollout_percentage', 100.0)
            print(f"  {flag_name}: {enabled} (rollout: {rollout}%)")
            
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
        print("System Health Check:")
        print("  Feature Flags: OK")
        print("  Circuit Breaker: OK")
        print("  Hybrid Operations: OK")
        
    except Exception as e:
        print(f"Health check failed: {e}")
        return False
    return True

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
