#!/usr/bin/env python
"""
Feature flag management for hybrid messaging operations.

This module provides centralized feature flag management for controlling
hybrid messaging behavior, enabling safe rollouts and quick rollbacks.

Author: wwPDB Migration Team
Date: July 2025
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Set, List
from enum import Enum
from dataclasses import dataclass, asdict
from datetime import datetime


class FeatureFlagScope(Enum):
    """Defines the scope of feature flag application"""
    GLOBAL = "global"
    SITE_SPECIFIC = "site_specific"
    DEPOSITION_SPECIFIC = "deposition_specific"
    USER_SPECIFIC = "user_specific"


@dataclass
class FeatureFlag:
    """Represents a feature flag configuration"""
    name: str
    enabled: bool
    scope: FeatureFlagScope
    description: str
    default_value: Any = None
    valid_values: Optional[Set[Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    rollout_percentage: float = 100.0
    target_groups: Optional[Set[str]] = None


class FeatureFlagManager:
    """
    Manages feature flags for hybrid messaging operations.
    
    Supports hierarchical configuration with environment variables,
    configuration files, and runtime overrides.
    """
    
    def __init__(self, config_file: Optional[str] = None, site_id: str = "RCSB"):
        """
        Initialize feature flag manager.
        
        Args:
            config_file: Optional path to feature flag configuration file
            site_id: Site identifier for site-specific flags
        """
        self.site_id = site_id
        self.logger = logging.getLogger(__name__)
        
        # Initialize default flags
        self._flags = self._initialize_default_flags()
        
        # Load from configuration file if provided
        if config_file and os.path.exists(config_file):
            self._load_from_file(config_file)
        
        # Override with environment variables
        self._load_from_environment()
        
        self.logger.info(f"FeatureFlagManager initialized with {len(self._flags)} flags")
    
    def _initialize_default_flags(self) -> Dict[str, FeatureFlag]:
        """Initialize default feature flags for hybrid messaging"""
        flags = {}
        
        # Write strategy flags
        flags['hybrid_dual_write'] = FeatureFlag(
            name='hybrid_dual_write',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable dual-write to both CIF and database',
            default_value=False,
            rollout_percentage=0.0
        )
        
        flags['hybrid_db_primary'] = FeatureFlag(
            name='hybrid_db_primary',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Use database as primary with CIF fallback',
            default_value=False,
            rollout_percentage=0.0
        )
        
        flags['hybrid_db_only'] = FeatureFlag(
            name='hybrid_db_only',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Write to database only (no CIF)',
            default_value=False,
            rollout_percentage=0.0
        )
        
        # Validation and monitoring flags
        flags['consistency_checks'] = FeatureFlag(
            name='consistency_checks',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable data consistency validation between backends',
            default_value=True
        )
        
        flags['performance_metrics'] = FeatureFlag(
            name='performance_metrics',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable performance metrics collection',
            default_value=True
        )
        
        flags['detailed_logging'] = FeatureFlag(
            name='detailed_logging',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable detailed operation logging',
            default_value=False
        )
        
        # Circuit breaker and resilience flags
        flags['circuit_breaker'] = FeatureFlag(
            name='circuit_breaker',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable circuit breaker for database operations',
            default_value=True
        )
        
        flags['auto_failover'] = FeatureFlag(
            name='auto_failover',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable automatic failover to CIF on database failures',
            default_value=True
        )
        
        # Migration-specific flags
        flags['migration_mode'] = FeatureFlag(
            name='migration_mode',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable migration mode for data transition',
            default_value=False
        )
        
        flags['read_from_db'] = FeatureFlag(
            name='read_from_db',
            enabled=False,
            scope=FeatureFlagScope.GLOBAL,
            description='Read messages from database instead of CIF',
            default_value=False,
            rollout_percentage=0.0
        )
        
        # Performance tuning flags
        flags['batch_operations'] = FeatureFlag(
            name='batch_operations',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable batch operations for better performance',
            default_value=True
        )
        
        flags['connection_pooling'] = FeatureFlag(
            name='connection_pooling',
            enabled=True,
            scope=FeatureFlagScope.GLOBAL,
            description='Enable database connection pooling',
            default_value=True
        )
        
        return flags
    
    def _load_from_file(self, config_file: str):
        """Load feature flags from configuration file"""
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            for flag_name, flag_config in config_data.get('flags', {}).items():
                if flag_name in self._flags:
                    # Update existing flag
                    existing_flag = self._flags[flag_name]
                    existing_flag.enabled = flag_config.get('enabled', existing_flag.enabled)
                    existing_flag.rollout_percentage = flag_config.get('rollout_percentage', existing_flag.rollout_percentage)
                    existing_flag.target_groups = set(flag_config.get('target_groups', [])) if flag_config.get('target_groups') else existing_flag.target_groups
                    existing_flag.updated_at = datetime.now()
                else:
                    # Create new flag from config
                    self._flags[flag_name] = FeatureFlag(
                        name=flag_name,
                        enabled=flag_config.get('enabled', False),
                        scope=FeatureFlagScope(flag_config.get('scope', 'global')),
                        description=flag_config.get('description', ''),
                        default_value=flag_config.get('default_value'),
                        rollout_percentage=flag_config.get('rollout_percentage', 100.0),
                        target_groups=set(flag_config.get('target_groups', [])) if flag_config.get('target_groups') else None,
                        created_at=datetime.now()
                    )
                    
            self.logger.info(f"Loaded feature flags from {config_file}")
            
        except Exception as e:
            self.logger.error(f"Failed to load feature flags from {config_file}: {e}")
    
    def _load_from_environment(self):
        """Load feature flag overrides from environment variables"""
        env_prefix = "MSGDB_FLAG_"
        
        for env_var, value in os.environ.items():
            if env_var.startswith(env_prefix):
                flag_name = env_var[len(env_prefix):].lower()
                
                if flag_name in self._flags:
                    # Parse boolean value
                    if value.lower() in ('true', '1', 'yes', 'on'):
                        self._flags[flag_name].enabled = True
                    elif value.lower() in ('false', '0', 'no', 'off'):
                        self._flags[flag_name].enabled = False
                    else:
                        # Try to parse as rollout percentage
                        try:
                            percentage = float(value)
                            if 0 <= percentage <= 100:
                                self._flags[flag_name].rollout_percentage = percentage
                        except ValueError:
                            self.logger.warning(f"Invalid value for {env_var}: {value}")
                    
                    self._flags[flag_name].updated_at = datetime.now()
                    self.logger.info(f"Updated flag {flag_name} from environment: {value}")
    
    def is_enabled(self, flag_name: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """
        Check if a feature flag is enabled.
        
        Args:
            flag_name: Name of the feature flag
            context: Optional context for scope-specific evaluation
            
        Returns:
            bool: True if flag is enabled
        """
        if flag_name not in self._flags:
            self.logger.warning(f"Unknown feature flag: {flag_name}")
            return False
        
        flag = self._flags[flag_name]
        
        # Check basic enabled status
        if not flag.enabled:
            return False
        
        # Check rollout percentage
        if flag.rollout_percentage < 100.0:
            # Simple hash-based rollout (deterministic per deposition/user)
            if context:
                hash_input = context.get('deposition_id', context.get('user_id', 'default'))
                import hashlib
                hash_value = int(hashlib.md5(hash_input.encode()).hexdigest()[:8], 16)
                rollout_threshold = (hash_value % 100) + 1
                if rollout_threshold > flag.rollout_percentage:
                    return False
        
        # Check target groups
        if flag.target_groups and context:
            user_groups = set(context.get('user_groups', []))
            site_groups = {self.site_id}
            
            if not (user_groups & flag.target_groups or site_groups & flag.target_groups):
                return False
        
        return True
    
    def get_flag_value(self, flag_name: str, default: Any = None, context: Optional[Dict[str, Any]] = None) -> Any:
        """
        Get the value of a feature flag.
        
        Args:
            flag_name: Name of the feature flag
            default: Default value if flag not found or disabled
            context: Optional context for evaluation
            
        Returns:
            Any: Flag value or default
        """
        if self.is_enabled(flag_name, context):
            flag = self._flags.get(flag_name)
            return flag.default_value if flag and flag.default_value is not None else True
        
        return default
    
    def set_flag(self, flag_name: str, enabled: bool, rollout_percentage: float = 100.0):
        """
        Set a feature flag value at runtime.
        
        Args:
            flag_name: Name of the feature flag
            enabled: Whether the flag is enabled
            rollout_percentage: Percentage rollout (0-100)
        """
        if flag_name in self._flags:
            self._flags[flag_name].enabled = enabled
            self._flags[flag_name].rollout_percentage = rollout_percentage
            self._flags[flag_name].updated_at = datetime.now()
            self.logger.info(f"Updated flag {flag_name}: enabled={enabled}, rollout={rollout_percentage}%")
        else:
            # Create new dynamic flag
            self._flags[flag_name] = FeatureFlag(
                name=flag_name,
                enabled=enabled,
                scope=FeatureFlagScope.GLOBAL,
                description=f"Dynamically created flag: {flag_name}",
                rollout_percentage=rollout_percentage,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            self.logger.info(f"Created new dynamic flag {flag_name}: enabled={enabled}, rollout={rollout_percentage}%")
    
    def enable_flag(self, flag_name: str, rollout_percentage: float = 100.0):
        """
        Enable a feature flag.
        
        Args:
            flag_name: Name of the feature flag
            rollout_percentage: Percentage rollout (0-100)
        """
        self.set_flag(flag_name, True, rollout_percentage)
    
    def disable_flag(self, flag_name: str):
        """
        Disable a feature flag.
        
        Args:
            flag_name: Name of the feature flag
        """
        self.set_flag(flag_name, False, 0.0)
    
    def get_all_flags(self) -> Dict[str, Dict[str, Any]]:
        """Get all feature flags and their current status"""
        return {name: asdict(flag) for name, flag in self._flags.items()}
    
    def export_config(self, file_path: str):
        """Export current feature flag configuration to file"""
        try:
            config = {
                'metadata': {
                    'site_id': self.site_id,
                    'exported_at': datetime.now().isoformat(),
                    'version': '1.0'
                },
                'flags': {name: asdict(flag) for name, flag in self._flags.items()}
            }
            
            with open(file_path, 'w') as f:
                json.dump(config, f, indent=2, default=str)
                
            self.logger.info(f"Feature flag configuration exported to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to export feature flag configuration: {e}")
    
    def get_strategy_flags(self) -> Dict[str, bool]:
        """Get the current state of all strategy-related flags"""
        strategy_flags = [
            'hybrid_dual_write',
            'hybrid_db_primary', 
            'hybrid_db_only'
        ]
        
        return {flag: self.is_enabled(flag) for flag in strategy_flags}
    
    def get_recommended_write_strategy(self, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Get the recommended write strategy based on current flags.
        
        Args:
            context: Optional context for evaluation
            
        Returns:
            str: Recommended write strategy
        """
        if self.is_enabled('hybrid_db_only', context):
            return 'db_only'
        elif self.is_enabled('hybrid_db_primary', context):
            return 'db_primary_cif_fallback'
        elif self.is_enabled('hybrid_dual_write', context):
            return 'dual_write'
        else:
            return 'cif_only'


class FeatureFlagContext:
    """Helper class to build context for feature flag evaluation"""
    
    def __init__(self):
        self._context = {}
    
    def with_deposition(self, deposition_id: str) -> 'FeatureFlagContext':
        """Add deposition ID to context"""
        self._context['deposition_id'] = deposition_id
        return self
    
    def with_user(self, user_id: str, user_groups: Optional[List[str]] = None) -> 'FeatureFlagContext':
        """Add user information to context"""
        self._context['user_id'] = user_id
        if user_groups:
            self._context['user_groups'] = user_groups
        return self
    
    def with_site(self, site_id: str) -> 'FeatureFlagContext':
        """Add site ID to context"""
        self._context['site_id'] = site_id
        return self
    
    def build(self) -> Dict[str, Any]:
        """Build the context dictionary"""
        return dict(self._context)


# Global feature flag manager instance
_global_flag_manager: Optional[FeatureFlagManager] = None


def get_feature_flag_manager(site_id: str = "RCSB") -> FeatureFlagManager:
    """Get the global feature flag manager instance"""
    global _global_flag_manager
    
    if _global_flag_manager is None:
        config_file = os.getenv('MSGDB_FEATURE_FLAGS_CONFIG')
        _global_flag_manager = FeatureFlagManager(config_file=config_file, site_id=site_id)
    
    return _global_flag_manager


def is_feature_enabled(flag_name: str, context: Optional[Dict[str, Any]] = None, site_id: str = "RCSB") -> bool:
    """Convenience function to check if a feature is enabled"""
    manager = get_feature_flag_manager(site_id)
    return manager.is_enabled(flag_name, context)
