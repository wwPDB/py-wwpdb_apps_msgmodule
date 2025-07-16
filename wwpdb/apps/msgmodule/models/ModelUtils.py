"""
Model Conversion Utilities for wwPDB Communication Module

This module provides utility functions for converting between different
model representations (SQLAlchemy ORM models, dataclasses, and dictionaries).
These utilities enable seamless interoperability between database and
application layers.
"""

from typing import Any, Dict, Type, TypeVar, Optional

# Type variables for generic functions
T = TypeVar('T')
M = TypeVar('M')


def model_to_dataclass(model_instance: M, dataclass_type: Type[T]) -> Optional[T]:
    """Convert SQLAlchemy model instance to dataclass
    
    Args:
        model_instance: SQLAlchemy model instance
        dataclass_type: Target dataclass type
        
    Returns:
        Dataclass instance or None if model_instance is None
    """
    if not model_instance:
        return None
    
    # Get all fields from the dataclass
    model_data = {}
    for field_name in dataclass_type.__dataclass_fields__.keys():
        if hasattr(model_instance, field_name):
            model_data[field_name] = getattr(model_instance, field_name)
    
    return dataclass_type(**model_data)


def dataclass_to_model_data(dataclass_instance: T) -> Dict[str, Any]:
    """Convert dataclass to dict for model creation
    
    Args:
        dataclass_instance: Dataclass instance to convert
        
    Returns:
        Dictionary suitable for SQLAlchemy model creation
    """
    # Exclude None/empty id fields for auto-increment
    data = {}
    for field, value in dataclass_instance.__dict__.items():
        if field == 'id' and (value is None or value == 0):
            continue
        data[field] = value
    return data


def dataclass_to_model(dataclass_instance: T, model_class: Type[M]) -> M:
    """Convert dataclass to SQLAlchemy model instance
    
    Args:
        dataclass_instance: Dataclass instance to convert
        model_class: Target SQLAlchemy model class
        
    Returns:
        SQLAlchemy model instance
    """
    data = {}
    for field, value in dataclass_instance.__dict__.items():
        if field == 'id' and (value is None or value == 0):
            continue
        if hasattr(model_class, field):
            data[field] = value
    return model_class(**data)


def dict_to_dataclass(data_dict: Dict[str, Any], dataclass_type: Type[T]) -> T:
    """Convert dictionary to dataclass instance
    
    Args:
        data_dict: Dictionary with data
        dataclass_type: Target dataclass type
        
    Returns:
        Dataclass instance
    """
    # Filter dict to only include fields that exist in the dataclass
    valid_fields = {}
    for field_name in dataclass_type.__dataclass_fields__.keys():
        if field_name in data_dict:
            valid_fields[field_name] = data_dict[field_name]
    
    return dataclass_type(**valid_fields)


def dataclass_to_dict(dataclass_instance: T, exclude_none: bool = False) -> Dict[str, Any]:
    """Convert dataclass to dictionary
    
    Args:
        dataclass_instance: Dataclass instance to convert
        exclude_none: Whether to exclude None values
        
    Returns:
        Dictionary representation
    """
    data = dataclass_instance.__dict__.copy()
    
    if exclude_none:
        data = {k: v for k, v in data.items() if v is not None}
    
    return data


def update_dataclass_from_dict(dataclass_instance: T, updates: Dict[str, Any]) -> T:
    """Update dataclass instance with values from dictionary
    
    Args:
        dataclass_instance: Dataclass instance to update
        updates: Dictionary with field updates
        
    Returns:
        New dataclass instance with updated values
    """
    current_data = dataclass_to_dict(dataclass_instance)
    
    # Apply updates only for fields that exist in the dataclass
    for field_name in dataclass_instance.__dataclass_fields__.keys():
        if field_name in updates:
            current_data[field_name] = updates[field_name]
    
    return type(dataclass_instance)(**current_data)
