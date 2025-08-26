#!/usr/bin/env python3
"""
Database-aware PdbxMessageCategoryStyle implementation.

This replaces mmcif_utils.style.PdbxMessageCategoryStyle with a version that
derives schema information from our SQLAlchemy database models instead of
hardcoded style definitions.
"""

__docformat__ = "restructuredtext en"
__author__ = "wwPDB Database Migration Team"
__email__ = "help@wwpdb.org"
__license__ = "Apache 2.0"

import sys
from typing import List, Dict, Any, Tuple, Optional

try:
    from wwpdb.apps.msgmodule.db.Models import MessageInfo, MessageFileReference, MessageStatus
    from sqlalchemy import inspect
except ImportError:
    MessageInfo = None
    MessageFileReference = None  
    MessageStatus = None
    inspect = None


class PdbxMessageCategoryStyle:
    """
    Database-aware message category style provider.
    
    This class provides the same interface as mmcif_utils.style.PdbxMessageCategoryStyle
    but derives schema information from our SQLAlchemy database models instead of
    hardcoded definitions.
    """
    
    # Style ID for compatibility
    _styleId = "PDBX_DEPOSITION_MESSAGE_INFO_DB_V1"
    
    # Category mappings: database model -> CIF category name
    _MODEL_TO_CATEGORY = {
        'MessageInfo': 'pdbx_deposition_message_info',
        'MessageFileReference': 'pdbx_deposition_message_file_reference', 
        'MessageStatus': 'pdbx_deposition_message_status'
    }
    
    # Reverse mapping: CIF category -> database model
    _CATEGORY_TO_MODEL = {v: k for k, v in _MODEL_TO_CATEGORY.items()}
    
    def __init__(self, verbose: bool = False, log = sys.stderr):
        """Initialize database-aware style provider"""
        self.__verbose = verbose
        self.__lfh = log
        self.__debug = False
        
        # Cache for attribute lists
        self.__attributeCache: Dict[str, List[str]] = {}
        
        # Initialize category info from database models
        self.__categoryInfo = self._buildCategoryInfo()
        self.__itemDict = self._buildItemDict()
    
    def _buildCategoryInfo(self) -> List[Tuple[str, str]]:
        """Build category information from database models"""
        return [
            ('pdbx_deposition_message_info', 'table'),
            ('pdbx_deposition_message_file_reference', 'table'),
            ('pdbx_deposition_message_origcomm_reference', 'table'),  # For compatibility
            ('pdbx_deposition_message_status', 'table')
        ]
    
    def _buildItemDict(self) -> Dict[str, List[Tuple[str, str, str, str]]]:
        """Build item dictionary from SQLAlchemy models"""
        itemDict = {}
        
        if MessageInfo is not None and inspect is not None:
            # Map MessageInfo to pdbx_deposition_message_info
            itemDict['pdbx_deposition_message_info'] = self._getModelAttributes(
                MessageInfo, 'pdbx_deposition_message_info'
            )
            
            # Map MessageFileReference to pdbx_deposition_message_file_reference
            itemDict['pdbx_deposition_message_file_reference'] = self._getModelAttributes(
                MessageFileReference, 'pdbx_deposition_message_file_reference'
            )
            
            # Map MessageStatus to pdbx_deposition_message_status
            itemDict['pdbx_deposition_message_status'] = self._getModelAttributes(
                MessageStatus, 'pdbx_deposition_message_status'
            )
        else:
            # Fallback to hardcoded definitions if models not available
            itemDict = self._getFallbackItemDict()
        
        # Add origcomm_reference for compatibility (not in database yet)
        itemDict['pdbx_deposition_message_origcomm_reference'] = [
            ('_pdbx_deposition_message_origcomm_reference.ordinal_id', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.message_id', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.deposition_data_set_id', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_message_id', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_deposition_data_set_id', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_timestamp', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_sender', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_recipient', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_message_subject', '%s', 'str', ''),
            ('_pdbx_deposition_message_origcomm_reference.orig_attachments', '%s', 'str', '')
        ]
        
        return itemDict
    
    def _getModelAttributes(self, model_class, category_name: str) -> List[Tuple[str, str, str, str]]:
        """Extract attribute definitions from SQLAlchemy model"""
        attributes = []
        
        try:
            mapper = inspect(model_class)
            for column in mapper.columns:
                attr_name = f"_{category_name}.{column.name}"
                # Format: (attribute_name, format_string, data_type, default_value)
                attributes.append((attr_name, '%s', 'str', ''))
        
        except Exception as e:
            if self.__verbose:
                print(f"Warning: Could not inspect model {model_class.__name__}: {e}", file=self.__lfh)
            
        return attributes
    
    def _getFallbackItemDict(self) -> Dict[str, List[Tuple[str, str, str, str]]]:
        """Fallback hardcoded definitions if database models not available"""
        return {
            'pdbx_deposition_message_info': [
                ('_pdbx_deposition_message_info.ordinal_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.message_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.deposition_data_set_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.timestamp', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.sender', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.context_type', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.context_value', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.parent_message_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.message_subject', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.message_text', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.message_type', '%s', 'str', ''),
                ('_pdbx_deposition_message_info.send_status', '%s', 'str', '')
            ],
            'pdbx_deposition_message_file_reference': [
                ('_pdbx_deposition_message_file_reference.ordinal_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.message_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.deposition_data_set_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.content_type', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.content_format', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.partition_number', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.version_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.storage_type', '%s', 'str', ''),
                ('_pdbx_deposition_message_file_reference.upload_file_name', '%s', 'str', '')
            ],
            'pdbx_deposition_message_status': [
                ('_pdbx_deposition_message_status.message_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_status.deposition_data_set_id', '%s', 'str', ''),
                ('_pdbx_deposition_message_status.read_status', '%s', 'str', ''),
                ('_pdbx_deposition_message_status.action_reqd', '%s', 'str', ''),
                ('_pdbx_deposition_message_status.for_release', '%s', 'str', '')
            ]
        }
    
    def getAttributeNameList(self, categoryName: str) -> List[str]:
        """
        Get list of attribute names for a given category.
        
        This is the main method used by MessagingIo.py at line 106.
        """
        if categoryName in self.__attributeCache:
            return self.__attributeCache[categoryName]
        
        attribute_list = []
        
        if categoryName in self.__itemDict:
            for attr_def in self.__itemDict[categoryName]:
                # Extract attribute name from full definition
                # Format: _category.attribute_name -> attribute_name
                full_name = attr_def[0]
                if '.' in full_name:
                    attr_name = full_name.split('.', 1)[1]
                else:
                    attr_name = full_name
                attribute_list.append(attr_name)
        
        # Cache the result
        self.__attributeCache[categoryName] = attribute_list
        
        if self.__debug:
            print(f"getAttributeNameList({categoryName}) -> {attribute_list}", file=self.__lfh)
        
        return attribute_list
    
    def getCategoryList(self) -> List[str]:
        """Get list of all available categories"""
        return [cat_name for cat_name, _ in self.__categoryInfo]
    
    def getStyleId(self) -> str:
        """Get style identifier"""
        return self._styleId
    
    def getItemFormatInfo(self, categoryName: str, attributeName: str) -> Optional[Tuple[str, str, str]]:
        """Get format information for a specific item"""
        if categoryName in self.__itemDict:
            for attr_def in self.__itemDict[categoryName]:
                full_name = attr_def[0]
                if full_name.endswith(f".{attributeName}"):
                    # Return (format_string, data_type, default_value)
                    return (attr_def[1], attr_def[2], attr_def[3])
        return None
    
    def getCategoryInfo(self) -> List[Tuple[str, str]]:
        """Get category information list"""
        return self.__categoryInfo
    
    def getItemDict(self) -> Dict[str, List[Tuple[str, str, str, str]]]:
        """Get complete item dictionary"""
        return self.__itemDict


def test_database_style():
    """Test function to verify database style provider works"""
    style = PdbxMessageCategoryStyle(verbose=True)
    
    print("=== Database-Aware PdbxMessageCategoryStyle Test ===")
    print(f"Style ID: {style.getStyleId()}")
    print(f"Categories: {style.getCategoryList()}")
    
    # Test the main method used by MessagingIo
    msg_attrs = style.getAttributeNameList('pdbx_deposition_message_info')
    print(f"Message info attributes: {msg_attrs}")
    
    file_attrs = style.getAttributeNameList('pdbx_deposition_message_file_reference')
    print(f"File reference attributes: {file_attrs}")
    
    status_attrs = style.getAttributeNameList('pdbx_deposition_message_status')
    print(f"Status attributes: {status_attrs}")
    
    print("=== Test Complete ===")


if __name__ == "__main__":
    test_database_style()
