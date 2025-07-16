"""
wwPDB Messaging Module

A comprehensive messaging system for wwPDB supporting both CIF file-based 
and database-backed message storage, with web interface components and 
automated message handling capabilities.

Key modules:
- io: File I/O operations and backend factory
- db: Database operations and configuration  
- models: Message model classes
- util: Utility functions and classes
- depict: Web interface and templates
- webapp: Web application components
"""

__docformat__ = "restructuredtext en"
__author__ = "Ezra Peisach"
__email__ = "ezra.peisach@rcsb.org"
__license__ = "Apache 2.0"
__version__ = "0.179"

# Main factory for creating messaging backends
from .io import MessagingFactory

__all__ = [
    "MessagingFactory",
]
