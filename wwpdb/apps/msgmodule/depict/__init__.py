"""
Depict module for wwPDB messaging system.

Message visualization and template handling for rendering messages in web interfaces,
including HTML templates and message formatting functionality.
"""

from .MessagingDepict import MessagingDepict
from .MessagingTemplates import MessagingTemplates

__all__ = [
    "MessagingDepict",
    "MessagingTemplates",
]