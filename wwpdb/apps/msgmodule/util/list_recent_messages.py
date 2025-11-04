"""
List Recent Messages Script

This script provides a command-line interface to query and list communications
within a specified time period from the messaging database. It supports various
filtering options and multiple output formats for consumption by the Workflow
Manager (WFM) and other systems.

Features:
    - Query messages by date range (e.g., last 7, 30, 60 days)
    - Filter by deposition ID, content type, sender, and keywords
    - Multiple output formats: JSON (default), CSV, plain text
    - Efficient database queries using indexed timestamp field
    - Comprehensive logging and error handling
    - Support for multi-site deployment

Examples:
    # List all messages from last 7 days as JSON (uses current site)
    python list_recent_messages.py --days 7

    # List all messages from last 7 days with explicit site ID
    python list_recent_messages.py --site-id WWPDB_DEPLOY_TEST --days 7

    # List messages for specific depositions in last 30 days
    python list_recent_messages.py --site-id RCSB --days 30 \\
        --depositions D_1000000001 D_1000000002

    # Filter by content type and keywords, output as CSV
    python list_recent_messages.py --site-id PDBe --days 14 \\
        --content-type messages-to-depositor \\
        --keywords "validation" "error" \\
        --format csv --output results.csv

    # Filter by sender with human-readable output (uses current site)
    python list_recent_messages.py --days 30 \\
        --sender "annotator@example.com" \\
        --format text

    # Custom date range with JSON logging
    python list_recent_messages.py --site-id RCSB \\
        --start-date 2025-10-01 --end-date 2025-10-31 \\
        --json-log query.log --log-level DEBUG
"""
# pylint: disable=unnecessary-pass,logging-fstring-interpolation

import sys
import logging
import argparse
import json
import csv
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Optional, Any

# Initialize ConfigInfo to get database configuration
from wwpdb.utils.config.ConfigInfo import ConfigInfo, getSiteId

# Database imports
from wwpdb.apps.msgmodule.db import DataAccessLayer, MessageInfo


# Custom exceptions for better error handling
class QueryError(Exception):
    """Base exception for query-related errors"""
    pass


class DatabaseConfigError(QueryError):
    """Raised when database configuration is invalid"""
    pass


class OutputFormatError(QueryError):
    """Raised when output format is invalid or writing fails"""
    pass


# Configuration constants
class DbConfigKeys:
    """Database configuration key names"""
    HOST = "SITE_MESSAGE_DB_HOST_NAME"
    USER = "SITE_MESSAGE_DB_USER_NAME"
    DATABASE = "SITE_MESSAGE_DB_NAME"
    PORT = "SITE_MESSAGE_DB_PORT_NUMBER"
    PASSWORD = "SITE_MESSAGE_DB_PASSWORD"


# Enhanced logging setup (reused from existing scripts)
class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging"""

    def formatTime(self, record, datefmt=None):
        """Return RFC3339 timestamp with microseconds and Z suffix."""
        dt = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return dt.isoformat(timespec="microseconds").replace("+00:00", "Z")

    def format(self, record):
        obj = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "event": getattr(record, "event", "general"),
            "message": record.getMessage(),
        }
        # Add structured data if present
        if hasattr(record, 'extra_data'):
            obj.update(record.extra_data)
        return json.dumps(obj, ensure_ascii=False)


# Map events to log levels
EVENT_LEVELS = {
    # errors
    "db_connection_failed": logging.ERROR,
    "query_failed": logging.ERROR,
    "output_write_failed": logging.ERROR,
    "invalid_date_format": logging.ERROR,
    "invalid_content_type": logging.ERROR,
    # warnings
    "no_results": logging.WARNING,
    # info
    "init_query": logging.INFO,
    "db_connected": logging.INFO,
    "query_start": logging.INFO,
    "query_complete": logging.INFO,
    "output_written": logging.INFO,
    # debug
    "query_params": logging.DEBUG,
    "result_processing": logging.DEBUG,
}


def setup_logging(json_log_file: Optional[str] = None, log_level: str = "INFO"):
    """Setup dual logging: console for humans, JSON for parsing"""
    root_logger = logging.getLogger()
    root_logger.handlers = []  # Clear existing handlers
    lvl = getattr(logging, (log_level or "INFO").upper(), logging.INFO)
    root_logger.setLevel(lvl)

    # Console handler for humans
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(lvl)
    console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    root_logger.addHandler(console_handler)

    # JSON file handler for parsing
    if json_log_file:
        file_handler = logging.FileHandler(json_log_file, encoding='utf-8')
        file_handler.setLevel(lvl)
        file_handler.setFormatter(JsonFormatter())
        root_logger.addHandler(file_handler)


def log_event(event: str, level: Optional[int] = None, **kwargs):
    """Log structured events for easy querying"""
    #  logger = logging.getLogger(__name__)
    lvl = level if level is not None else EVENT_LEVELS.get(event, logging.INFO)
    # Avoid duplicating 'message' in extra_data; it is already the log message
    extra_data = {k: v for k, v in kwargs.items() if k != 'message'}
    logger.log(lvl, kwargs.get('message', ''), extra={'event': event, 'extra_data': extra_data})


# Setup basic logging initially
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database connection configuration"""

    def __init__(self, host: str, port: int, database: str, username: str,
                 password: str = "", charset: str = "utf8mb4"):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.charset = charset

    def to_dict(self) -> Dict:
        """Convert to dictionary for DataAccessLayer"""
        return {
            "host": self.host,
            "port": self.port,
            "database": self.database,
            "username": self.username,
            "password": self.password,
            "charset": self.charset,
        }


class MessageFormatter:
    """Formats message data for different output formats"""

    @staticmethod
    def to_dict(message: MessageInfo, include_file_refs: bool = True,
                include_status: bool = True) -> Dict[str, Any]:
        """Convert MessageInfo to dictionary with metadata.

        Args:
            message: MessageInfo object to convert
            include_file_refs: Whether to include file reference information
            include_status: Whether to include status information

        Returns:
            Dictionary with message metadata
        """
        data = {
            "message_id": message.message_id,
            "deposition_data_set_id": message.deposition_data_set_id,
            "timestamp": message.timestamp.isoformat() if message.timestamp else None,
            "sender": message.sender,
            "message_subject": message.message_subject,
            "message_type": message.message_type,
            "content_type": message.content_type,
            "context_type": message.context_type,
            "context_value": message.context_value,
            "parent_message_id": message.parent_message_id,
            "send_status": message.send_status,
        }

        # Include file attachments if requested and available
        if include_file_refs and hasattr(message, 'file_references') and message.file_references:
            data["file_attachments"] = [
                {
                    "content_type": ref.content_type,
                    "content_format": ref.content_format,
                    "version_id": ref.version_id,
                    "partition_number": ref.partition_number,
                    "storage_type": ref.storage_type,
                    "upload_file_name": ref.upload_file_name,
                }
                for ref in message.file_references
            ]
        else:
            data["file_attachments"] = []

        # Include status flags if requested and available
        if include_status and hasattr(message, 'status') and message.status:
            data["read_status"] = message.status.read_status
            data["action_reqd"] = message.status.action_reqd
            data["for_release"] = message.status.for_release
        else:
            data["read_status"] = None
            data["action_reqd"] = None
            data["for_release"] = None

        return data

    @staticmethod
    def to_json(messages: List[MessageInfo], indent: int = 2) -> str:
        """Format messages as JSON.

        Args:
            messages: List of MessageInfo objects
            indent: JSON indentation level (None for compact output)

        Returns:
            JSON string
        """
        data = {
            "count": len(messages),
            "messages": [MessageFormatter.to_dict(msg) for msg in messages]
        }
        return json.dumps(data, indent=indent, ensure_ascii=False)

    @staticmethod
    def to_csv(messages: List[MessageInfo]) -> str:
        """Format messages as CSV.

        Args:
            messages: List of MessageInfo objects

        Returns:
            CSV string
        """
        import io
        output = io.StringIO()

        fieldnames = [
            "message_id", "deposition_data_set_id", "timestamp", "sender",
            "message_subject", "message_type", "content_type", "context_type",
            "context_value", "parent_message_id", "send_status",
            "has_attachments", "read_status", "action_reqd", "for_release"
        ]

        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()

        for msg in messages:
            data = MessageFormatter.to_dict(msg)
            row = {
                "message_id": data["message_id"],
                "deposition_data_set_id": data["deposition_data_set_id"],
                "timestamp": data["timestamp"] or "",
                "sender": data["sender"],
                "message_subject": data["message_subject"],
                "message_type": data["message_type"],
                "content_type": data["content_type"],
                "context_type": data["context_type"] or "",
                "context_value": data["context_value"] or "",
                "parent_message_id": data["parent_message_id"] or "",
                "send_status": data["send_status"] or "",
                "has_attachments": "Y" if data["file_attachments"] else "N",
                "read_status": data["read_status"] or "",
                "action_reqd": data["action_reqd"] or "",
                "for_release": data["for_release"] or "",
            }
            writer.writerow(row)

        return output.getvalue()

    @staticmethod
    def to_text(messages: List[MessageInfo]) -> str:
        """Format messages as human-readable plain text.

        Args:
            messages: List of MessageInfo objects

        Returns:
            Plain text string
        """
        lines = [f"Found {len(messages)} message(s)\n"]
        lines.append("=" * 80)

        for i, msg in enumerate(messages, 1):
            data = MessageFormatter.to_dict(msg)

            lines.append(f"\nMessage {i}:")
            lines.append(f"  ID: {data['message_id']}")
            lines.append(f"  Deposition: {data['deposition_data_set_id']}")
            lines.append(f"  Timestamp: {data['timestamp']}")
            lines.append(f"  Sender: {data['sender']}")
            lines.append(f"  Subject: {data['message_subject']}")
            lines.append(f"  Content Type: {data['content_type']}")
            lines.append(f"  Message Type: {data['message_type']}")

            if data['context_type']:
                lines.append(f"  Context: {data['context_type']} = {data['context_value']}")

            if data['parent_message_id']:
                lines.append(f"  Reply To: {data['parent_message_id']}")

            if data['file_attachments']:
                lines.append(f"  Attachments: {len(data['file_attachments'])}")
                for att in data['file_attachments']:
                    lines.append(f"    - {att['content_type']}.{att['content_format']} "
                                 f"(v{att['version_id']})")

            if data['read_status']:
                status_parts = []
                if data['read_status'] == 'Y':
                    status_parts.append("Read")
                if data['action_reqd'] == 'Y':
                    status_parts.append("Action Required")
                if data['for_release'] == 'Y':
                    status_parts.append("For Release")
                if status_parts:
                    lines.append(f"  Status: {', '.join(status_parts)}")

            lines.append("-" * 80)

        return "\n".join(lines)


class MessageQueryService:
    """Service for querying messages from the database"""

    def __init__(self, site_id: str, config_info: Optional[ConfigInfo] = None):
        """Initialize query service with database configuration.

        Args:
            site_id: Site identifier (e.g., 'WWPDB_DEPLOY_TEST', 'RCSB')
            config_info: Optional ConfigInfo instance for testing/dependency injection
        """
        self.site_id = site_id
        self.config_info = config_info or ConfigInfo(site_id)

        # Get database configuration
        db_config = self._get_database_config()
        log_event("init_query", site_id=site_id, db_host=db_config.host,
                  db_name=db_config.database)

        self.data_access = DataAccessLayer(db_config.to_dict())
        log_event("db_connected", site_id=site_id)

    def _get_database_config(self) -> DatabaseConfig:
        """Get database configuration from ConfigInfo.

        Returns:
            DatabaseConfig: Structured database configuration

        Raises:
            DatabaseConfigError: If required configuration is missing
        """
        host = self.config_info.get(DbConfigKeys.HOST)
        user = self.config_info.get(DbConfigKeys.USER)
        database = self.config_info.get(DbConfigKeys.DATABASE)
        port = self.config_info.get(DbConfigKeys.PORT, "3306")
        password = self.config_info.get(DbConfigKeys.PASSWORD, "")

        if not all([host, user, database]):
            raise DatabaseConfigError(
                f"Missing required database configuration. "
                f"Required: {DbConfigKeys.HOST}, {DbConfigKeys.USER}, {DbConfigKeys.DATABASE}"
            )

        return DatabaseConfig(
            host=host,
            port=int(port),
            database=database,
            username=user,
            password=password
        )

    def query_messages(self, days: Optional[int] = None,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       deposition_ids: Optional[List[str]] = None,
                       content_types: Optional[List[str]] = None,
                       sender: Optional[str] = None,
                       keywords: Optional[List[str]] = None) -> List[MessageInfo]:
        """Query messages with specified filters.

        Args:
            days: Number of days back from now (takes precedence over start_date)
            start_date: Start of date range (inclusive)
            end_date: End of date range (inclusive, defaults to now)
            deposition_ids: Optional list of deposition IDs to filter by
            content_types: Optional list of content types to filter by
            sender: Optional sender email/identifier to filter by
            keywords: Optional keywords to search in subject and text

        Returns:
            List[MessageInfo]: List of messages matching the criteria

        Raises:
            QueryError: If query execution fails
        """
        # Calculate date range
        if days is not None:
            start_date = datetime.now() - timedelta(days=days)
            end_date = datetime.now()
        elif start_date is None:
            raise QueryError("Either --days or --start-date must be specified")

        # Set end_date to now if not specified
        if end_date is None:
            end_date = datetime.now()

        log_event("query_start", start_date=start_date.isoformat(),
                  end_date=end_date.isoformat(), deposition_count=len(deposition_ids) if deposition_ids else 0,
                  content_types=content_types, sender=sender, keywords=keywords)

        log_event("query_params", level=logging.DEBUG,
                  start_date=start_date.isoformat(), end_date=end_date.isoformat(),
                  deposition_ids=deposition_ids, content_types=content_types,
                  sender=sender, keywords=keywords)

        try:
            # Execute query using the new DataAccessLayer method
            messages = self.data_access.messages.get_by_date_range(
                start_date=start_date,
                end_date=end_date,
                deposition_ids=deposition_ids,
                content_types=content_types,
                sender=sender,
                keywords=keywords
            )

            log_event("query_complete", message_count=len(messages))

            if not messages:
                log_event("no_results", message="No messages found matching the criteria")

            return messages

        except Exception as e:
            log_event("query_failed", error=str(e))
            raise QueryError(f"Query execution failed: {e}") from e

    def close(self):
        """Close database connection and cleanup resources"""
        try:
            self.data_access.close()
        except Exception as e:
            logger.debug(f"Error closing data access: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensure resources are cleaned up"""
        self.close()
        return False  # Don't suppress exceptions


def parse_date(date_string: str) -> datetime:
    """Parse date string in various formats.

    Args:
        date_string: Date string in formats like 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS'

    Returns:
        datetime object

    Raises:
        ValueError: If date format is invalid
    """
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"]:
        try:
            return datetime.strptime(date_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid date format: {date_string}. Use YYYY-MM-DD or YYYY-MM-DD HH:MM:SS")


def validate_content_types(content_types: List[str]) -> List[str]:
    """Validate content types.

    Args:
        content_types: List of content type strings

    Returns:
        List of validated content types

    Raises:
        ValueError: If any content type is invalid
    """
    valid_types = ["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"]
    for ct in content_types:
        if ct not in valid_types:
            raise ValueError(
                f"Invalid content type: {ct}. Must be one of: {', '.join(valid_types)}"
            )
    return content_types


def write_output(content: str, output_file: Optional[str] = None, format_type: str = "json"):
    """Write output to file or stdout.

    Args:
        content: Content to write
        output_file: Optional output file path (None = stdout)
        format_type: Format type for logging

    Raises:
        OutputFormatError: If writing fails
    """
    try:
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(content)
            log_event("output_written", output_file=output_file, format=format_type)
        else:
            print(content)
    except Exception as e:
        log_event("output_write_failed", error=str(e), output_file=output_file)
        raise OutputFormatError(f"Failed to write output: {e}") from e


def create_argument_parser() -> argparse.ArgumentParser:
    """Create and configure the command-line argument parser.

    Returns:
        argparse.ArgumentParser: Configured argument parser
    """
    parser = argparse.ArgumentParser(
        description="List communications within a specified time period from the messaging database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all messages from last 7 days (uses current site)
  %(prog)s --days 7

  # List all messages from last 7 days with explicit site ID
  %(prog)s --site-id WWPDB_DEPLOY_TEST --days 7

  # List messages for specific depositions in last 30 days
  %(prog)s --site-id RCSB --days 30 --depositions D_1000000001 D_1000000002

  # Filter by content type and keywords, output as CSV
  %(prog)s --site-id PDBe --days 14 \\
      --content-type messages-to-depositor \\
      --keywords "validation" "error" \\
      --format csv --output results.csv

  # Custom date range with text output (uses current site)
  %(prog)s --start-date 2025-10-01 --end-date 2025-10-31 --format text
        """
    )

    # Site configuration (optional - defaults to current site)
    parser.add_argument(
        "--site-id",
        help="Site ID (e.g., WWPDB_DEPLOY_TEST, RCSB, PDBe, PDBj, BMRB). "
             "If not specified, uses the current site from configuration"
    )

    # Date range options (mutually exclusive with custom dates)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--days",
        type=int,
        help="Number of days back from now (e.g., 7, 30, 60)"
    )
    date_group.add_argument(
        "--start-date",
        help="Start date (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')"
    )

    parser.add_argument(
        "--end-date",
        help="End date (YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS'), defaults to now"
    )

    # Filter options
    parser.add_argument(
        "--depositions",
        nargs="+",
        help="Filter by deposition ID(s) (e.g., D_1000000001 D_1000000002)"
    )
    parser.add_argument(
        "--content-type",
        nargs="+",
        choices=["messages-to-depositor", "messages-from-depositor", "notes-from-annotator"],
        help="Filter by content type(s)"
    )
    parser.add_argument(
        "--sender",
        help="Filter by sender email/identifier (partial match)"
    )
    parser.add_argument(
        "--keywords",
        nargs="+",
        help="Keywords to search in message subject and content (AND logic)"
    )

    # Output options
    parser.add_argument(
        "--format",
        choices=["json", "csv", "text"],
        default="json",
        help="Output format (default: json)"
    )
    parser.add_argument(
        "--output",
        help="Output file path (default: stdout)"
    )
    parser.add_argument(
        "--compact",
        action="store_true",
        help="Compact JSON output (no indentation)"
    )

    # Logging options
    parser.add_argument(
        "--json-log",
        help="Path to JSON log file for structured logging"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Console and JSON log level (default: %(default)s)"
    )

    return parser


def main():
    """Main entry point for the script"""
    parser = create_argument_parser()
    args = parser.parse_args()

    # Setup enhanced logging
    setup_logging(args.json_log, args.log_level)

    try:
        # Parse dates if provided
        start_date = None
        end_date = None

        if args.start_date:
            try:
                start_date = parse_date(args.start_date)
            except ValueError as e:
                log_event("invalid_date_format", error=str(e), date_string=args.start_date)
                logger.error(str(e))
                sys.exit(1)

        if args.end_date:
            try:
                end_date = parse_date(args.end_date)
            except ValueError as e:
                log_event("invalid_date_format", error=str(e), date_string=args.end_date)
                logger.error(str(e))
                sys.exit(1)

        # Validate content types if provided
        if args.content_type:
            try:
                validate_content_types(args.content_type)
            except ValueError as e:
                log_event("invalid_content_type", error=str(e), content_types=args.content_type)
                logger.error(str(e))
                sys.exit(1)

        # Determine site ID (use provided or default to current site)
        site_id = args.site_id
        if not site_id:
            try:
                site_id = getSiteId()
                logger.info(f"Using current site: {site_id}")
            except Exception as e:
                log_event("site_id_resolution_failed", error=str(e))
                logger.error(f"Failed to determine site ID: {e}")
                logger.error("Please specify --site-id explicitly")
                sys.exit(1)

        # Execute query
        with MessageQueryService(site_id) as service:
            messages = service.query_messages(
                days=args.days,
                start_date=start_date,
                end_date=end_date,
                deposition_ids=args.depositions,
                content_types=args.content_type,
                sender=args.sender,
                keywords=args.keywords
            )

            # Format and output results
            if args.format == "json":
                indent = None if args.compact else 2
                output = MessageFormatter.to_json(messages, indent=indent)
            elif args.format == "csv":
                output = MessageFormatter.to_csv(messages)
            else:  # text
                output = MessageFormatter.to_text(messages)

            write_output(output, args.output, args.format)

            logger.info(f"Successfully retrieved {len(messages)} message(s)")

    except DatabaseConfigError as e:
        log_event("db_connection_failed", error=str(e))
        logger.error(f"Database configuration error: {e}")
        sys.exit(1)
    except OutputFormatError as e:
        log_event("output_write_failed", error=str(e))
        logger.error(f"Output failed: {e}")
        sys.exit(1)
    except QueryError as e:
        log_event("query_failed", error=str(e))
        logger.error(f"Query failed: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
