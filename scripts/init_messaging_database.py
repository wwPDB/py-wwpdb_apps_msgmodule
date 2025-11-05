#!/usr/bin/env python
"""
Standalone Initialize Messaging Database Script

This script creates the necessary tables for the wwPDB messaging system
database migration from CIF file-based storage. The database schema
exactly mirrors the mmCIF categories:
- pdbx_deposition_message_info
- pdbx_deposition_message_file_reference  
- pdbx_deposition_message_status

The content_type field differentiates between:
- messages-to-depositor
- messages-from-depositor
- notes-from-annotator

This is a standalone version that includes all dependencies and can be run
directly on production servers without checking out the branch.
"""

import os
import sys
import logging
import argparse
import pymysql
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_database_config(args):
    """Get database configuration from command line args or try ConfigInfo fallback"""
    config = {}
    
    # First try command line arguments
    if all([args.host, args.user, args.database]):
        config = {
            "host": args.host,
            "port": args.port or 3306,
            "user": args.user,
            "password": args.password or "",
            "database": args.database,
            "charset": "utf8mb4",
        }
        
        if args.socket:
            config["unix_socket"] = args.socket
            
        logger.info("Using database configuration from command line arguments")
        config_display = dict((k, "***" if k == "password" else v) for k, v in config.items())
        logger.info(f"Database config: {config_display}")
        return config
    
    # Fallback to ConfigInfo if available and site_id provided
    if args.site_id:
        try:
            from wwpdb.utils.config.ConfigInfo import ConfigInfo
            
            config_info = ConfigInfo(args.site_id)
            
            # Get database configuration from ConfigInfo (using messaging-specific keys)
            host = config_info.get("SITE_MESSAGE_DB_HOST_NAME")
            user = config_info.get("SITE_MESSAGE_DB_USER_NAME")
            database = config_info.get("SITE_MESSAGE_DB_NAME")
            port = config_info.get("SITE_MESSAGE_DB_PORT_NUMBER", "3306")
            password = config_info.get("SITE_MESSAGE_DB_PASSWORD", "")
            socket = config_info.get("SITE_MESSAGE_DB_SOCKET")
            
            if not all([host, user, database]):
                missing = [k for k, v in [("SITE_MESSAGE_DB_HOST_NAME", host), ("SITE_MESSAGE_DB_USER_NAME", user), ("SITE_MESSAGE_DB_NAME", database)] if not v]
                raise RuntimeError(f"Missing required ConfigInfo database settings: {', '.join(missing)}")

            config = {
                "host": host,
                "port": int(port),
                "user": user,
                "password": password,
                "database": database,
                "charset": "utf8mb4",
            }
            
            if socket:
                config["unix_socket"] = socket
            
            logger.info(f"Using database configuration from ConfigInfo for site {args.site_id}")
            config_display = dict((k, "***" if k == "password" else v) for k, v in config.items())
            logger.info(f"Database config: {config_display}")
            return config
            
        except ImportError:
            logger.warning("ConfigInfo not available, falling back to command line arguments")
        except Exception as e:
            logger.error(f"Failed to get database config from ConfigInfo: {e}")
    
    # If we get here, neither command line nor ConfigInfo worked
    raise RuntimeError(
        "Database configuration required. Please provide either:\n"
        "1. Command line arguments: --host, --user, --database (and optionally --port, --password, --socket)\n"
        "2. Or --site-id with properly configured ConfigInfo"
    )


def create_database_if_not_exists(config):
    """Create the database if it doesn't exist"""
    try:
        # Connect without specifying database
        conn_config = config.copy()
        database_name = conn_config.pop("database")

        connection = pymysql.connect(**conn_config)
        cursor = connection.cursor()

        # Create database if it doesn't exist
        cursor.execute(
            f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
        )
        logger.info(f"Database '{database_name}' created or already exists")

        cursor.close()
        connection.close()

    except Exception as e:
        logger.error(f"Error creating database: {e}")
        raise


def drop_database_if_exists(config):
    """Drop the database if it exists (DANGER: destructive operation!)"""
    try:
        conn_config = config.copy()
        database_name = conn_config.pop("database")
        connection = pymysql.connect(**conn_config)
        cursor = connection.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {database_name}")
        logger.info(f"Database '{database_name}' dropped (if it existed)")
        cursor.close()
        connection.close()
    except Exception as e:
        logger.error(f"Error dropping database: {e}")
        raise


def get_create_table_statements():
    """Return SQL statements to create all messaging tables"""

    statements = []

    # Main messages table - maps to _pdbx_deposition_message_info category
    statements.append(
        """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_info (
            ordinal_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            message_id VARCHAR(64) UNIQUE NOT NULL,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,
            sender VARCHAR(150) NOT NULL,
            context_type VARCHAR(50),
            context_value VARCHAR(255),
            parent_message_id VARCHAR(64),
            message_subject TEXT NOT NULL,
            message_text LONGTEXT NOT NULL,
            message_type VARCHAR(20) DEFAULT 'text',
            send_status CHAR(1) DEFAULT 'Y',
            content_type ENUM('messages-to-depositor', 'messages-from-depositor', 'notes-from-annotator') NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_message_id (message_id),
            INDEX idx_timestamp (timestamp),
            INDEX idx_sender (sender),
            INDEX idx_context_type (context_type),
            INDEX idx_content_type (content_type),
            INDEX idx_created_at (created_at),
            INDEX idx_ordinal_id (ordinal_id),
            
            FOREIGN KEY (parent_message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE SET NULL
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    )

    # Message file references table - maps to _pdbx_deposition_message_file_reference category
    statements.append(
        """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_file_reference (
            ordinal_id BIGINT PRIMARY KEY AUTO_INCREMENT,
            message_id VARCHAR(64) NOT NULL,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            content_type VARCHAR(50) NOT NULL,
            content_format VARCHAR(20) NOT NULL,
            partition_number INT DEFAULT 1,
            version_id INT DEFAULT 1,
            storage_type VARCHAR(20) DEFAULT 'archive',
            upload_file_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            INDEX idx_message_id (message_id),
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_content_type (content_type),
            INDEX idx_storage_type (storage_type),
            INDEX idx_ordinal_id (ordinal_id),
            
            FOREIGN KEY (message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE CASCADE
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    )

    # Message status table - maps to _pdbx_deposition_message_status category
    statements.append(
        """
        CREATE TABLE IF NOT EXISTS pdbx_deposition_message_status (
            message_id VARCHAR(64) PRIMARY KEY,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            read_status CHAR(1) DEFAULT 'N',
            action_reqd CHAR(1) DEFAULT 'N',
            for_release CHAR(1) DEFAULT 'N',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            INDEX idx_deposition_id (deposition_data_set_id),
            INDEX idx_read_status (read_status),
            INDEX idx_action_reqd (action_reqd),
            INDEX idx_for_release (for_release),
            
            FOREIGN KEY (message_id) REFERENCES pdbx_deposition_message_info(message_id) ON DELETE CASCADE
        ) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
    """
    )

    return statements


def create_tables(config):
    """Create all messaging tables"""
    try:
        connection = pymysql.connect(**config)
        cursor = connection.cursor()

        statements = get_create_table_statements()

        for statement in statements:
            logger.info("Executing table creation statement...")
            cursor.execute(statement)
            logger.info("Table created successfully")

        connection.commit()
        logger.info("All tables created successfully")

        cursor.close()
        connection.close()

    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise


def verify_tables(config):
    """Verify that all tables were created correctly"""
    try:
        connection = pymysql.connect(**config)
        cursor = connection.cursor()

        # List expected tables - matching mmCIF category names
        expected_tables = [
            "pdbx_deposition_message_info",
            "pdbx_deposition_message_file_reference",
            "pdbx_deposition_message_status",
        ]

        cursor.execute("SHOW TABLES")
        existing_tables = [table[0] for table in cursor.fetchall()]

        logger.info(f"Existing tables: {existing_tables}")

        for table in expected_tables:
            if table in existing_tables:
                logger.info(f"✓ Table '{table}' exists")

                # Show table structure
                cursor.execute(f"DESCRIBE {table}")
                columns = cursor.fetchall()
                logger.info(f"  Columns in {table}: {[col[0] for col in columns]}")
            else:
                logger.error(f"✗ Table '{table}' missing")

        cursor.close()
        connection.close()

    except Exception as e:
        logger.error(f"Error verifying tables: {e}")
        raise


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Initialize messaging database schema (standalone version)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Using command line database credentials:
  python init_messaging_database_standalone.py --host localhost --user msguser --database messaging --password mypass

  # Using ConfigInfo (if available):
  python init_messaging_database_standalone.py --site-id RCSB

  # Override ConfigInfo with specific credentials:
  python init_messaging_database_standalone.py --site-id RCSB --host prod-db.example.com --user produser
        """
    )
    
    # Database connection options
    parser.add_argument("--host", help="Database host")
    parser.add_argument("--port", type=int, help="Database port (default: 3306)")
    parser.add_argument("--user", help="Database user")
    parser.add_argument("--password", help="Database password")
    parser.add_argument("--database", help="Database name")
    parser.add_argument("--socket", help="Unix socket path (optional)")
    
    # ConfigInfo fallback
    parser.add_argument(
        "--site-id", help="Site ID for ConfigInfo database configuration (e.g., RCSB, PDBe, PDBj, BMRB)"
    )
    
    # Operations
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing tables, do not create",
    )
    parser.add_argument(
        "--drop-and-recreate", 
        action="store_true", 
        help="Drop and recreate the database (DANGEROUS: all data will be lost!)"
    )

    args = parser.parse_args()

    # Get database configuration
    try:
        config = get_database_config(args)
    except Exception as e:
        logger.error(f"Database configuration error: {e}")
        sys.exit(1)

    logger.info(
        f"Connecting to database: {config['host']}:{config['port']}/{config['database']}"
    )

    try:
        if args.drop_and_recreate:
            drop_database_if_exists(config)
        if not args.verify_only:
            # Create database if needed
            create_database_if_not_exists(config)

            # Create tables
            create_tables(config)

        # Verify tables exist
        verify_tables(config)

        logger.info("Database initialization completed successfully")

    except Exception as e:
        logger.error(f"Database initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()