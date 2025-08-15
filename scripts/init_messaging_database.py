#!/usr/bin/env python
"""
Initialize the messaging database schema.

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
"""

import os
import sys
import logging
import argparse
import mysql.connector
from datetime import datetime

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_database_config_from_configinfo(site_id):
    """Get database configuration from ConfigInfo instead of environment variables"""
    if not site_id:
        raise ValueError("site_id is required and cannot be empty")
        
    try:
        from wwpdb.utils.config.ConfigInfo import ConfigInfo
        
        config_info = ConfigInfo(site_id)
        
        # Get database configuration from ConfigInfo (same keys as MessagingDb)
        host = config_info.get("SITE_DB_HOST_NAME")
        user = config_info.get("SITE_DB_ADMIN_USER")
        database = config_info.get("WWPDB_MESSAGING_DB_NAME")
        port = config_info.get("SITE_DB_PORT_NUMBER", "3306")
        password = config_info.get("SITE_DB_ADMIN_PASS", "")
        
        if not all([host, user, database]):
            missing = [k for k, v in [("SITE_DB_HOST_NAME", host), ("SITE_DB_ADMIN_USER", user), ("WWPDB_MESSAGING_DB_NAME", database)] if not v]
            raise RuntimeError(f"Missing required ConfigInfo database settings: {', '.join(missing)}")

        config = {
            "host": host,
            "port": int(port),
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
        }
        
        logger.info(f"Using database configuration from ConfigInfo for site {site_id}")
        # Log config without password
        config_display = dict((k, "***" if k == "password" else v) for k, v in config.items())
        logger.info(f"Database config: {config_display}")
        
        return config
        
    except Exception as e:
        logger.error(f"Failed to get database config from ConfigInfo: {e}")
        raise RuntimeError(f"Database configuration error: {e}")


def create_database_if_not_exists(config):
    """Create the database if it doesn't exist"""
    try:
        # Connect without specifying database
        conn_config = config.copy()
        database_name = conn_config.pop("database")

        connection = mysql.connector.connect(**conn_config)
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
        connection = mysql.connector.connect(**conn_config)
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
            message_id VARCHAR(255) UNIQUE NOT NULL,
            deposition_data_set_id VARCHAR(50) NOT NULL,
            timestamp DATETIME NOT NULL,
            sender VARCHAR(255) NOT NULL,
            context_type VARCHAR(50),
            context_value VARCHAR(255),
            parent_message_id VARCHAR(255),
            message_subject BLOB NOT NULL,
            message_text LONGBLOB NOT NULL,
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
            message_id VARCHAR(255) NOT NULL,
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
            message_id VARCHAR(255) PRIMARY KEY,
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
        connection = mysql.connector.connect(**config)
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
        connection = mysql.connector.connect(**config)
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
    parser = argparse.ArgumentParser(description="Initialize messaging database schema")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Only verify existing tables, do not create",
    )
    parser.add_argument("--drop-and-recreate", action="store_true", help="Drop and recreate the database (DANGEROUS: all data will be lost!)")
    parser.add_argument(
        "--site-id", required=True, help="Site ID for ConfigInfo database configuration (e.g., RCSB, PDBe, PDBj, BMRB)"
    )
    parser.add_argument("--host", help="Database host (overrides ConfigInfo)")
    parser.add_argument("--port", type=int, help="Database port (overrides ConfigInfo)")
    parser.add_argument("--user", help="Database user (overrides ConfigInfo)")
    parser.add_argument("--password", help="Database password (overrides ConfigInfo)")
    parser.add_argument("--database", help="Database name (overrides ConfigInfo)")

    args = parser.parse_args()

    # Get configuration from ConfigInfo
    try:
        config = get_database_config_from_configinfo(args.site_id)
    except Exception as e:
        logger.error(f"Failed to get database configuration from ConfigInfo: {e}")
        logger.error("You may need to provide database parameters via command line arguments")
        sys.exit(1)

    # Override with command line arguments if provided
    if args.host:
        config["host"] = args.host
        logger.info(f"Overriding host with command line value: {args.host}")
    if args.port:
        config["port"] = args.port
        logger.info(f"Overriding port with command line value: {args.port}")
    if args.user:
        config["user"] = args.user
        logger.info(f"Overriding user with command line value: {args.user}")
    if args.password:
        config["password"] = args.password
        logger.info("Overriding password with command line value")
    if args.database:
        config["database"] = args.database
        logger.info(f"Overriding database with command line value: {args.database}")

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
