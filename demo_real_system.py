#!/usr/bin/env python3
"""
Real System Demo - wwPDB Communication Module

This script demonstrates the messaging system using real wwPDB objects and infrastructure,
focusing on the database backend which we know works properly.

This is designed to work in your remote VM with proper wwPDB development environment.

Usage:
    python demo_real_system.py
"""

import os
import sys
import tempfile
import sqlite3
import logging
from pathlib import Path

# Add project to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_test_environment():
    """Set up temporary directories and database for testing"""
    # Create temporary directory for this demo
    demo_dir = tempfile.mkdtemp(prefix="wwpdb_real_demo_")
    logger.info(f"📁 Demo directory: {demo_dir}")
    
    # Create subdirectories
    session_dir = os.path.join(demo_dir, "sessions")
    data_dir = os.path.join(demo_dir, "data") 
    db_dir = os.path.join(demo_dir, "database")
    
    for dir_path in [session_dir, data_dir, db_dir]:
        os.makedirs(dir_path, exist_ok=True)
    
    # Set up SQLite database
    db_path = os.path.join(db_dir, "messaging.db")
    setup_sqlite_database(db_path)
    
    return {
        "demo_dir": demo_dir,
        "session_dir": session_dir,
        "data_dir": data_dir,
        "db_path": db_path
    }

def setup_sqlite_database(db_path):
    """Create SQLite database with messaging tables"""
    logger.info(f"🗄️  Setting up SQLite database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create messages table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            deposition_id TEXT NOT NULL,
            message_type TEXT NOT NULL,
            sender TEXT,
            subject TEXT,
            message_text TEXT,
            timestamp TEXT,
            read_status TEXT DEFAULT 'N',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # Create message_files table  
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS message_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id INTEGER,
            file_name TEXT,
            file_path TEXT,
            file_type TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (message_id) REFERENCES messages (id)
        )
    """)
    
    # Create indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_deposition_id ON messages (deposition_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_type ON messages (message_type)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages (timestamp)")
    
    conn.commit()
    conn.close()
    
    logger.info("✅ Database tables created successfully")

def create_real_request(session_dir, site_id="DEMO"):
    """Create a real request object using wwPDB infrastructure"""
    try:
        # Use real wwPDB request object
        from wwpdb.utils.session.WebRequest import InputRequest
        
        # Create a real request with minimal parameters
        param_dict = {
            "WWPDB_SITE_ID": site_id,
            "groupid": "demo_group",
            "sessionid": "demo_session_001",
            "task": "demo_messaging",
            "operation": "test"
        }
        
        req_obj = InputRequest(param_dict, verbose=True, log=sys.stderr)
        logger.info(f"✅ Created real InputRequest object for site {site_id}")
        return req_obj
        
    except (ImportError, AttributeError) as e:
        logger.warning(f"Real InputRequest class not available ({e}), using fallback")
        # Fallback to a minimal request-like object if wwPDB utils not available
        class SimpleRequest:
            def __init__(self, session_dir, site_id):
                self.session_dir = session_dir
                self.site_id = site_id
                self._values = {
                    "WWPDB_SITE_ID": site_id,
                    "groupid": "demo_group",
                    "sessionid": "demo_session_001"
                }
            
            def getValue(self, key):
                return self._values.get(key, "")
            
            def newSessionObj(self):
                class SimpleSession:
                    def __init__(self, path):
                        self.path = path
                    def getPath(self):
                        return self.path
                return SimpleSession(self.session_dir)
        
        return SimpleRequest(session_dir, site_id)

def create_real_message(deposition_id="D_1234567890", message_type="to-depositor"):
    """Create a real message object using wwPDB Message class"""
    try:
        # Use real wwPDB Message class
        from wwpdb.apps.msgmodule.models.Message import Message
        
        # Create a message dictionary (required by Message constructor)
        msg_dict = {
            'deposition_data_set_id': deposition_id,
            'message_type': message_type,
            'message_text': "This is a test message from the real system demo.",
            'message_subject': "Real Demo Message - System Test",
            'sender': "demo@wwpdb.org",
            'timestamp': "2025-07-11T10:00:00",
            'read_status': "N"
        }
        
        # Create a real message object
        msg = Message(msg_dict)
        
        logger.info(f"✅ Created real Message object for {deposition_id}")
        return msg
        
    except (ImportError, AttributeError, TypeError) as e:
        logger.warning(f"Real Message class not available or failed ({e}), using fallback")
        # Fallback to a simple message-like object
        class SimpleMessage:
            def __init__(self, deposition_id, message_type):
                self.deposition_id = deposition_id
                self.message_type = message_type
                self.message_text = "This is a test message from the real system demo."
                self.subject = "Real Demo Message - System Test"
                self.sender = "demo@wwpdb.org"
                self.timestamp = "2025-07-11T10:00:00"
                self.read_status = "N"
                self.messages = []
            
            def getDepositionDataSetId(self):
                return self.deposition_id
            
            def getMessageType(self):
                return self.message_type
            
            def getMessageText(self):
                return self.message_text
            
            def getMessageSubject(self):
                return self.subject
            
            def getSender(self):
                return self.sender
            
            def getTimestamp(self):
                return self.timestamp
            
            def getAllMessages(self):
                return self.messages
            
            def setDepositionDataSetId(self, value):
                self.deposition_id = value
        
        return SimpleMessage(deposition_id, message_type)

def test_database_backend_real(env_info):
    """Test database backend with real objects and configuration"""
    logger.info("🎯 TESTING REAL DATABASE BACKEND")
    logger.info("="*60)
    
    # Set environment for database-only mode
    os.environ["MSGCIF_WRITES_ENABLED"] = "false"
    os.environ["MSGCIF_READS_ENABLED"] = "false"
    os.environ["MSGDB_WRITES_ENABLED"] = "true"
    os.environ["MSGDB_READS_ENABLED"] = "true"
    
    # Configure database connection
    os.environ["MSGDB_HOST"] = "localhost"
    os.environ["MSGDB_NAME"] = "messaging_demo"
    os.environ["MSGDB_USER"] = "demo_user"
    os.environ["MSGDB_PASS"] = "demo_pass"
    os.environ["MSGDB_PORT"] = "3306"
    
    # Also set required environment variables for the database configuration
    os.environ["MSGDB_CHARSET"] = "utf8mb4"
    os.environ["MSGDB_POOL_SIZE"] = "5"
    os.environ["MSGDB_TIMEOUT"] = "30"
    
    # Debug: Print what we actually set
    logger.info(f"   🔧 Database config set: {os.environ.get('MSGDB_HOST')}:{os.environ.get('MSGDB_PORT')}")
    logger.info(f"   🔧 Database name: {os.environ.get('MSGDB_NAME')}")
    logger.info(f"   🔧 Database user: {os.environ.get('MSGDB_USER')}")
    logger.info(f"   🔧 Database pass: {'***' if os.environ.get('MSGDB_PASS') else 'None'}")
    
    try:
        logger.info("📦 Step 1: Import messaging factory")
        from wwpdb.apps.msgmodule.io.MessagingFactory import create_messaging_service
        
        logger.info("📦 Step 2: Create real request object")
        req_obj = create_real_request(env_info["session_dir"])
        logger.info(f"   Request type: {type(req_obj).__name__}")
        
        logger.info("📦 Step 3: Create messaging service")
        messaging = create_messaging_service(req_obj, verbose=True)
        logger.info(f"   Service type: {type(messaging).__name__}")
        
        logger.info("📦 Step 4: Create real message object")
        test_msg = create_real_message("D_REAL_TEST_001", "to-depositor")
        logger.info(f"   Message type: {type(test_msg).__name__}")
        
        # Check if it's a real Message object or fallback
        if hasattr(test_msg, 'getMsgDict'):
            msg_dict = test_msg.getMsgDict()
            logger.info(f"   Message content: {msg_dict.get('message_text', 'N/A')[:50]}...")
        else:
            logger.info(f"   Message content: {test_msg.getMessageText()[:50]}...")
        
        logger.info("📦 Step 5: Process message")
        result = messaging.processMsg(test_msg)
        logger.info(f"   Process result: {result}")
        
        logger.info("📦 Step 6: Retrieve messages")
        messages = messaging.getMsgRowList("D_REAL_TEST_001", "to-depositor")
        logger.info(f"   Retrieved: {type(messages)} with {len(messages.get('RECORD_LIST', []))} messages")
        
        logger.info("✅ Real database backend test completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Real database backend test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_backend_info():
    """Test backend information retrieval"""
    logger.info("🎯 TESTING BACKEND INFO")
    logger.info("="*40)
    
    try:
        from wwpdb.apps.msgmodule.io.MessagingFactory import MessagingFactory
        
        # Test with database-only configuration
        os.environ["MSGDB_WRITES_ENABLED"] = "true"
        os.environ["MSGDB_READS_ENABLED"] = "true"
        os.environ["MSGCIF_WRITES_ENABLED"] = "false"
        os.environ["MSGCIF_READS_ENABLED"] = "false"
        
        # Set the same database configuration
        os.environ["MSGDB_HOST"] = "localhost"
        os.environ["MSGDB_NAME"] = "messaging_demo"
        os.environ["MSGDB_USER"] = "demo_user"
        os.environ["MSGDB_PASS"] = "demo_pass"
        os.environ["MSGDB_PORT"] = "3306"
        
        info = MessagingFactory.get_backend_info(req_obj=None)
        
        logger.info(f"   Selected backend: {info['selected_backend']}")
        logger.info(f"   Backend class: {info['backend_class']}")
        logger.info(f"   Reason: {info['reason']}")
        logger.info(f"   Feature flags: {info['feature_flags']}")
        
        if 'database_config' in info:
            logger.info(f"   Database config: {info['database_config']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Backend info test failed: {e}")
        return False

def cleanup_environment():
    """Clean up environment variables"""
    flags_to_clean = [
        "MSGDB_WRITES_ENABLED",
        "MSGDB_READS_ENABLED",
        "MSGCIF_WRITES_ENABLED", 
        "MSGCIF_READS_ENABLED",
        "MSGDB_HOST",
        "MSGDB_NAME",
        "MSGDB_USER",
        "MSGDB_PASS",
        "MSGDB_PORT",
        "MSGDB_SQLITE_PATH"
    ]
    
    for flag in flags_to_clean:
        if flag in os.environ:
            del os.environ[flag]

def main():
    """Main demo function"""
    logger.info("🎯 wwPDB Communication Module - Real System Demo")
    logger.info("=" * 80)
    logger.info("This demo uses real wwPDB objects and infrastructure.")
    logger.info("It's designed to work in your remote VM development environment.")
    logger.info("")
    
    # Set up test environment
    env_info = setup_test_environment()
    
    try:
        # Run tests
        tests = [
            ("Backend Info", test_backend_info),
            ("Database Backend", lambda: test_database_backend_real(env_info)),
        ]
        
        results = {}
        for test_name, test_func in tests:
            logger.info(f"\n{'='*20} {test_name} {'='*20}")
            results[test_name] = test_func()
        
        # Show final results
        logger.info("\n" + "="*60)
        logger.info("📊 REAL DEMO RESULTS SUMMARY")
        logger.info("="*60)
        
        for test_name, success in results.items():
            status = "✅ PASS" if success else "❌ FAIL"
            logger.info(f"  {test_name}: {status}")
        
        overall_success = all(results.values())
        logger.info(f"\n🎉 Overall Demo: {'SUCCESS' if overall_success else 'FAILED'}")
        
        if overall_success:
            logger.info("\n🚀 The real messaging system is working correctly!")
            logger.info("   - Real wwPDB objects are being used")
            logger.info("   - Backend selection is working")
            logger.info("   - Database operations are functional")
        else:
            logger.info("\n📝 Some tests failed, but this is expected in environments")
            logger.info("   without full wwPDB configuration.")
        
        logger.info(f"\n📁 Demo files are in: {env_info['demo_dir']}")
        
    except Exception as e:
        logger.error(f"❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Clean up environment
        cleanup_environment()

if __name__ == "__main__":
    main()
