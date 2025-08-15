"""
Data Access Layer for messaging system.

Streamlined design with bulk operations, minimal round-trips, and idempotent inserts.
"""

import logging
from contextlib import contextmanager
from typing import Iterable, Set, List, Dict

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.exc import SQLAlchemyError

from .Models import Base, MessageInfo, MessageFileReference, MessageStatus

logger = logging.getLogger(__name__)

class DataAccessLayer:
    def __init__(self, db_config: Dict):
        # mysql+pymysql://user:pass@host:port/db?charset=utf8mb4
        conn = (
            f"mysql+pymysql://{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['database']}?charset={db_config.get('charset','utf8mb4')}"
        )
        self.engine = create_engine(
            conn, pool_pre_ping=True, pool_size=db_config.get("pool_size", 10), max_overflow=20, echo=False
        )
        self.Session = sessionmaker(bind=self.engine)

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    # ---------- Bulk helpers ----------

    def messages_exist(self, session, ids: Iterable[str]) -> Set[str]:
        ids = list({i for i in ids if i})
        if not ids:
            return set()
        q = session.query(MessageInfo.message_id).filter(MessageInfo.message_id.in_(ids))
        return {row[0] for row in q.all()}

    def bulk_insert_messages_ignore(self, session, rows: List[MessageInfo]):
        """PK is message_id. INSERT IGNORE semantics via ON DUPLICATE no-op."""
        if not rows:
            return
        table = MessageInfo.__table__
        payload = [self._row_from_model(m) for m in rows]
        stmt = mysql_insert(table).values(payload).prefix_with("IGNORE")
        session.execute(stmt)

    def bulk_insert_file_refs_ignore(self, session, rows: List[MessageFileReference]):
        if not rows:
            return
        table = MessageFileReference.__table__
        payload = [self._row_from_model(r) for r in rows]
        stmt = mysql_insert(table).values(payload).prefix_with("IGNORE")
        session.execute(stmt)

    def bulk_upsert_statuses(self, session, rows: List[MessageStatus]):
        """Status likely has unique key on message_id; upsert latest values."""
        if not rows:
            return
        table = MessageStatus.__table__
        payload = [self._row_from_model(s) for s in rows]
        stmt = mysql_insert(table).values(payload)
        update_cols = {c.name: stmt.inserted[c.name] for c in table.columns if c.name not in ("id",)}
        stmt = stmt.on_duplicate_key_update(**update_cols)
        session.execute(stmt)

    @staticmethod
    def _row_from_model(model_obj):
        # Convert SQLAlchemy model instance -> dict suitable for Core insert
        d = {}
        for col in model_obj.__table__.columns:
            if col.name in model_obj.__dict__:
                d[col.name] = getattr(model_obj, col.name)
        return d

    def close(self):
        try:
            self.engine.dispose()
        except Exception:
            pass
