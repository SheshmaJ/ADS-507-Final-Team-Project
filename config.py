"""
Configuration module for database connections.
Reads database credentials from environment variables.
"""

import os
from sqlalchemy import create_engine

from dotenv import load_dotenv
load_dotenv()

def get_engine():
    """Create and return a SQLAlchemy engine using environment variables."""
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    db = os.getenv("DB_NAME")

    if not user or not password or not host or not port or not db:
        raise ValueError("DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, and DB_NAME must be set as environment variables.")

    conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}"
    return create_engine(conn_str, pool_pre_ping=True)
