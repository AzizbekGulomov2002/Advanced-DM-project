"""
Environment-driven connection settings for Slide_ADM(3).pdf project:
  Part 1 — PostgreSQL + MongoDB (+ Spark jobs use these)
  Part 2 — PostgreSQL + Neo4j (+ Spark GraphFrames / RDD jobs)

Loads `.env` from the project root (parent of `config/`) so scripts work regardless of cwd.
Install `python-dotenv` in the venv: `pip install python-dotenv`
"""
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ENV_FILE = _PROJECT_ROOT / ".env"

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None  # type: ignore[misc, assignment]

if load_dotenv is not None:
    load_dotenv(_ENV_FILE)
else:
    # Without python-dotenv, only OS environment variables apply (defaults below).
    pass

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "company_project")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "company_project")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "")
