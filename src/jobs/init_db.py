import os

from src.database.connection import create_sql_server_engine
from src.database.models import Base


def create_database():
    db_name = os.getenv("DB_NAME", "real_estate_db")

    try:
        print(f"Initializing schema in SQL Server database '{db_name}'...")
        engine = create_sql_server_engine()
        Base.metadata.create_all(engine)

        print("Schema initialized successfully via SQLAlchemy ORM.")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    create_database()
