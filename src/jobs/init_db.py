import os
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def create_database():
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "real_estate_db")

    try:
        # Connect to the default 'postgres' database to create a new database
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname="postgres"
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # Check if database exists
        cur.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}';")
        exists = cur.fetchone()
        
        if not exists:
            print(f"Creating database {db_name}...")
            cur.execute(f"CREATE DATABASE {db_name};")
            print(f"Database {db_name} created successfully.")
        else:
            print(f"Database {db_name} already exists.")
            
        cur.close()
        conn.close()
        
        # Connect to the new database and initialize the schema
        print("Initializing schema...")
        conn = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=db_name
        )
        cur = conn.cursor()
        
        schema_path = os.path.join(os.path.dirname(__file__), '..', '..', 'sql', 'schema.sql')
        with open(schema_path, 'r') as f:
            schema_sql = f.read()
            
        cur.execute(schema_sql)
        conn.commit()
        print("Schema initialized successfully.")
        
        cur.close()
        conn.close()

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    create_database()
