import os
import sqlite3
from dotenv import load_dotenv

try:
    import psycopg
    from psycopg_pool import ConnectionPool
    PSYCOPG_INSTALLED = True
except ImportError:
    PSYCOPG_INSTALLED = False

# Load local environment vars primarily looking for PG_URI
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def seed_sqlite():
    print("Seeding local SQLite Sandbox...")
    db_path = os.path.join(os.path.dirname(__file__), '..', 'sandbox.db')
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS website_events")
            cur.execute('''
                CREATE TABLE website_events (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    event_type TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insert tracking events matching DAB structure
            cur.execute("INSERT INTO website_events (user_id, event_type) VALUES (1, 'page_view')")
            cur.execute("INSERT INTO website_events (user_id, event_type) VALUES (2, 'click')")
            cur.execute("INSERT INTO website_events (user_id, event_type) VALUES (1, 'checkout_start')")
            
            conn.commit()
            print(f" -> SQLite seeded successfully at {db_path}")
    except Exception as e:
        print(f" -> SQLite seeding failed: {e}")

def seed_postgres():
    print("Seeding PostgreSQL Sandbox...")
    
    if not PSYCOPG_INSTALLED:
        print(" -> Skipping Postgres: 'psycopg' module is not installed in your current environment.")
        return
        
    pg_uri = os.getenv("PG_URI", "")
    if not pg_uri:
        print(" -> Skipping Postgres: No PG_URI found in environment variables.")
        return
        
    try:
        with psycopg.connect(pg_uri) as conn:
            with conn.cursor() as cur:
                # Warning: Destructive drops to ensure clean schema states
                cur.execute("DROP TABLE IF EXISTS orders;")
                cur.execute("DROP TABLE IF EXISTS users;")
                
                cur.execute('''
                    CREATE TABLE users (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(100),
                        signup_date DATE
                    )
                ''')
                cur.execute('''
                    CREATE TABLE orders (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id),
                        details VARCHAR(255),
                        campaign VARCHAR(50)
                    )
                ''')
                
                # Mock Business Data 
                cur.execute("INSERT INTO users (name, signup_date) VALUES ('Alice', '2023-01-15')")
                cur.execute("INSERT INTO users (name, signup_date) VALUES ('Bob', '2023-06-20')")
                
                # Insert data specifically triggering "Black Friday" rule test
                cur.execute("INSERT INTO orders (user_id, details, campaign) VALUES (1, 'Macbook Air', 'Black Friday')")
                cur.execute("INSERT INTO orders (user_id, details, campaign) VALUES (2, 'Airpods', 'Holiday Promo')")
                
            conn.commit()
            print(" -> Postgres Seeded Successfully with 'users' and 'orders' tables.")
    except Exception as e:
        print(f" -> Postgres Seeding Error: {e}")

if __name__ == "__main__":
    print("====================================")
    print("    FORGE AGENT V3 - DAB DB SEED    ")
    print("====================================\n")
    seed_sqlite()
    seed_postgres()
    print("\nDatabase initialization stream finished.")
