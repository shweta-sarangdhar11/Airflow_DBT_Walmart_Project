import os
from pathlib import Path

import psycopg2


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = Path(__file__).resolve().parent / "data"


def load_connection_string():
    """Load POSTGRES_CONNECTION from the project .env without exposing it."""
    connection_string = os.environ.get("POSTGRES_CONNECTION")
    if connection_string:
        return connection_string

    env_path = PROJECT_ROOT / ".env"
    for encoding in ("utf-8", "utf-16"):
        try:
            env_text = env_path.read_text(encoding=encoding)
            break
        except (FileNotFoundError, UnicodeError):
            continue
    else:
        raise RuntimeError(f"Unable to read {env_path}")

    for line in env_text.splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() == "POSTGRES_CONNECTION":
            return value.strip().strip('"').strip("'")

    raise RuntimeError("POSTGRES_CONNECTION was not found in the project .env")


# CSV files mapping to tables
csv_files = {
    "customers.csv": "raw.customers",
    "stores.csv": "raw.stores",
    "products.csv": "raw.products",
    "employees.csv": "raw.employees",
    "orders.csv": "raw.orders",
    "order_items.csv": "raw.order_items",
}

conn = None
try:
    conn = psycopg2.connect(load_connection_string())
    cursor = conn.cursor()
    
    # Load each CSV file into its corresponding table
    for csv_file, table_name in csv_files.items():
        csv_path = DATA_DIR / csv_file
        
        if os.path.exists(csv_path):
            print(f"Loading {csv_file} into {table_name}...")
            
            with csv_path.open("r", encoding="utf-8", newline="") as f:
                cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
            
            conn.commit()
            print(f"✓ Successfully loaded {csv_file}")
        else:
            print(f"✗ File not found: {csv_path}")
    
    cursor.close()
    conn.close()
    print("\n✓ All data loaded successfully!")
    
except Exception as e:
    print(f"Error: {e}")
    if conn is not None:
        conn.rollback()
        conn.close()
