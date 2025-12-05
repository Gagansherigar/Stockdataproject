import psycopg2
from api_request import fetch_data
import os


def connect_to_db():
    print("Connecting to the PostgresSQL database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "db"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            dbname=os.getenv("POSTGRES_DB", "db"),
            user=os.getenv("POSTGRES_USER", "db_user"),
            password=os.getenv("POSTGRES_PASSWORD", "db_password")
        )
        return(conn)
    except psycopg2.error as e:
        print(f"Database connection failed{e}")
        raise

def create_table(conn):
    print("Creating table if not exists")
    try:
        cursor=conn.cursor()
        cursor.execute("""
        CREATE SCHEMA IF NOT EXISTS dev;

        CREATE TABLE IF NOT EXISTS dev.stock_data (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(16) NOT NULL,
            timestamp TIMESTAMPTZ NOT NULL,
            interval VARCHAR(16),
            source VARCHAR(64),
            open NUMERIC(14, 6),
            high NUMERIC(14, 6),
            low NUMERIC(14, 6),
            close NUMERIC(14, 6),
            volume BIGINT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            UNIQUE(symbol, timestamp, interval, source)
        );

        CREATE INDEX IF NOT EXISTS idx_stock_symbol_time
        ON dev.stock_data (symbol, timestamp DESC);""")
        conn.commit()
        print("Table was created")
    except psycopg2.Error as e:
        print(f"failed to create table:{e}")
        raise
        

conn=connect_to_db()
create_table(conn)

def insert_records(conn, data):
    print("inserting stock data into database....")
    try:
        cursor = conn.cursor()

        meta = data['Meta Data']
        symbol = meta['2. Symbol']
        interval = meta['4. Interval']
        source = 'alpha_vantage'

        time_series = data['Time Series (5min)']

        for timestamp, values in time_series.items():
            open_price  = float(values['1. open'])
            high_price  = float(values['2. high'])
            low_price   = float(values['3. low'])
            close_price = float(values['4. close'])
            volume      = int(values['5. volume'])

            cursor.execute("""
                INSERT INTO dev.stock_data (
                    symbol,
                    timestamp,
                    interval,
                    source,
                    open,
                    high,
                    low,
                    close,
                    volume
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (symbol, timestamp, interval, source)
                DO NOTHING;""", (
                symbol,
                timestamp,
                interval,
                source,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            ))

        conn.commit()
        print("data successfully inserted")
    except psycopg2.Error as e:
        print(f"Error inserting data into database{e}")
        raise

def main():
    try:
        conn = connect_to_db()
        create_table(conn)

        symbols = os.getenv("SYMBOLS", "IBM").split(",")

        for sym in symbols:
            sym = sym.strip().upper()
            print(f"\n=== FETCHING {sym} ===")
            data = fetch_data(sym)
            insert_records(conn, data)

    except Exception as e:
        print(f"An error occurred during main(): {e}")

    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")



main()




