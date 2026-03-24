import mysql.connector
from mysql.connector import pooling
from config import *

connection_pool = pooling.MySQLConnectionPool(
    pool_name="main_pool",
    pool_size=30,
    **DB_CONFIG
)

def make_connection():
    return connection_pool.get_connection()

def create_table(cursor:any,Tab1:str,Tab2:str):
    # cursor.execute(f"""
    # CREATE TABLE IF NOT EXISTS {Tab1}(
    #     id INT AUTO_INCREMENT PRIMARY KEY,
    #     Country VARCHAR(100),
    #     Url TEXT,
    #     Status VARCHAR(10)
          
    # )
    # """)

    #rehions table

    # cursor.execute(f"""
    # CREATE TABLE IF NOT EXISTS {Tab2}(
    #     id INT AUTO_INCREMENT PRIMARY KEY,
    #     Region VARCHAR(100),
    #     Url TEXT,
    #     Status VARCHAR(10)
          
    # )
    # """)


    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sub_regions(
        id INT AUTO_INCREMENT PRIMARY KEY,
        Sub_Region VARCHAR(100),
        Url TEXT,
        Status VARCHAR(20) 
                       )
""")

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sub_sub_regions(
            id INT AUTO_INCREMENT PRIMARY KEY,
            Sub_Sub_Region VARCHAR(100),
            Url TEXT,
            Status VARCHAR(20) 
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS pincodes_table(
            id INT AUTO_INCREMENT PRIMARY KEY,
            Region VARCHAR(100),
            Pincode TEXT 
        )
    """)

def insert_into_db(cursor, con, data, Tab, batch_size=100):

    if not data:
        return

    cols = list(data[0].keys())
    col_str = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))

    query = f"INSERT INTO {Tab} ({col_str}) VALUES ({placeholders});"

    values = [tuple(row.get(col) for col in cols) for row in data]

    try:
        for i in range(0, len(values), batch_size):
            batch = values[i:i + batch_size]
            cursor.executemany(query, batch)
            con.commit()
            print(f"Inserted batch {i} → {i + len(batch)}")

    except Exception as e:
        con.rollback()
        print(f"Insert error: {e}")



def fetch_urls(tab, *columns, status="pending"):
    con = make_connection()
    cursor = con.cursor()

    col_str = ", ".join(columns)
    query = f"SELECT {col_str} FROM {tab} WHERE status = %s"

    try:
        cursor.execute(query, (status,))
        rows = cursor.fetchall()
        print(f"Rows fetched: {len(rows)}")
        return rows
    except Exception as e:
        print(f"Fetch error: {e}")
        return []
    finally:
        cursor.close()
        con.close()


def update_q(tab, filter_col, filter_val, status="responded"):
    con = make_connection()
    cursor = con.cursor()

    query = f"UPDATE {tab} SET status = %s WHERE {filter_col} = %s"

    try:
        cursor.execute(query, (status, filter_val))
        con.commit()
    except Exception as e:
        print(f"Update error: {e}")
    finally:
        cursor.close()
        con.close()        
