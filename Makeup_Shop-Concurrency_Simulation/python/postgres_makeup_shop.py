import psycopg2
import threading
import time

pg_conn = psycopg2.connect(
    dbname="Makeup_Shop",
    user="Janine",
    password="janine",
    host="localhost",
    port="5432"
)
pg_conn.autocommit = False  # Disable autocommit for manual transaction control

# PostgreSQL does NOT allow Dirty Read, so this test is not necessary! nu accepta read uncommited postgres

def write_skew():
    """ Simulates Write Skew """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    print("\n[Session 1] Checking available suppliers...")
    cursor.execute("SELECT COUNT(*) FROM suppliers;")
    supplier_count = cursor.fetchone()[0]
    print("[Session 1] Number of suppliers:", supplier_count)

    def second_booking():
        time.sleep(1)
        cursor2 = pg_conn.cursor()
        cursor2.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

        cursor2.execute("SELECT COUNT(*) FROM suppliers;")
        supplier_count2 = cursor2.fetchone()[0]
        print("[Session 2] Number of suppliers:", supplier_count2)

        if supplier_count2 < 5:
            cursor2.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (10, 'New Supplier');")
            pg_conn.commit()
            print("[Session 2] Added a new supplier!")

        cursor2.close()

    t = threading.Thread(target=second_booking)
    t.start()

    time.sleep(2)

    if supplier_count < 5:
        cursor.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (11, 'Another Supplier');")
        pg_conn.commit()
        print("[Session 1] Added another supplier!")

    cursor.close()


def deadlock():
    """ Simulates Deadlock """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    print("\n[Session 1] Locking Artists...")
    cursor.execute("SELECT * FROM suppliers WHERE supplierid = 1 FOR UPDATE;")

    def second_transaction():
        time.sleep(1)
        cursor2 = pg_conn.cursor()
        cursor2.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

        print("[Session 2] Locking makeup_products...")
        cursor2.execute("SELECT * FROM makeup_products WHERE productid = 1 FOR UPDATE;")

        time.sleep(2)

        print("[Session 2] Trying to lock Artists...")
        cursor2.execute("SELECT * FROM suppliers WHERE supplierid = 1 FOR UPDATE;")  # Aici apare deadlock-ul
        pg_conn.commit()
        cursor2.close()

    t = threading.Thread(target=second_transaction)
    t.start()

    time.sleep(2)

    print("[Session 1] Trying to lock Albums...")
    cursor.execute("SELECT * FROM makeup_products WHERE productid = 1 FOR UPDATE;")  # Aici apare deadlock-ul

    pg_conn.commit()
    cursor.close()


def unrepeatable_read():
    """ Simulates Unrepeatable Read """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Initial read...")
    cursor.execute("SELECT categoryname FROM categories WHERE categoryid = 1;")
    initial_category = cursor.fetchone()
    print("[Session 1] Initial date:", initial_category)

    def update_category():
        time.sleep(1)
        cursor2 = pg_conn.cursor()
        cursor2.execute("UPDATE categories SET categoryname = 'Luxury Makeup' WHERE categoryid = 1;")
        pg_conn.commit()
        print("[Session 2] Updated the category name to 'Luxury Makeup'")
        cursor2.close()

    t = threading.Thread(target=update_category)
    t.start()

    time.sleep(2)

    print("[Session 1] Read after modification...")
    cursor.execute("SELECT categoryname FROM categories WHERE categoryid = 1;")
    new_category = cursor.fetchone()
    print("[Session 1] Category name after modification:", new_category)

    cursor.close()
    pg_conn.commit()

def phantom_read():
    """ Simulates Phantom Read with locking """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

    print("\n[Session 1] Initial read (with LOCKING)...")

    # ✅ SELECT without COUNT(*), but with FOR UPDATE
    cursor.execute("SELECT * FROM makeup_products WHERE brand = 'Maybelline' FOR UPDATE;")
    initial_rows = cursor.fetchall()
    print("[Session 1] Initial number of Maybelline products:", len(initial_rows))

    def insert_new_product():
        time.sleep(2)
        cursor2 = pg_conn.cursor()

        # ✅ Using user_id = 1, which already exists
        cursor2.execute("INSERT INTO makeup_products (productid, name, brand, price) VALUES (100, 'New Lipstick', 'Maybelline', 15.99);")
        pg_conn.commit()
        print("[Session 2] Added a new Maybelline product.")
        cursor2.close()

    t = threading.Thread(target=insert_new_product)
    t.start()

    time.sleep(3)

    print("[Session 1] Read after modification...")
    cursor.execute("SELECT * FROM makeup_products WHERE brand = 'Maybelline';")
    new_rows = cursor.fetchall()
    print("[Session 1] Final number of Maybelline products:", len(new_rows))

    cursor.close()
    pg_conn.commit()

def lost_update():
    """ Simulates Lost Update """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Initial read...")
    cursor.execute("SELECT price FROM makeup_products WHERE productid = 1;")
    initial_price = cursor.fetchone()
    print("[Session 1] Initial price:", initial_price)

    def update_price():
        time.sleep(1)
        cursor2 = pg_conn.cursor()
        cursor2.execute("UPDATE makeup_products SET price = 20.99 WHERE productid = 1;")
        pg_conn.commit()
        print("[Session 2] Updated the price to 20.99")
        cursor2.close()

    t = threading.Thread(target=update_price)
    t.start()

    time.sleep(2)

    print("[Session 1] Updating to 'Disease'")
    cursor.execute("UPDATE makeup_products SET price = 18.50 WHERE productid = 1;")
    pg_conn.commit()

    print("[Session 1] Final price:", initial_price)

    cursor.close()

def uncommitted_dependency():
    """ Simulates Uncommitted Dependency """
    cursor = pg_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Insert supplier without commit...")

    cursor.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (12, 'Temporary Supplier');")

    def read_uncommitted():
        time.sleep(1)
        cursor2 = pg_conn.cursor()
        cursor2.execute("SELECT * FROM suppliers;")
        print("[Session 2] Read suppliers:", cursor2.fetchall())
        cursor2.close()

    t = threading.Thread(target=read_uncommitted)
    t.start()

    time.sleep(2)
    print("[Session 1] Performing ROLLBACK...")
    pg_conn.rollback()

    cursor.close()

# Running tests in PostgreSQL
# print("\n=== Test Unrepeatable Read ===")
# unrepeatable_read()
# print("\n=== Test Phantom Read ===")
# phantom_read()
# print("\n=== Test Lost Update ===")
# lost_update()
# print("\n=== Test Uncommitted Dependency ===")
# uncommitted_dependency()
# print("\n=== Test Write Skew ===")
# write_skew()
# print("\n=== Test Deadlock ===")
# deadlock()