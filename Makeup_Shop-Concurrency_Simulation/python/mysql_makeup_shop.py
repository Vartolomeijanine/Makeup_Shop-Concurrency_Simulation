import mysql.connector
import threading
import time

mysql_conn = mysql.connector.connect(
    host="localhost",
    user="Janine",
    password="janine",
    database="Makeup_Shop"
)
mysql_conn.autocommit = False  # Disable autocommit to manually control transactions
# print("Connected to database!")

# write skew apare cand doua tranzactii citesc aceeasi informatie, iau decizii pe baza ei si apoi fac modificari fara sa vada modificarile celeilalte
def write_skew():
    """ Simulates Write Skew """

    cursor = mysql_conn.cursor()

    # Set isolation level before starting the transactions
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    print("\n[Session 1] Checking available suppliers...")
    cursor.execute("SELECT COUNT(*) FROM suppliers;")
    supplier_count = cursor.fetchone()[0]
    print("[Session 1] Number of suppliers:", supplier_count)

    def add_supplier():
        time.sleep(1)
        cursor2 = mysql_conn.cursor()

        cursor2.execute("START TRANSACTION;")

        # Get the max supplierid and add +1
        cursor2.execute("SELECT MAX(supplierid) FROM suppliers;")
        max_supplier_id = cursor2.fetchone()[0] or 0  # If no suppliers exist, start from 1
        new_supplier_id = max_supplier_id + 1

        print(f"[Session 2] Next available supplier ID: {new_supplier_id}")

        cursor2.execute("SELECT COUNT(*) FROM suppliers;")
        supplier_count2 = cursor2.fetchone()[0]
        print("[Session 2] Number of suppliers:", supplier_count2)

        if supplier_count2 < 5:
            cursor2.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (%s, 'New Supplier');",
                            (new_supplier_id,))
            mysql_conn.commit()  # Commit the transaction
            print("[Session 2] Added a new supplier!")

        cursor2.close()

    # Start the second transaction in a separate thread
    t = threading.Thread(target=add_supplier)
    t.start()

    time.sleep(2)

    # Continue with Session 1's transaction
    cursor.execute("START TRANSACTION;")

    # Get the max supplierid and add +1
    cursor.execute("SELECT MAX(supplierid) FROM suppliers;")
    max_supplier_id = cursor.fetchone()[0] or 0
    new_supplier_id = max_supplier_id + 1

    print(f"[Session 1] Next available supplier ID: {new_supplier_id}")

    if supplier_count < 5:
        cursor.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (%s, 'Another Supplier');",
                       (new_supplier_id,))
        mysql_conn.commit()
        print("[Session 1] Added another supplier!")

    cursor.close()

# deadlock apare cand doua tranzactii blocheaza resurse diferite si fiecare asteapta ca cealalta sa elibereze resursa, ceea ce duce la blocaj complet
def deadlock():
    """ Simulates Deadlock """

    # Set isolation level once at the start (before transactions)
    mysql_conn.cursor().execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE;")

    cursor = mysql_conn.cursor()
    print("\n[Session 1] Locking suppliers...")
    cursor.execute("SELECT * FROM suppliers WHERE supplierid = 1 FOR UPDATE;")
    cursor.fetchall()  # Ensure result is read

    def second_transaction():
        time.sleep(1)

        # Use a separate connection for the second session
        connection2 = mysql.connector.connect(
            host="localhost",
            user="Janine",
            password="janine",
            database="Makeup_Shop"
        )
        cursor2 = connection2.cursor()

        print("[Session 2] Locking makeup_products...")
        cursor2.execute("SELECT * FROM makeup_products WHERE productid = 1 FOR UPDATE;")
        cursor2.fetchall()  # Ensure result is read

        time.sleep(2)

        print("[Session 2] Trying to lock suppliers...")
        cursor2.execute("SELECT * FROM suppliers WHERE supplierid = 1 FOR UPDATE;")  # Deadlock occurs here
        cursor2.fetchall()  # Ensure

# dirty read apare cand o tranzactie citeste date care au fost modificate de alta tranzactie, dar care nu au fost inca confirmate, ceea ce poate duce la inconsistente
def dirty_read():
    """ Simulates Dirty Read """
    cursor = mysql_conn.cursor()

    # SET TRANSACTION ISOLATION LEVEL before starting the transaction
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;")
    cursor.execute("START TRANSACTION;")

    print("\n[Session 1] Modifying country for Artist 1 without commit...")
    cursor.execute("UPDATE suppliers SET suppliername = 'Temporary Supplier' WHERE supplierid = 1;")

    def read_dirty():
        time.sleep(1)
        cursor2 = mysql_conn.cursor()

        # Do NOT set the isolation level again, as it was already set at the beginning
        cursor2.execute("START TRANSACTION;")

        cursor2.execute("UPDATE suppliers SET suppliername = 'Temporary Supplier' WHERE supplierid = 1;")

        print("[Session 2] Read Dirty Read:", cursor2.fetchone())
        cursor2.close()

    t = threading.Thread(target=read_dirty)
    t.start()

    time.sleep(2)
    print("[Session 1] Performing ROLLBACK...")
    mysql_conn.rollback()
    cursor.close()

# unrepeatable read apare cand o tranzactie citeste aceleasi date de doua ori, dar intre cele doua citiri alta tranzactie modifica acele date, astfel incat valorile sunt diferite

def unrepeatable_read():
    """ Simulates Unrepeatable Read """
    cursor = mysql_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Initial read...")
    cursor.execute("SELECT categoryname FROM categories WHERE categoryid = 1;")
    initial_category = cursor.fetchone()
    print("[Session 1] Initial date:", initial_category)

    def update_category():
        time.sleep(1)
        cursor2 = mysql_conn.cursor()
        cursor2.execute("UPDATE categories SET categoryname = 'Luxury Makeup' WHERE categoryid = 1;")
        mysql_conn.commit()
        print("[Session 2] Updated category name to 'Luxury Makeup'")
        cursor2.close()

    t = threading.Thread(target=update_category)
    t.start()

    time.sleep(2)

    print("[Session 1] Read after modification...")
    cursor.execute("SELECT categoryname FROM categories WHERE categoryid = 1;")
    new_category = cursor.fetchone()
    print("[Session 1] Category name after modification:", new_category)

    cursor.close()
    mysql_conn.commit()

# phantom read apare cand o tranzactie citeste un set de date bazat pe o anumita conditie, dar alta tranzactie insereaza noi randuri care se potrivesc acelei conditii, astfel incat rezultatele citirii initiale se schimba
def phantom_read():
    """ Simulates Phantom Read with locking """
    cursor = mysql_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")

    print("\n[Session 1] Initial read (with LOCKING)...")
    cursor.execute("SELECT COUNT(*) FROM makeup_products WHERE brand = 'Maybelline' FOR UPDATE;")
    initial_count = cursor.fetchone()
    print("[Session 1] Initial number of Maybelline products:", initial_count)

    def insert_new_product():
        time.sleep(2)
        cursor2 = mysql_conn.cursor()
        cursor2.execute("INSERT INTO makeup_products (productid, name, brand, price) VALUES (100, 'New Lipstick', 'Maybelline', 15.99);")
        mysql_conn.commit()
        print("[Session 2] Added a new Maybelline product.")
        cursor2.close()

    t = threading.Thread(target=insert_new_product)
    t.start()

    time.sleep(3)

    print("[Session 1] Read after modification...")
    cursor.execute("SELECT COUNT(*) FROM makeup_products WHERE brand = 'Maybelline' FOR UPDATE;")
    new_count = cursor.fetchone()
    print("[Session 1] Final number of musical genres:", new_count)

    cursor.close()
    mysql_conn.commit()

# lost update apare cand doua tranzactii citesc aceeasi valoare, o modifica si apoi o salveaza, dar modificarile uneia sunt suprascrise de cealalta, ceea ce duce la pierderea unei actualizari
def lost_update():
    """ Simulates Lost Update """
    cursor = mysql_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Initial read...")
    cursor.execute("SELECT price FROM makeup_products WHERE productid = 1;")
    initial_price = cursor.fetchone()
    print("[Session 1] Initial price:", initial_price)

    def update_price():
        time.sleep(1)
        cursor2 = mysql_conn.cursor()
        cursor2.execute("UPDATE makeup_products SET price = 20.99 WHERE productid = 1;")
        mysql_conn.commit()
        print("[Session 2] Updated the title to 'Abracadabra'")
        cursor2.close()

    t = threading.Thread(target=update_price)
    t.start()

    time.sleep(2)

    print("[Session 1] Updating price to 18.50")
    cursor.execute("UPDATE makeup_products SET price = 18.50 WHERE productid = 1;")
    mysql_conn.commit()

    print("[Session 1] Final title:", initial_price)

    cursor.close()

# uncommitted dependency apare cand o tranzactie depinde de modificari facute de alta tranzactie care nu au fost inca confirmate, iar daca prima tranzactie este anulata, a doua ramane cu date inconsistente
def uncommitted_dependency():
    """ Simulates Uncommitted Dependency """
    cursor = mysql_conn.cursor()
    cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    print("\n[Session 1] Insert without commit...")
    cursor.execute("INSERT INTO suppliers (supplierid, suppliername) VALUES (12, 'Temporary Supplier');")

    def read_uncommitted():
        time.sleep(1)
        cursor2 = mysql_conn.cursor()
        cursor2.execute("SELECT * FROM suppliers;")
        print("[Session 2] Read suppliers:", cursor2.fetchall())
        cursor2.close()

    t = threading.Thread(target=read_uncommitted)
    t.start()

    time.sleep(2)
    print("[Session 1] Performing ROLLBACK...")
    mysql_conn.rollback()

    cursor.close()

# Running tests in MySQL
# print("\n=== Test Dirty Read ===")
# dirty_read()
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