import mysql.connector
import psycopg2
import getpass
import time
import threading

def get_credentials():
    mysql_user = input("Introduce el usuario de MySQL: ")
    mysql_password = getpass.getpass("Introduce la contraseña de MySQL: ")
    postgres_user = input("Introduce el usuario de PostgreSQL: ")
    postgres_password = getpass.getpass("Introduce la contraseña de PostgreSQL: ")
    return mysql_user, mysql_password, postgres_user, postgres_password

def show_mysql_databases(mysql_user, mysql_password):
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user=mysql_user,
        password=mysql_password
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute('SHOW DATABASES')
    databases = mysql_cursor.fetchall()
    print("Bases de datos en MySQL:")
    for db in databases:
        print(db[0])
    mysql_cursor.close()
    mysql_conn.close()

def show_postgresql_databases(postgres_user, postgres_password):
    pg_conn = psycopg2.connect(
        host='localhost',
        user=postgres_user,
        password=postgres_password
    )
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute('SELECT datname FROM pg_database')
    databases = pg_cursor.fetchall()
    print("Bases de datos en PostgreSQL:")
    for db in databases:
        print(db[0])
    pg_cursor.close()
    pg_conn.close()

def create_postgresql_database(postgres_user, postgres_password, postgres_db):
    pg_conn = psycopg2.connect(
        host='localhost',
        user=postgres_user,
        password=postgres_password,
        database='postgres'
    )
    pg_conn.autocommit = True
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{postgres_db}'")
    exists = pg_cursor.fetchone()
    if not exists:
        pg_cursor.execute(f"CREATE DATABASE {postgres_db}")
        print(f"Base de datos '{postgres_db}' creada en PostgreSQL.")
    pg_cursor.close()
    pg_conn.close()

def create_mysql_database(mysql_user, mysql_password, mysql_db):
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user=mysql_user,
        password=mysql_password
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_db}")
    print(f"Base de datos '{mysql_db}' creada en MySQL.")
    mysql_cursor.close()
    mysql_conn.close()

def drop_postgresql_database(postgres_user, postgres_password, postgres_db):
    pg_conn = psycopg2.connect(
        host='localhost',
        user=postgres_user,
        password=postgres_password,
        database='postgres'
    )
    pg_conn.autocommit = True
    pg_cursor = pg_conn.cursor()
    pg_cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{postgres_db}'")
    exists = pg_cursor.fetchone()
    if exists:
        pg_cursor.execute(f"DROP DATABASE {postgres_db}")
        print(f"Base de datos '{postgres_db}' eliminada en PostgreSQL.")
    pg_cursor.close()
    pg_conn.close()

def drop_mysql_database(mysql_user, mysql_password, mysql_db):
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user=mysql_user,
        password=mysql_password
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute(f"DROP DATABASE IF EXISTS {mysql_db}")
    print(f"Base de datos '{mysql_db}' eliminada en MySQL.")
    mysql_cursor.close()
    mysql_conn.close()

def check_mysql_database_exists(mysql_user, mysql_password, mysql_db):
    try:
        mysql_conn = mysql.connector.connect(
            host='localhost',
            user=mysql_user,
            password=mysql_password,
            database=mysql_db
        )
        mysql_conn.close()
        return True
    except mysql.connector.Error as err:
        if err.errno == mysql.connector.errorcode.ER_BAD_DB_ERROR:
            return False
        else:
            raise

def get_table_schema_mysql(mysql_cursor, table_name):
    mysql_cursor.execute(f"SHOW COLUMNS FROM {table_name}")
    columns = mysql_cursor.fetchall()
    schema = []
    for col in columns:
        name, dtype, null, key, default, extra = col
        if extra == "auto_increment":
            schema.append(f"{name} SERIAL PRIMARY KEY")
        elif "int" in dtype.lower():
            schema.append(f"{name} INTEGER")
        elif "char" in dtype.lower() or "text" in dtype.lower():
            schema.append(f"{name} TEXT")
        else:
            schema.append(f"{name} TEXT")  # Tipo por defecto
    return schema

def get_table_schema_postgresql(pg_cursor, table_name):
    pg_cursor.execute(f"SELECT column_name, data_type, column_default FROM information_schema.columns WHERE table_name = '{table_name}'")
    columns = pg_cursor.fetchall()
    schema = []
    for col in columns:
        name, dtype, default = col
        if default and 'nextval' in default:
            schema.append(f"{name} SERIAL PRIMARY KEY")
        elif dtype in ["integer", "bigint"]:
            schema.append(f"{name} INTEGER")
        elif dtype in ["character varying", "text"]:
            schema.append(f"{name} TEXT")
        else:
            schema.append(f"{name} TEXT")  # Tipo por defecto
    return schema

def copy_mysql_to_postgresql(mysql_user, mysql_password, postgres_user, postgres_password, mysql_db, postgres_db):
    if not check_mysql_database_exists(mysql_user, mysql_password, mysql_db):
        print(f"La base de datos '{mysql_db}' no existe en MySQL. Creando la base de datos...")
        create_mysql_database(mysql_user, mysql_password, mysql_db)
    
    create_postgresql_database(postgres_user, postgres_password, postgres_db)
    
    mysql_conn = mysql.connector.connect(
        host='localhost',
        user=mysql_user,
        password=mysql_password,
        database=mysql_db
    )
    mysql_cursor = mysql_conn.cursor()
    
    pg_conn = psycopg2.connect(
        host='localhost',
        user=postgres_user,
        password=postgres_password,
        database=postgres_db
    )
    pg_cursor = pg_conn.cursor()

    mysql_cursor.execute('SHOW TABLES')
    tables = mysql_cursor.fetchall()
    for (table_name,) in tables:
        schema = get_table_schema_mysql(mysql_cursor, table_name)
        pg_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(schema)})"
        pg_cursor.execute(create_table_query)

        mysql_cursor.execute(f'SELECT * FROM {table_name}')
        rows = mysql_cursor.fetchall()
        columns = [desc[0] for desc in mysql_cursor.description]

        for row in rows:
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
            pg_cursor.execute(insert_query, row)
    
    pg_conn.commit()
    mysql_cursor.close()
    mysql_conn.close()
    pg_cursor.close()
    pg_conn.close()
    print(f"Base de datos '{mysql_db}' copiada a '{postgres_db}' en PostgreSQL.")

def copy_postgresql_to_mysql(mysql_user, mysql_password, postgres_user, postgres_password, postgres_db, mysql_db):
    create_mysql_database(mysql_user, mysql_password, mysql_db)
    
    pg_conn = psycopg2.connect(
        host='localhost',
        user=postgres_user,
        password=postgres_password,
        database=postgres_db
    )
    pg_cursor = pg_conn.cursor()

    mysql_conn = mysql.connector.connect(
        host='localhost',
        user=mysql_user,
        password=mysql_password,
        database=mysql_db
    )
    mysql_cursor = mysql_conn.cursor()

    pg_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = pg_cursor.fetchall()
    for (table_name,) in tables:
        schema = get_table_schema_postgresql(pg_cursor, table_name)
        mysql_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
        create_table_query = f"CREATE TABLE {table_name} ({', '.join(schema)})"
        mysql_cursor.execute(create_table_query)

        pg_cursor.execute(f'SELECT * FROM {table_name}')
        rows = pg_cursor.fetchall()
        columns = [desc[0] for desc in pg_cursor.description]

        for row in rows:
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s' for _ in columns])})"
            mysql_cursor.execute(insert_query, row)
    
    mysql_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    mysql_cursor.close()
    mysql_conn.close()
    print(f"Base de datos '{postgres_db}' copiada a '{mysql_db}' en MySQL.")

def continuous_sync(mysql_user, mysql_password, postgres_user, postgres_password):
    mysql_db = input("Nombre de la base de datos en MySQL para sincronizar: ")
    postgres_db = input("Nombre de la base de datos en PostgreSQL para sincronizar: ")
    
    def sync_loop():
        while True:
            print("\nSincronizando datos...")
            copy_mysql_to_postgresql(mysql_user, mysql_password, postgres_user, postgres_password, mysql_db, postgres_db)
            copy_postgresql_to_mysql(mysql_user, mysql_password, postgres_user, postgres_password, postgres_db, mysql_db)
            
            print("Sincronización completa. Esperando 20 segundos antes de la siguiente sincronización.")
            time.sleep(20)  # Esperar 20 segundos

    sync_thread = threading.Thread(target=sync_loop)
    sync_thread.start()

    while True:
        user_input = input("Escribe 'exit' para detener la sincronización continua: ")
        if user_input.strip().lower() == 'exit':
            print("Deteniendo la sincronización continua...")
            break

    sync_thread.join()

def main():
    mysql_user, mysql_password, postgres_user, postgres_password = get_credentials()
    while True:
        print("\nElige una opción:")
        print("1. Mostrar bases de datos de MySQL")
        print("2. Mostrar bases de datos de PostgreSQL")
        print("3. Copiar base de datos de MySQL a PostgreSQL")
        print("4. Copiar base de datos de PostgreSQL a MySQL")
        print("5. Eliminar base de datos en MySQL")
        print("6. Eliminar base de datos en PostgreSQL")
        print("7. Sincronizar continuamente")
        print("8. Salir")

        option = input("Opción: ")

        if option == "1":
            show_mysql_databases(mysql_user, mysql_password)
        elif option == "2":
            show_postgresql_databases(postgres_user, postgres_password)
        elif option == "3":
            mysql_db = input("Nombre de la base de datos en MySQL: ")
            postgres_db = input("Nombre de la base de datos en PostgreSQL: ")
            copy_mysql_to_postgresql(mysql_user, mysql_password, postgres_user, postgres_password, mysql_db, postgres_db)
        elif option == "4":
            postgres_db = input("Nombre de la base de datos en PostgreSQL: ")
            mysql_db = input("Nombre de la base de datos en MySQL: ")
            copy_postgresql_to_mysql(mysql_user, mysql_password, postgres_user, postgres_password, postgres_db, mysql_db)
        elif option == "5":
            mysql_db = input("Nombre de la base de datos en MySQL a eliminar: ")
            drop_mysql_database(mysql_user, mysql_password, mysql_db)
        elif option == "6":
            postgres_db = input("Nombre de la base de datos en PostgreSQL a eliminar: ")
            drop_postgresql_database(postgres_user, postgres_password, postgres_db)
        elif option == "7":
            continuous_sync(mysql_user, mysql_password, postgres_user, postgres_password)
        elif option == "8":
            break
        else:
            print("Opción no válida. Inténtalo de nuevo.")

if __name__ == "__main__":
    main()
