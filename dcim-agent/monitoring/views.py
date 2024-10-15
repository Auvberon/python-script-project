import json
import random
import string
import secrets

import mysql.connector
import psycopg2
from django.http import HttpResponse, JsonResponse
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from rest_framework.decorators import api_view


def index(request):
    return HttpResponse("DCIM Agent")

def remove_quotes(parsed_json):
    for key, value in parsed_json.items():
        if isinstance(value, str):
            try:
                parsed_json[key] = int(value)
            except ValueError:
                try:
                    parsed_json[key] = float(value)
                except ValueError:
                    pass
        elif isinstance(value, list):
            for item in value:
                remove_quotes(item)
    return parsed_json

def generate_password_postgresql(password_length=8, exclude_chars="'<>\/^` \n "):
    letters = string.ascii_letters
    digits = string.digits
    special_chars = string.punctuation
    alphabet = letters + digits + special_chars
    password = ''
    for i in range(password_length):
        password += ''.join(secrets.choice(alphabet))

    print(password)
    return password

def generate_password_mysql(password_length=8, exclude_chars="'<>\/^`[]{}|~,?-_+*()&;=:\n\" "):
    allowed_chars = string.ascii_letters + string.digits + string.punctuation
    allowed_chars = ''.join(c for c in allowed_chars if c not in exclude_chars)
    password = random.choice([c for c in string.punctuation if c not in exclude_chars])
    password += random.choice(string.digits)
    for i in range(password_length - 2):
        if i % 2 == 0:
            password += random.choice(string.ascii_lowercase)
        else:
            password += random.choice(string.ascii_uppercase)
    password = ''.join(random.sample(password, len(password)))
    return password

@api_view(['POST'])
def check_database_connection_details(request):
    conn_data = json.loads(request.body.decode("utf-8"))
    if conn_data["vendor"] == "postgres":
        con = psycopg2.connect(user=conn_data["username"], host=conn_data["host"], password=conn_data["password"],
                               port=conn_data["port"], database="postgres")
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = con.cursor()
        keys = ('total_conn', 'max_conn', 'database_conn')
        result = []
        cursor.execute("select (SELECT sum(numbackends) as a FROM pg_stat_database), \
            (SELECT cast(setting as bigint) FROM pg_settings  WHERE name = 'max_connections');")
        rows_connection = cursor.fetchall()
        for row in rows_connection:
            result.append(dict(zip(keys, row)))

        database_result = []
        keys_database = ('idle_conn', 'active_conn', 'name', 'db_size')
        for x in conn_data["databases"]:
            cursor.execute("select (select count(*) from pg_stat_activity\
            where state='active' and datname='" + x + "') as active_connection\
            , (select count(*) from pg_stat_activity where state='idle' and datname='" + x + "') as idle_connection\
            ,(select distinct datname from pg_stat_activity where datname='" + x + "') as db_name\
            , (ROUND(pg_database_size('" + x + "')::numeric / 1073741824.0, 1)) as db_size")
            rows_databases = cursor.fetchall()
            for row in rows_databases:
                database_result.append(dict(zip(keys_database, row)))
        thisdict = dict(total_conn=rows_connection[0][0], max_conn=rows_connection[0][1], db_conn=database_result)
        json_data=json.dumps(thisdict,default=str)
        parsed_json = json.loads(json_data)
        parsed_json = remove_quotes(parsed_json)

    elif conn_data["vendor"] == "mysql":
        cnx = mysql.connector.connect(user=conn_data["username"], password=conn_data["password"],
                                      host=conn_data["host"], port=conn_data["port"], ssl_disabled='TRUE')
        cursor = cnx.cursor()
        result = []
        keys = ('total_conn', 'max_conn', 'database_conn')
        cursor.execute(
            "SELECT(SELECT VARIABLE_VALUE FROM performance_schema.global_status\
            WHERE VARIABLE_NAME = 'Threads_connected')AS 'total_conn',(SELECT@@MAX_CONNECTIONS)AS 'max_conn'")
        rows_connection = cursor.fetchall()
        for row in rows_connection:
            result.append(dict(zip(keys, row)))

        database_result = []
        keys_database = ('name', 'active_conn', 'idle_conn', 'db_size')
        for x in conn_data["databases"]:
            cursor.execute("SELECT(SELECT schema_name FROM information_schema.schemata WHERE schema_name =  '" + x + "') AS db,\
                          (SELECT COUNT(*) FROM information_schema.processlist WHERE state  != '' AND db = '" + x + "') AS active_conn,\
                          (SELECT COUNT(*) FROM information_schema.processlist WHERE db = '" + x + "' AND state = '' OR info IS NULL OR command = 'Sleep') AS idle_conn,\
                          (SELECT SUM(data_length + index_length) / 1024 / 1024 / 1024 AS 'Size (GB)' FROM information_schema.TABLES WHERE table_schema = '" + x + "') AS db_size")
            rows_databases = cursor.fetchall()
            for row in rows_databases:
                database_result.append(dict(zip(keys_database, row)))

        print(database_result)

        print(rows_connection[0][0])

        thisdict = dict(total_conn=rows_connection[0][0], max_conn=rows_connection[0][1], db_conn=database_result)

        print(thisdict)
        json_data=json.dumps(thisdict,default=str)
        parsed_json = json.loads(json_data)
        parsed_json = remove_quotes(parsed_json)
    else:
        return JsonResponse({"error": "Vendor Not Found"}, status=400)
    json_data = json.dumps(parsed_json, indent=4, sort_keys=True, default=str)
    cursor.close()
    return HttpResponse(json_data, content_type="application/json")


@api_view(['POST'])
def create_database_user(request):
    conn_data = json.loads(request.body.decode("utf-8"))
    print(conn_data["vendor"])
    if conn_data["vendor"] == "postgres":
        con = psycopg2.connect(user=conn_data["username"], host=conn_data["host"], password=conn_data["password"],
                               port=conn_data["port"], database=conn_data["databases"])
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        password = generate_password_postgresql()
        cursor = con.cursor()
        cursor.execute("SELECT EXISTS (SELECT FROM pg_user WHERE usename = %s)", (conn_data["user"],))
        user_exists = cursor.fetchone()[0]
        if conn_data["permission"] == "write":
            if not user_exists:
                cursor.execute("CREATE USER " + conn_data["user"] + " WITH PASSWORD '" + password + "'")

            cursor.execute("GRANT ALL PRIVILEGES ON DATABASE " + conn_data["database"] + " TO " + conn_data["user"])
            cursor.execute("DO $$\
            DECLARE  \
                username varchar;\
                target varchar;\
                res record;\
                q varchar;\
            begin\
                select '" + conn_data["user"] + "' into username;\
                FOR res IN SELECT nspname as sch FROM pg_namespace\
                loop \
                    EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(res.sch) || ' TO ' || username;\
                END LOOP;\
            FOR res IN SELECT nspname as sch FROM pg_namespace \
                loop\
                    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || quote_ident(res.sch) || ' TO ' || username;\
                END LOOP;\
            END$$")
        elif conn_data["permission"] == "read":
            if not user_exists:
                cursor.execute("CREATE USER " + conn_data["user"] + " WITH PASSWORD '" + password + "'")
            cursor.execute("DO $$\
            DECLARE  \
                username varchar ;\
                target varchar;\
                res record;\
                q varchar;\
            begin\
                select '" + conn_data["user"] + "' into username;\
                FOR res IN SELECT nspname as sch FROM pg_namespace\
                loop \
                    EXECUTE 'GRANT USAGE ON SCHEMA ' || quote_ident(res.sch) || ' TO ' || username;\
                END LOOP;\
            FOR res IN SELECT nspname as sch FROM pg_namespace \
                loop\
                    EXECUTE 'GRANT SELECT ON ALL TABLES IN SCHEMA ' || quote_ident(res.sch) || ' TO ' || username;\
                END LOOP;\
            END$$")

    elif conn_data["vendor"] == "mysql":
        cnx = mysql.connector.connect(user=conn_data["username"], password=conn_data["password"],
                                      host=conn_data["host"], port=conn_data["port"], ssl_disabled='TRUE')
        cursor = cnx.cursor()
        password = generate_password_mysql()
        cursor.execute("FLUSH PRIVILEGES")
        cursor.execute("SELECT EXISTS(SELECT 1 FROM mysql.user WHERE user = %s)", (conn_data["user"],))
        user_exists = cursor.fetchone()[0]
        if conn_data["permission"] == "write":
            if not user_exists:
                cursor.execute("CREATE USER '" + conn_data["user"] + "'@'%' IDENTIFIED BY '" + password + "'")
            cursor.execute(
                "GRANT SELECT, INSERT, DELETE, UPDATE, SHOW VIEW ON " + conn_data["databases"] + ".* TO '" + conn_data[
                    "user"] + "'@'%' IDENTIFIED BY '" + password + "'")
            cursor.execute("FLUSH PRIVILEGES")
        elif conn_data["permission"] == "read":
            if not user_exists:
                cursor.execute("CREATE USER '" + conn_data["user"] + "'@'%' IDENTIFIED BY '" + password + "'")
            cursor.execute("CREATE USER '" + conn_data["user"] + "'@'%' IDENTIFIED BY '" + password + "'")
            cursor.execute("GRANT SELECT, SHOW VIEW ON " + conn_data["databases"] + ".* TO '" + conn_data[
                "user"] + "'@'%' IDENTIFIED BY '" + password + "'")
            cursor.execute("FLUSH PRIVILEGES")
    cursor.close()
    return HttpResponse("password: " + password, content_type="application/json")
    

@api_view(['POST'])
def update_database_user_password(request):
    conn_data = json.loads(request.body.decode("utf-8"))
    if conn_data["vendor"] == "postgres":
        con = psycopg2.connect(user=conn_data["username"], host=conn_data["host"], password=conn_data["password"],
                               port=conn_data["port"], database=conn_data["databases"])
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        password = generate_password_postgresql()
        cursor = con.cursor()
        cursor.execute("")
    elif conn_data["vendor"] == "mysql":
        cnx = mysql.connector.connect(user=conn_data["username"], password=conn_data["password"],
                                      host=conn_data["host"], port=conn_data["port"], ssl_disabled='TRUE')
        cursor = cnx.cursor()
        password = generate_password_mysql()
        cursor.execute("ALTER USER '" + conn_data["user"] + "'@'%' IDENTIFIED BY '" + password + "'")
        cursor.execute("FLUSH PRIVILEGES")
    cursor.close()
    return HttpResponse("password: " + password, content_type="application/json")

@api_view(['POST'])
def delete_database_user(request):
    conn_data = json.loads(request.body.decode("utf-8"))
    if conn_data["vendor"] == "postgres":
        con = psycopg2.connect(user=conn_data["username"], host=conn_data["host"], password=conn_data["password"],
                               port=conn_data["port"], database=conn_data["databases"])
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        new_string = conn_data["user"].replace("'", "").replace('"', '')

        cursor = con.cursor()
        cursor.execute("DROP OWNED BY " + new_string)
        cursor.execute("DROP USER " + new_string)

    elif conn_data["vendor"] == "mysql":
        cnx = mysql.connector.connect(user=conn_data["username"], password=conn_data["password"],
                                      host=conn_data["host"], port=conn_data["port"], ssl_disabled='TRUE')
        cursor = cnx.cursor()
        cursor.execute("DROP USER '" + conn_data["user"] + "'@'%'")
        cursor.execute("FLUSH PRIVILEGES")
    cursor.close()
    return HttpResponse("User : '"+ conn_data["user"] + "'@'%'" + "succesfully unregistered.", content_type="application/json")


@api_view(['POST'])
def check_database_slave_status(request):
    if request.method == 'POST':
        # Get the JSON data from the request body
        data = request.body
        # Parse the JSON data
        data = json.loads(data)
        hostname = data.get("host")
        port = data.get("port")
        user = data.get("username")
        password = data.get("password")
        vendor = data.get("vendor")
        databases = data.get("databases")

        if vendor == 'mysql':
            # Connect to the MySQL database
            try:
                conn = mysql.connector.connect(
                    host=hostname,
                    user=user,
                    password=password,
                    port=port,
                    ssl_disabled='TRUE')
                cursor = conn.cursor()

                # Execute the SHOW SLAVE STATUS query
                cursor.execute("SHOW SLAVE STATUS")
                result = cursor.fetchall()

                # Close the cursor and connection
                cursor.close()
                conn.close()
            except Exception as e:
                return JsonResponse({"error": f"Error connecting to database: {e}"}, status=500)

            # Convert the result into a dictionary
            try:
                response = {
                    'slave_status': result[0][0],
                    'master_host': result[0][1],
                    'sec_behind_master': result[0][32],
                    'slave_sql_running': bool(result[0][10] == 'Yes'),
                    'slave_io_running': bool(result[0][11] == 'Yes'),
                    'last_errno': result[0][18],
                    'last_error': result[0][19]
                }
            except IndexError:
                return JsonResponse({'error': 'Result out of index'})

        elif vendor == 'postgres':
            try:
                # Connect to the Postgresql database
                conn = psycopg2.connect(
                    host=hostname,
                    user=user,
                    password=password,
                    port=port
                )
                cursor = conn.cursor()

                # Execute the SHOW SLAVE STATUS query
                cursor.execute('SELECT * FROM pg_stat_replication')
                result = cursor.fetchall()

                # Close the cursor and connection
                cursor.close()
                conn.close()
            except Exception as e:
                return JsonResponse({"error": f"Error connecting to database: {e}"}, status=500)

            # Convert the result into a dictionary
            try:
                response = {
                    'slave_status': result[0][0],
                    'master_host': result[0][1],
                    'sec_behind_master': result[0][32],
                    'slave_sql_running': bool(result[0][10] == 'Yes'),
                    'slave_io_running': bool(result[0][11] == 'Yes'),
                    'last_errno': result[0][18],
                    'last_error': result[0][19]
                }
            except IndexError:
                return JsonResponse({'error': 'Result out of index'})

        else:
            return JsonResponse({"error": "Vendor Not Found"}, status=400)

    else:
        # Return a bad request response if the request method is not POST
        return JsonResponse({"error": "Bad request"}, status=400)
