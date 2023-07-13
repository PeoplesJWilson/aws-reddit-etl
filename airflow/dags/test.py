import mysql.connector
import config

def query_records():
    connection = mysql.connector.connect(
        host=config.rds_endpoint,
        user=config.rds_user,
        password=config.rds_password,
        database=config.rds_db_name
    )

    cursor = connection.cursor()

    select_query = "SELECT * FROM posts LIMIT 10"
    cursor.execute(select_query)

    posts = cursor.fetchall()

    for post in posts:
        print(post)



    query = f"""
        SELECT SUM(data_length + index_length)
        FROM information_schema.tables
        WHERE table_schema = '{config.rds_db_name}'
    """
    cursor.execute(query)

    database_size = cursor.fetchone()[0]

    if database_size:
        print(f"Database '{config.rds_db_name}' size: {database_size} bytes")

    cursor.close()
    connection.close()

# Call
query_records()