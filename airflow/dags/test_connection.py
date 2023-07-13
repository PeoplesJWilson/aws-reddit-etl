import config
import mysql.connector


try:
    # Establish a MySQL connection
    connection = mysql.connector.connect(
        host=config.rds_endpoint,
        user=config.rds_user,
        password=config.rds_password,
        database=config.rds_db_name
    )

    # Connection successful
    print("MySQL connection successful!")
    print("Server Info:", connection.get_server_info())
    print("MySQL version:", connection.get_server_version())

    connection.close()

except mysql.connector.Error as error:
    # Connection failed
    print("MySQL connection failed:", error)
