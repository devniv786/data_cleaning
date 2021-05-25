import psycopg2

# establishing the connection
conn = psycopg2.connect(
    database="FNO", user="postgres", password="admin", host="127.0.0.1", port="5432"
)
