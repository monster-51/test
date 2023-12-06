from clickhouse_driver import Client


clickhouse_host = 'localhost'  
clickhouse_port = 9000
clickhouse_user = 'default'
clickhouse_password = '123'
clickhouse_database = 'search_service'

client = Client(
    host=clickhouse_host,
    port=clickhouse_port,
    user=clickhouse_user,
    password=clickhouse_password,
    database=clickhouse_database
)

query = "SELECT * FROM user_data"
result = client.execute(query)

for row in result:
    print(row)
