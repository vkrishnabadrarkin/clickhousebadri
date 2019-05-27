from clickhouse_driver import Client
client = Client("localhost")
print(client)
client.execute('SHOW DATABASES')