from clickhouse_driver import Client
import os
from dotenv import load_dotenv

def get_clickhouse_client():
    load_dotenv()  
  
    host = os.getenv('CLICKHOUSE_HOST', 'localhost')
    port = int(os.getenv('CLICKHOUSE_PORT', 9000))
    user = os.getenv('CLICKHOUSE_USER', 'default')
    password = os.getenv('CLICKHOUSE_PASSWORD', '')
    database = os.getenv('CLICKHOUSE_DB', 'purchase_analysis')


    client = Client(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    
    return client

def test_connection():

    try:
        client = get_clickhouse_client()
        result = client.execute('SELECT 1')
        print("Подключение к ClickHouse успешно установлено!")
        return True
    except Exception as e:
        print(f"Ошибка подключения к ClickHouse: {e}")
        return False

if __name__ == '__main__':
    test_connection()
