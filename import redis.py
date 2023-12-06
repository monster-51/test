import redis
from clickhouse_driver import Client
import requests
import json
from multiprocessing import Process

redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
clickhouse_client = Client(host='localhost', port=9000, user='default', password='123')

external_service_url = 'https://pastebin.com/api/api_post.php'

def search_and_send(data):
    ipv4 = data['ipv4']
    mac = data['mac']

    query = f"SELECT username FROM search_service.user_data WHERE ipv4 = '{ipv4}' AND mac = '{mac}' LIMIT 1"
    result = clickhouse_client.execute(query)

    if result:
        username = result[0][0]
        payload = {'username': username, 'ipv4': ipv4, 'mac': mac}
        response = requests.post(external_service_url, json=payload)

        if response.status_code == 200:
            # Пытаемся получить URL из ответа
            try:
                response_json = response.json()
                url = response_json.get('url')
                if url:
                    # Сохраняем URL в файл
                    with open('/home/karabas/url.txt', 'a') as file:
                        file.write(url + '\n')
                else:
                    print("Ответ от внешнего сервиса не содержит URL.")
            except json.JSONDecodeError:
                print("Ошибка декодирования JSON ответа от внешнего сервиса.")
        else:
            print(f"Ошибка при отправке запроса на внешний сервис. Код ответа: {response.status_code}")
        
def process_queue():
    while True:
    
        queue_data = redis_client.blpop('search_queue') # берем из очереди

        if queue_data:
            data = json.loads(queue_data[1])
            p = Process(target=search_and_send, args=(data,))
            p.start()

if __name__ == '__main__':                  # Запуск паралельных процессов
                                            
    for _ in range(4):                      # 4 процесса из очереди
        p = Process(target=process_queue)
        p.start()

