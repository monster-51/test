import redis
from clickhouse_driver import Client
import requests
import json
from multiprocessing import Process

# Глобальные переменные
api_dev_key = 'yAMu96g8htI5R4dDmLh6PfZmhyKRJd-Q'
external_service_url = 'https://pastebin.com/api/api_post.php'
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
clickhouse_client = Client(host='localhost', port=9000, user='default', password='123')


def create_pastebin(api_dev_key, api_paste_code, api_paste_private, api_paste_name,
                    api_paste_expire_date, api_paste_format, api_user_key, api_option):
    #Заполняем словарь для API (pastebin)
    payload = {
        'api_dev_key': api_dev_key,
        'api_paste_code': api_paste_code,
        'api_paste_private': api_paste_private,
        'api_paste_name': api_paste_name,
        'api_paste_expire_date': api_paste_expire_date,
        'api_paste_format': api_paste_format,
        'api_user_key': api_user_key,
        'api_option': api_option
    }
    return payload

def search_and_send(data):
    ipv4 = data['ipv4']
    mac = data['mac']

    query = f"SELECT username FROM search_service.user_data WHERE ipv4 = '{ipv4}' AND mac = '{mac}' LIMIT 1"
    result = clickhouse_client.execute(query)

    if result:
        username = result[0][0]
        userdict = {'username': username, 'ipv4': ipv4, 'mac': mac}
        response = requests.post(external_service_url, data=create_pastebin(api_dev_key, json.dumps(userdict), '0', 'result', '1D', 'json', '', 'paste'))
        # response = requests.post(external_service_url, json=payload)
        print(f"Код ответа: {response.status_code}")
        #print(f"Результат {api_dev_key}, {json.dumps(payload)}, {result}, '10M', 'json', '', 'paste'")

        if response.status_code == 200:
            with open('url.txt', 'a') as file:
                file.write(response.text + '\n')
            print('Пост успешно создан. URL:', response.text)


                       # print(f"Результат {api_dev_key}, {json.dumps(payload)}, {result}, '10M', 'json', '', 'paste'")

def process_queue():
    while True:
        queue_data = redis_client.blpop('search_queue')

        if queue_data:
            data = json.loads(queue_data[1])
            p = Process(target=search_and_send, args=(data,))
            p.start()

if __name__ == '__main__':
    for _ in range(5):
        p = Process(target=process_queue)
        p.start()
