import redis
from clickhouse_driver import Client
import requests
import json
from multiprocessing import Process
import time

# Глобальные переменные
api_dev_key = 'yAMu96g8htI5R4dDmLh6PfZmhyKRJd-Q'
external_service_url = 'https://pastebin.com/api/api_post.php'
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
clickhouse_client = Client(host='localhost', port=9000, user='default', password='123')

def retry(func, max_retries=3, delay=2):
    for _ in range(max_retries):
        try:
            return func()
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(delay)
    raise RuntimeError("Достизнуто максимальное количество попыток")

def create_pastebin_retry(api_dev_key, api_paste_code, api_paste_private, api_paste_name,
                    api_paste_expire_date, api_paste_format, api_user_key, api_option):
    def create_pastebin():
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
    return retry(create_pastebin)

def search_and_send_retry(data):
    def search_and_send():
        ipv4 = data['ipv4']
        mac = data['mac']

        query = f"SELECT username FROM search_service.user_data WHERE ipv4 = '{ipv4}' AND mac = '{mac}' LIMIT 1"
        if query:
            print ('\nНайдена запись в очереди REDIS...')
        result = clickhouse_client.execute(query)

        if result:
            print ('Есть совпадение в базе, публикуем результат...')
            username = result[0][0]
            userdict = {'username': username, 'ipv4': ipv4, 'mac': mac}
            response = requests.post(external_service_url, data=create_pastebin_retry(api_dev_key, json.dumps(userdict), '0', 'result', '1M', 'json', '', 'paste'))
            # response = requests.post(external_service_url, json=payload)
            print(f"Код ответа: {response.status_code}")
            #print(f"Результат {api_dev_key}, {json.dumps(payload)}, {result}, '10M', 'json', '', 'paste'")

            if response.status_code == 200:
                with open('url.txt', 'a') as file:
                    file.write(response.text + '\n')
                print('Пост успешно создан. URL:', response.text)


                        # print(f"Результат {api_dev_key}, {json.dumps(payload)}, {result}, '10M', 'json', '', 'paste'")
        else:
            print('нет совпадений в базе clickhouse')

    return retry(search_and_send)

def process_queue():
    while True:
        queue_data = redis_client.blpop('search_queue')

        if queue_data:
            data = json.loads(queue_data[1])
            p = Process(target=search_and_send_retry, args=(data,))
            p.start()

if __name__ == '__main__':
    for _ in range(5):
        p = Process(target=process_queue)
        p.start()
