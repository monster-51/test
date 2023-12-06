import requests


def create_pastebin(api_dev_key, api_paste_code, api_paste_private, api_paste_name,
                    api_paste_expire_date, api_paste_format, api_user_key, api_option):
    url = 'https://pastebin.com/api/api_post.php'
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

    response = requests.post(url, data=payload)

    if response.text.startswith('Bad API request'):
        print('Ошибка при отправке запроса на внешний сервис.', response.text)
    else:
        print('Пост успешно создан. URL:', response.text)

# Данные для доступа к API Pastebin.com
api_dev_key = 'yAMu96g8htI5R4dDmLh6PfZmhyKRJd-Q'
api_paste_code = 'test_text'
api_paste_private = '0'  # 0=public 1=unlisted 2=private
api_paste_name = 'Aticle'
api_paste_expire_date = '10M'
api_paste_format = 'json'
api_user_key = ''  # Так надо
api_option = 'paste'


create_pastebin(api_dev_key, api_paste_code, api_paste_private, api_paste_name,
                api_paste_expire_date, api_paste_format, api_user_key, api_option)
