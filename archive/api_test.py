import requests
from datetime import datetime, timezone, timedelta


data = {'grant_type': 'refresh_token',
        'refresh_token': token}
response = requests.post('https://login.questrade.com/oauth2/token', data=data)

response = response.json()

test = requests.get(f"{response['api_server']}v1/accounts/",
                    headers={'Authorization': f'Bearer {response["access_token"]}'})

test2 = requests.get(f"{response['api_server']}v1/symbols/search", params={'prefix': 'BMO'},
                     headers={'Authorization': f'Bearer {response["access_token"]}'})

begin = datetime.strptime('2014-10-01', '%Y-%m-%d')
end = datetime.strptime('2014-12-01', '%Y-%m-%d')
candles = {
    'startTime': f'{begin.isoformat()}-05:00',
    'endTime': f'{end.isoformat()}-05:00',
    'interval': "OneDay"
}

test3 = requests.get(f"{response['api_server']}v1/markets/candles/9292", params=candles,
                     headers={'Authorization': f'Bearer {response["access_token"]}'})


