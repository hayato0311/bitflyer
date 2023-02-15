import os
from logging import getLogger

import requests
from manage import LOCAL

logger = getLogger(__name__)

if LOCAL:
    from dotenv import load_dotenv
    load_dotenv()
else:
    import aws


class LineNotify:
    def __init__(self):
        self.base_url = 'https://notify-api.line.me/api/'

        self.access_token = os.environ.get('LINE_NOTIFY_ACCESS_TOKEN')

        if not LOCAL:
            self.access_token = aws.decrypt(self.access_token)

    def notify(self, message="message"):
        body = {'message': message}
        self._post('notify', body)

    def _get(self, action, params={}):
        api_url = self.base_url + action

        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }

        response = requests.get(api_url, headers=headers, params=params)
        response_json = response.json()

        if response.status_code == 200:
            logger.debug(f'[LINE notify] GET:{api_url} {response_json["status"]} {response_json["message"]}')
        else:
            logger.warning(f'[LINE notify] POST:{api_url} {response_json["status"]} {response_json["message"]}')
        return response

    def _post(self, action, body):
        api_url = self.base_url + action

        headers = {
            'Authorization': f'Bearer {self.access_token}',
        }
        response = requests.post(api_url, headers=headers, data=body)
        response_json = response.json()

        if response.status_code == 200:
            logger.debug(f'[LINE notify] POST:{api_url} {response_json["status"]} {response_json["message"]}')
        else:
            logger.warning(f'[LINE notify] POST:{api_url} {response_json["status"]} {response_json["message"]}')
        return response
