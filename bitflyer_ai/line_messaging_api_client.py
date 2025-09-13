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


class LineMessagingAPIClient:
    def __init__(self):
        self.base_url = 'https://api.line.me/v2/bot/message/'
        self.access_token = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')

        if not LOCAL:
            self.access_token = aws.decrypt(self.access_token)

    def notify(self, message="message"):
        return self._broadcast(message)


    def _broadcast(self, message: str):
        """
        全ユーザーにテキストメッセージを送信（Broadcast）
        :param message: 送信する文字列
        """
        body = {
            "messages": [
                {"type": "text", "text": message}
            ]
        }
        return self._post("broadcast", body)

    def _post(self, action, body):
        api_url = self.base_url + action

        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }

        response = requests.post(api_url, headers=headers, json=body)

        if response.status_code == 200:
            logger.debug(f"[LINE messaging] POST:{api_url} {response.status_code}")
        else:
            logger.warning(f"[LINE messaging] POST:{api_url} {response.status_code} {response.text}")
        return response
