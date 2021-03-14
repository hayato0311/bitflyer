from manage import LOCAL, REF_LOCAL
import pandas as pd
from logging import getLogger
import os
from pathlib import Path
from pprint import pprint
import json
import datetime
import pytz
import time
import requests
import hmac
import hashlib


logger = getLogger(__name__)

if LOCAL:
    from dotenv import load_dotenv
    load_dotenv()
else:
    import aws

# else:
#     from aws import decrypt

# HTTP Public API (GET)
HTTP_PUBLIC_API = {
    'GET': {
        'market_list': '/v1/getmarkets',
        'ticker': '/v1/getticker',
        'executions': '/v1/getexecutions',
    }
}

# HTTP Private API

HTTP_PRIVATE_API = {
    'GET': {
        'withdraw_history': '/v1/me/getwithdrawals',
        'balance': '/v1/me/getbalance',
        'coin_in_history': '/v1/me/getcoinins',
        'coin_out_history': '/v1/me/getcoinins',
        'child_orders': '/v1/me/getchildorders'

    },
    'POST': {
        'withdraw': '/v1/withdraw',
        'send_child_order': '/v1/me/sendchildorder',
        'cancel_child_order': '/v1/me/cancelchildorder',
        'cancel_all_child_order': '/v1/me/cancelallchildorders'
    }

}

API_KEY_PATH = 'private/api_key.json'


class BitflyerAPI:
    def __init__(self, method, process_path, params={}):
        self.base_url = 'https://api.bitflyer.com'
        self.process_path = process_path
        self.params = params

        self.query = ''

        if len(params) > 0:
            self.query += '?'
            for i, (key, val) in enumerate(params.items()):
                if i != len(params) - 1:
                    self.query += f'{key}={val}&'
                else:
                    self.query += f'{key}={val}'

        self.process_path_with_query = process_path + self.query

        self.api_url = self.base_url + self.process_path

        self.method = method
        self.unix_time = str(time.time())

        self.api_key = os.environ.get('API_KEY')
        self.api_secret = os.environ.get('API_SECRET')

        if not LOCAL:
            self.api_key = aws.decrypt(self.api_key)
            self.api_secret = aws.decrypt(self.api_secret)

        # if REF_LOCAL:
        #     self.api_key = os.environ.get('API_KEY')
        #     self.api_secret = os.environ.get('API_SECRET')
        # else:
        #     self.api_key = decrypt(os.environ.get('API_KEY'))
        #     self.api_secret = decrypt(os.environ.get('API_SECRET'))

    def sign(self, body={}):
        if self.method == 'GET':
            payload = self.unix_time + self.method + self.process_path_with_query
        elif self.method == 'POST':
            payload = self.unix_time + self.method + \
                self.process_path + json.dumps(body)
        else:
            raise ValueError('Method name is not correct. set "GET" or "POST"')

        return hmac.new(self.api_secret.encode('utf-8'), payload.encode('utf-8'),
                        hashlib.sha256).hexdigest()

    def get(self, private=True, name=''):
        if private:
            headers = {
                'ACCESS-KEY': self.api_key,
                'ACCESS-TIMESTAMP': self.unix_time,
                'ACCESS-SIGN': self.sign(),
                'Content-Type': 'application/json'
            }
        else:
            headers = {'Content-Type': 'application/json'}

        result = requests.get(
            self.api_url, headers=headers, params=self.params)

        if result.status_code == 200:
            logger.debug(f'[{name}] GETに成功しました！')
        else:
            logger.error(
                f'[{name} {result.json()["error_message"]}] GETに失敗しました。')
        return result

    def post(self, body, name=''):
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-TIMESTAMP': self.unix_time,
            'ACCESS-SIGN': self.sign(body),
            'Content-Type': 'application/json'
        }
        result = requests.post(self.api_url, headers=headers, json=body)
        if result.status_code == 200:
            logger.debug(f'[{name}] POSTに成功しました！')
        else:
            logger.error(
                f'[{name} {result.json()["error_message"]}] POSTに失敗しました。')
        return result


# HTTP_PUBLIC_API


def get_ticker(product_code='ETH_JPY'):
    """プロダクトの情報を取得
    Args:
        product_code (str, optional): 'BTC_JPY', 'ETH_BTC', 'BCH_BTC', 'ETH_JPY'が利用可能。デフォルト値は 'ETH_JPY'。
    Returns:
        dict: レスポンス
    """
    method = 'GET'
    process_path = HTTP_PUBLIC_API[method]['ticker']
    params = {'product_code': product_code}

    bf = BitflyerAPI(method, process_path, params=params)
    result = bf.get(private=False)

    return result


def get_executions(product_code='ETH_JPY', count=100, before=0, after=0, region='Asia/Tokyo'):
    """約定履歴を取得

    Args:
        product_code (str, optional): 'BTC_JPY', 'ETH_BTC', 'BCH_BTC', 'ETH_JPY'が利用可能。デフォルト値は 'ETH_JPY'。
        count (int, optional):  結果の個数を指定。デフォルトは100。
        before (str, optional): このパラメータに指定した値より小さい id を持つデータを取得。
        after (str, optional): このパラメータに指定した値より大きい id を持つデータを取得。
        region (str, optional): 住んでいる地域。

    Returns:
        [type]: [description]
    """
    method = 'GET'
    process_path = HTTP_PUBLIC_API[method]['executions']
    params = {'product_code': product_code, 'count': count}

    if before != 0:
        params['before'] = before
    if after != 0:
        params['after'] = after

    bf = BitflyerAPI(method, process_path, params=params)
    result = bf.get(private=False, name='get_executions')
    if result.status_code == 200:
        df_result = pd.DataFrame(result.json())
        if not df_result.empty:
            df_result['exec_date'] = pd.to_datetime(
                df_result['exec_date'], utc=True)
            df_result = df_result.set_index('exec_date', drop=True)
            df_result = df_result.tz_convert(region)

        return df_result
    else:
        response_json = response.json()
        logger.error(response_json['error_message'])
        raise Exception('get execution error')


# HTTP_PRIVATE_API


def get_balance():
    method = 'GET'
    process_path = HTTP_PRIVATE_API[method]['balance']

    bf = BitflyerAPI(method, process_path)

    result = bf.get(private=True, name='get_balance')

    df = pd.DataFrame(result.json())

    return df


def get_child_orders(product_code='ETH_JPY', count=100, before=0, after=0,
                     child_order_state='',
                     child_order_id='', child_order_acceptance_id='',
                     parent_order_id='', region='Asia/Tokyo'):

    method = 'GET'
    process_path = HTTP_PRIVATE_API[method]['child_orders']
    params = {'product_code': product_code, 'count': count}

    if before != 0:
        params['before'] = before
    if after != 0:
        params['after'] = after
    if child_order_state != '':
        params['child_order_state'] = child_order_state
    if child_order_id != '':
        params['child_order_id'] = child_order_id
    if child_order_acceptance_id != '':
        params['child_order_acceptance_id'] = child_order_acceptance_id
    if parent_order_id != '':
        params['parent_order_id'] = parent_order_id

    bf = BitflyerAPI(method, process_path, params=params)

    response = bf.get(private=True, name='get_child_orders')
    df = pd.DataFrame()
    if response.status_code == 200:
        df = pd.DataFrame(response.json())
        if not df.empty:
            df['child_order_date'] = pd.to_datetime(
                df['child_order_date'], utc=True)
            df['child_order_date'] = df['child_order_date'].dt.tz_convert(
                region)
            df = df.sort_values('child_order_date').reset_index(drop=True)
            df = df.set_index('child_order_acceptance_id')
    else:
        response_json = response.json()
        logger.error(response_json['error_message'])
        raise Exception(f"get_child_order was failed")

    return df


def send_child_order(product_code, child_order_type, side, price, size,
                     minute_to_expire=43200, time_in_force='GTC'):
    """新規注文を出す
    Args:
        product_code (str): 注文するプロダクト。BTC_JPY, ETH_BTC, BCH_BTC, ETH_JPYが利用可能
        child_order_type (str): 注文タイプ。指値注文（値段を指定して購入）の場合は "LIMIT", 成行注文（現在の価格で購入）の場合は "MARKET" を指定
        side (str): 買い注文の場合は "BUY", 売り注文の場合は "SELL" を指定
        price (int): 価格を指定。指値注文（LIMIT）の場合に用いる。
        size (int): 注文数量を指定。
        minute_to_expire (int, optional): 期限切れまでの時間を分で指定。デフォルトは43200(30日間)
        time_in_force (str, optional): 執行数量条件 を "GTC", "IOC", "FOK" のいずれかで指定。デフォルトは 'GTC'.

    Returns:
        dict: レスポンス
    """
    method = 'POST'
    process_path = HTTP_PRIVATE_API[method]['send_child_order']

    body = {
        "product_code": product_code,
        "child_order_type": child_order_type,
        "side": side,
        "price": price,
        "size": size,
        "minute_to_expire": minute_to_expire,
        "time_in_force": time_in_force
    }

    bf = BitflyerAPI(method, process_path)

    result = bf.post(body, name='send_child_order')

    return result


def cancel_child_order(product_code, child_order_acceptance_id):
    """注文をキャンセルする
    Args:
        product_code (str): 注文するプロダクト。BTC_JPY, ETH_BTC, BCH_BTC, ETH_JPYが利用可能
        child_order_id (str): キャンセルする注文の ID

    Returns:
        dict: レスポンス
    """
    method = 'POST'
    process_path = HTTP_PRIVATE_API[method]['cancel_child_order']

    body = {
        "product_code": product_code,
        'child_order_acceptance_id': child_order_acceptance_id
    }

    bf = BitflyerAPI(method, process_path)

    result = bf.post(body, name='cancel_child_order')

    return result
