import datetime
import math
import os
import time
from logging import getLogger
from pathlib import Path

import pandas as pd
from bitflyer_api import (cancel_child_order, get_balance, get_child_orders,
                          send_child_order)
from line_messaging_api_client import LineMessagingAPIClient
from manage import CHILD_ORDERS_DIR, REF_LOCAL
from utils import df_to_csv, path_exists, read_csv, rm_file

if not REF_LOCAL:
    from aws import S3
    s3 = S3()

logger = getLogger(__name__)


class AI:
    """自動売買システムのアルゴリズム

    """

    def __init__(self,
                 latest_summary,
                 product_code,
                 min_size,
                 min_volume_short=2000,
                 min_volume_long=10000,
                 max_volume_short=10000,
                 max_volume_long=30000,
                 min_reward_rate=0.01,
                 min_local_price_gap_rate=0.03,
                 time_diff=9,
                 region='Asia/Tokyo',
                 bucket_name='',
                 line_notify=LineMessagingAPIClient()):

        self.product_code = product_code
        self.min_size = min_size

        self.min_reward_rate = min_reward_rate
        self.min_local_price_gap_rate = min_local_price_gap_rate

        self.df_balance = get_balance()
        self.df_balance = self.df_balance.set_index('currency_code', drop=True)

        p_child_orders_dir = Path(CHILD_ORDERS_DIR)
        p_child_orders_dir = p_child_orders_dir.joinpath(self.product_code)
        self.p_child_orders_path = {
            'long': p_child_orders_dir.joinpath('long_term.csv'),
            'short': p_child_orders_dir.joinpath('short_term.csv'),
            'dca': p_child_orders_dir.joinpath('dca.csv')
        }
        self.child_orders = {
            'long': pd.DataFrame(),
            'short': pd.DataFrame(),
            'dca': pd.DataFrame(),
        }

        self.line_notify = line_notify

        self.latest_summary = latest_summary

        for term in ['long', 'short', 'dca']:
            if path_exists(self.p_child_orders_path[term]):
                self.child_orders[term] = read_csv(
                    str(self.p_child_orders_path[term])
                )

                self.child_orders[term] = self.child_orders[term].set_index(
                    'child_order_acceptance_id',
                    drop=True,
                )

                self.child_orders[term]['child_order_date'] = pd.to_datetime(self.child_orders[term]['child_order_date'])
                self.child_orders[term]['child_order_date'] = self.child_orders[term]['child_order_date'].dt.tz_convert(region)

        self.datetime_references = {
            'now': datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))),
        }
        self.datetime_references['hourly'] = self.datetime_references['now'] - datetime.timedelta(hours=6)

        self.datetime_references['daily'] = self.datetime_references['now'] - datetime.timedelta(days=1)

        self.datetime_references['weekly'] = self.datetime_references['now'] - datetime.timedelta(days=7)

        self.datetime_references['monthly'] = self.datetime_references['now'] - datetime.timedelta(days=31)

        self.min_volume = {
            'long': min_volume_long,
            'short': min_volume_short,
        }

        self.max_volume = {
            'long': max_volume_long,
            'short': max_volume_short,
        }

        self.max_buy_prices_rate = {
            'long': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_LONG')),
            'short': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_SHORT')),
            'dca': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_DCA')),
        }

    def _delete_order(self, term, child_order_acceptance_id):
        target_record = self.child_orders[term].loc[child_order_acceptance_id]
        self.child_orders[term].drop(
            index=[child_order_acceptance_id],
            inplace=True
        )
        # csvファイルを更新
        if len(self.child_orders[term]) == 0:
            rm_file(self.p_child_orders_path[term])
        else:
            df_to_csv(str(self.p_child_orders_path[term]), self.child_orders[term], index=True)

        logger.debug(f'{str(self.p_child_orders_path[term])} が更新されました。')
        return target_record

    def load_latest_child_orders(self,
                                 term,
                                 child_order_cycle,
                                 child_order_acceptance_id,
                                 related_child_order_acceptance_id='no_id'):
        logger.debug(f'child_order_acceptance_id: {child_order_acceptance_id}')
        # get a child order from api
        child_orders_tmp = pd.DataFrame()
        start_time = time.time()
        while child_orders_tmp.empty:
            child_orders_tmp = get_child_orders(
                product_code=self.product_code,
                region='Asia/Tokyo',
                child_order_acceptance_id=child_order_acceptance_id
            )
            if time.time() - start_time > 5:
                logger.warning(f'{child_order_acceptance_id} はすでに存在しないため、ファイルから削除します。')
                child_orders_tmp = self._delete_order(
                    term=term,
                    child_order_acceptance_id=child_order_acceptance_id
                )
                self.line_notify.notify(
                    "\n【注文が手動で削除されました】\n"
                    + f"term:\n{term}\n"
                    + f"child_order_cycle:\n{child_order_cycle}\n"
                    + f"child_order_acceptance_id:\n{child_order_acceptance_id}"
                )
                if child_orders_tmp['side'] == "SELL":
                    child_order_acceptance_id = child_orders_tmp['related_child_order_acceptance_id']
                    related_child_order_acceptance_id = 'no_id'
                    child_orders_tmp = get_child_orders(
                        product_code=self.product_code,
                        region='Asia/Tokyo',
                        child_order_acceptance_id=child_order_acceptance_id
                    )
                else:
                    return

        child_orders_tmp['child_order_cycle'] = child_order_cycle
        child_orders_tmp['related_child_order_acceptance_id'] = related_child_order_acceptance_id
        child_orders_tmp['total_commission_yen'] = 0
        child_orders_tmp['profit'] = 0
        child_orders_tmp['volume'] = child_orders_tmp['price'] * child_orders_tmp['size']

        if self.child_orders[term].empty:
            self.child_orders[term] = child_orders_tmp
        else:
            self.child_orders[term].loc[child_order_acceptance_id] = child_orders_tmp.loc[child_order_acceptance_id]

        if self.child_orders[term].at[child_order_acceptance_id,
                                      'child_order_state'] == 'COMPLETED':
            # 取引手数料を算出
            total_commission = self.child_orders[term].at[child_order_acceptance_id,
                                                          'total_commission']
            price = self.child_orders[term].at[child_order_acceptance_id, 'price']
            self.child_orders[term].at[child_order_acceptance_id,
                                       'total_commission_yen'] = price * total_commission

            if self.child_orders[term].at[child_order_acceptance_id, 'related_child_order_acceptance_id'] == 'no_id' \
                    or self.child_orders[term].at[child_order_acceptance_id, 'side'] == 'SELL':
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {self.child_orders[term].at[child_order_acceptance_id, "side"]}  {child_order_acceptance_id}] 約定しました!'
                )
                self.line_notify.notify(
                    "\n【約定しました】\n"
                    + f"term:\n{term}\n"
                    + f"child_order_cycle:\n{child_order_cycle}\n"
                    + f"order_price:\n{price}\n"
                    + f"price_discount_rate:\n{round(price/self.latest_summary['BUY']['all']['price']['high'],3)}\n"
                    + f"size:\n{self.child_orders[term].at[child_order_acceptance_id, 'size']}\n"
                    + f"volume:\n{self.child_orders[term].at[child_order_acceptance_id, 'volume']}\n"
                    + f"child_order_acceptance_id:\n{child_order_acceptance_id}"
                )

            if self.child_orders[term].at[child_order_acceptance_id, 'side'] == 'SELL':
                sell_price = self.child_orders[term].at[child_order_acceptance_id, 'price']
                sell_size = self.child_orders[term].at[child_order_acceptance_id, 'size']
                sell_commission = self.child_orders[term].at[child_order_acceptance_id,
                                                             'total_commission_yen']

                buy_price = self.child_orders[term].at[related_child_order_acceptance_id, 'price']
                buy_size = self.child_orders[term].at[related_child_order_acceptance_id, 'size']
                buy_commission = self.child_orders[term].at[related_child_order_acceptance_id,
                                                            'total_commission_yen']

                profit = sell_price * sell_size - buy_price * buy_size
                profit -= sell_commission + buy_commission

                logger.info(f'[{self.product_code} {term} {child_order_cycle}] {profit}円の利益が発生しました。')

                self.child_orders[term].at[child_order_acceptance_id, 'profit'] = profit
                self.child_orders[term]['cumsum_profit'] = self.child_orders[term]['profit'].cumsum()

                self.line_notify.notify(f"{profit}円の利益が発生しました")

        # csvファイルを更新
        df_to_csv(str(self.p_child_orders_path[term]), self.child_orders[term], index=True)
        logger.debug(f'{str(self.p_child_orders_path[term])} が更新されました。')

    def update_child_orders(self,
                            term,
                            child_order_acceptance_id="",
                            child_order_cycle="",
                            related_child_order_acceptance_id="no_id"):

        # --------------------------------
        # 既存の注文における約定状態を更新
        # --------------------------------
        for child_order_acceptance_id_tmp in self.child_orders[term].index.tolist():
            if self.child_orders[term].at[child_order_acceptance_id_tmp,
                                          'child_order_state'] == 'ACTIVE':
                self.load_latest_child_orders(
                    term=term,
                    child_order_cycle=self.child_orders[term].at[child_order_acceptance_id_tmp,
                                                                 'child_order_cycle'],
                    child_order_acceptance_id=child_order_acceptance_id_tmp,
                    related_child_order_acceptance_id=self.child_orders[term].at[child_order_acceptance_id_tmp,
                                                                                 'related_child_order_acceptance_id']
                )
        # --------------------------------
        # related_child_order_acceptance_idを指定して、注文情報を更新
        # --------------------------------
        if not child_order_acceptance_id == "":
            if child_order_cycle == "":
                raise ValueError("child_order_cycle must be setted")
            self.load_latest_child_orders(
                term=term,
                child_order_cycle=child_order_cycle,
                child_order_acceptance_id=child_order_acceptance_id,
                related_child_order_acceptance_id=related_child_order_acceptance_id
            )

    def _cancel(self,
                term,
                child_order_cycle,
                child_order_acceptance_id,
                child_order_type='buy'):
        # ----------------------------------------------------------------
        # キャンセル処理
        # ----------------------------------------------------------------
        response = cancel_child_order(
            product_code=self.product_code,
            child_order_acceptance_id=child_order_acceptance_id
        )
        if response.status_code == 200:
            self._delete_order(term, child_order_acceptance_id)
            print('================================================================')
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}  {child_order_type} {child_order_acceptance_id}] のキャンセルに成功しました。'
            )
            print('================================================================')
            self.df_balance = get_balance()
            self.df_balance = self.df_balance.set_index('currency_code', drop=True)
        else:
            response_json = response.json()
            logger.error(response_json['error_message'])
            self.line_notify.notify(
                f"\n【{self.product_code} のキャンセルに失敗しました】\n"
                + f"reason:\n{response_json['error_message']}\n"
                + f"term:\n{term}\n"
                + f"child_order_cycle:\n{child_order_cycle}\n"
                + f"child_order_acceptance_id:\n{child_order_acceptance_id}"
            )
            raise Exception("Cancel of buying order was failed")

    def _buy(self, term, child_order_cycle, local_prices):
        global_prices = self.latest_summary['BUY']['all']['price']
        local_global_price_rate = local_prices['low'] / global_prices['high']
        max_price_rate_th = 1 / 2

        if 1 - local_global_price_rate > max_price_rate_th:
            price_rate = 1
        else:
            price_rate = -4 * (1 - self.max_buy_prices_rate[term]) * (max_price_rate_th - local_global_price_rate) ** 2 + 1
            # price_rate = 2 * (1 - self.max_buy_prices_rate[term]) * (max_price_rate_th - local_global_price_rate) + self.max_buy_prices_rate[term]
            # price_rate = 4 * (1 - self.max_buy_prices_rate[term]) * (max_price_rate_th - local_global_price_rate) ** 2 + self.max_buy_prices_rate[term]

        price = int(local_prices['low'] * price_rate)
        if price >= global_prices['high'] * self.max_buy_prices_rate[term]:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price}] 過去最高価格に近いため、購入できません。'
            )
            return
        elif price <= self.latest_summary['BUY']['now']['price'] * 0.75:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price}] 注文価格が低すぎるため、購入できません。'
            )
            return
        elif price > local_prices['high'] * (1 - self.min_local_price_gap_rate):
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price} {local_prices["high"] * (1 - self.min_local_price_gap_rate)}] 注文価格が直近の最高価格と近すぎるため、購入できません。'
            )
            return
        elif local_prices['high'] / local_prices['low'] < (1 + self.min_local_price_gap_rate):
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price} {local_prices["high"] * (1 - self.min_local_price_gap_rate)}] 直近の最高価格と最低価格のギャップが小さすぎるため、購入できません。'
            )
            return

        # size_rate = 100 * (self.max_buy_prices_rate[term] - price / global_prices['high']) ** 2 + 1
        # size = self.min_size[term] * size_rate

        # volume_rate = 100 * (self.max_buy_prices_rate[term] - price / global_prices['high']) ** 2 + 1
        # volume = self.min_volume[term] * volume_rate

        # if volume < self.min_volume[term]:
        #     volume = self.min_volume[term]
        # elif volume > self.max_volume[term]:
        #     volume = self.max_volume[term]

        # 最大volumeで注文する際の過去最大価格に対する注文価格の割合
        max_volume_rate = 0.5

        if (price / global_prices['high']) <= max_volume_rate:
            volume = self.max_volume[term]
        else:
            volume = - ((self.max_volume[term] - self.min_volume[term]) / (self.max_buy_prices_rate[term] - max_volume_rate)) * \
                ((price / global_prices['high']) - max_volume_rate) + self.max_volume[term]

        if volume > self.df_balance.at['JPY', 'available']:
            volume = int(self.df_balance.at['JPY', 'available'])

        size = volume / price
        sig_digits = 3
        size = math.floor(size * 10 ** sig_digits) / (10 ** sig_digits)

        if size < self.min_size:
            size = self.min_size

        if size * price > self.df_balance.at['JPY', 'available']:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {volume}] JPYが不足しているため新規の買い注文ができません。'
            )
            return

        buy_active_same_price = pd.DataFrame()
        target_buy_history = pd.DataFrame()
        target_buy_history_active = pd.DataFrame()
        target_buy_history_completed = pd.DataFrame()
        same_category_buy_order = pd.DataFrame()
        target_datetime = self.datetime_references[child_order_cycle]
        if not self.child_orders[term].empty:
            buy_active_same_price = self.child_orders[term].query(
                'side == "BUY" and child_order_state == "ACTIVE" and price == @price and size == @size'
            )
            not_saled_buy_order = self.child_orders[term].query(
                'side == "BUY" and child_order_state == "COMPLETED" and related_child_order_acceptance_id == "no_id"'
            )
            target_buy_history = self.child_orders[term].query(
                'side == "BUY" and child_order_date > @target_datetime and child_order_cycle == @child_order_cycle'
            )
            target_buy_history_active = target_buy_history.query(
                'child_order_state == "ACTIVE"'
            )
            target_buy_history_completed = target_buy_history.query(
                'child_order_state == "COMPLETED"'
            )
            same_category_buy_order = self.child_orders[term].query(
                'side == "BUY" and child_order_state == "ACTIVE" and child_order_cycle == @child_order_cycle'
            ).copy()
            same_category_sell_order = self.child_orders[term].query(
                'side == "SELL" and child_order_state == "ACTIVE" and child_order_cycle == @child_order_cycle'
            ).copy()

        if not buy_active_same_price.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 同じ価格かつ同じサイズでの注文がすでにあるため、購入できません。'
            )
            return

        if not not_saled_buy_order.empty and term == 'short':
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 以前の買い注文に対する売り注文が完了していないため、新規の買い注文はできません。'
            )
            return

        if not target_buy_history_completed.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 約定済みの注文から十分な時間が経過していないため、新規の買い注文はできません。'
            )
            return

        if not same_category_sell_order.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 同じサイクルを持つACTIVEな売り注文が存在するため、新規の買い注文はできません。'
            )
            return
        elif same_category_buy_order.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 同じサイクルを持つACTIVEな買い注文が存在しないため、買い注文を行います。'
            )
        else:
            if len(same_category_buy_order) >= 2:
                logger.error(
                    f'[{term} {child_order_cycle}]同じサイクルを持つACTIVEな買い注文が2つ以上あります。'
                )
            if target_buy_history_active.empty:
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {same_category_buy_order.index[0]}] 前回の注文からサイクル時間以上の間約定しなかったため、買い注文を更新します。'
                )
                self.line_notify.notify(
                    f"\n【{self.product_code} の買い注文をキャンセルしました】\n"
                    + "reason:\n前回の注文からサイクル時間以上の間約定しなかったため\n"
                    + f"term:\n{term}\n"
                    + f"child_order_cycle:\n{child_order_cycle}\n"
                    + f"child_order_acceptance_id:\n{same_category_buy_order.index[0]}"
                )
            else:
                if price == same_category_buy_order['price'].values[0]:
                    logger.info(
                        f'[{self.product_code} {term} {child_order_cycle}] すでに注文済みのため、購入できません。'
                    )
                    return
                else:
                    logger.info(
                        f'[{self.product_code} {term} {child_order_cycle}] 価格が変動したため、買い注文を更新します。'
                    )
                    self.line_notify.notify(
                        f"\n【{self.product_code} の買い注文をキャンセルしました】\n"
                        + "reason:\n適正注文価格が変動したため\n"
                        + f"term:\n{term}\n"
                        + f"child_order_cycle:\n{child_order_cycle}\n"
                        + f"child_order_acceptance_id:\n{same_category_buy_order.index[0]}"
                    )

            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {same_category_buy_order["price"].values[0]} {same_category_buy_order["size"].values[0]}] 買い注文をキャンセルします。'
            )
            self._cancel(
                term=term,
                child_order_cycle=child_order_cycle,
                child_order_acceptance_id=same_category_buy_order.index[0],
                child_order_type='buy'
            )
        # ----------------------------------------------------------------
        # 買い注文
        # ----------------------------------------------------------------
        response = send_child_order(
            self.product_code, 'LIMIT', 'BUY', price=price, size=size
        )
        response_json = response.json()
        if response.status_code == 200:
            print('================================================================')
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price} {size} '
                + f'{response_json["child_order_acceptance_id"]}] 買い注文に成功しました!!'
            )
            print('================================================================')
            self.update_child_orders(
                term=term,
                child_order_acceptance_id=response_json['child_order_acceptance_id'],
                child_order_cycle=child_order_cycle,
            )
            self.line_notify.notify(
                f"\n{self.product_code}の買い注文を行いました\n"
                + f"term:\n{term}\n"
                + f"child_order_cycle:\n{child_order_cycle}\n"
                + f"order_price:\n{price}\n"
                + f"price_discount_rate:\n{round(price/self.latest_summary['BUY']['all']['price']['high'],3)}\n"
                + f"size:\n{size}\n"
                + f"volume:\n{price*size}\n"
                + f"child_order_acceptance_id:\n{response_json['child_order_acceptance_id']}"
            )
        else:
            response_json = response.json()
            logger.error(response_json['error_message'])
            self.line_notify.notify(
                f"\n【{self.product_code} の買い注文に失敗しました】\n"
                + f"reason:\n{response_json['error_message']}\n"
                + f"term:\n{term}\n"
                + f"child_order_cycle:\n{child_order_cycle}"
            )

    def _sell(self, term, child_order_cycle, price):
        if self.child_orders[term].empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 買い注文がないため、売り注文はできません。'
            )
            return

        related_buy_order = self.child_orders[term].query(
            'side=="BUY" and child_order_state == "COMPLETED" and child_order_cycle == @child_order_cycle and related_child_order_acceptance_id == "no_id"').copy()
        if related_buy_order.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 約定済みの買い注文がないため、売り注文はできません。'
            )
        else:
            if len(related_buy_order) >= 2:
                logger.warning(
                    f'[{self.product_code} {term} {child_order_cycle}] 同じフラグを持つ約定済みの買い注文が2つ以上あります。'
                )
            for i in range(len(related_buy_order)):
                # price = int(int(related_buy_order['price'].values[i]) * rate)
                # if price < self.latest_summary['SELL']['6h']['price']['high']:
                #     price = self.latest_summary['SELL']['6h']['price']['high']
                if price <= related_buy_order['price'].values[i] * (1 + self.min_reward_rate):
                    logger.info(
                        f'[{self.product_code} {term} {child_order_cycle} {price} {related_buy_order["price"]}] 売値が買値よりも低いため、売り注文はできません。'
                    )
                    continue
                size = round(float(related_buy_order['size'].values[i]), 3)
                response = send_child_order(self.product_code, 'LIMIT', 'SELL',
                                            price=price, size=size)
                if response.status_code == 200:
                    response_json = response.json()
                    print('================================================================')
                    logger.info(
                        f'[{self.product_code} {term} {child_order_cycle} {price} {size} {response_json["child_order_acceptance_id"]} '
                        + f'{price * size}] 売り注文に成功しました！！'
                    )
                    print('================================================================')

                    self.update_child_orders(
                        term=term,
                        child_order_cycle=child_order_cycle,
                        related_child_order_acceptance_id=related_buy_order.index[i],
                        child_order_acceptance_id=response_json['child_order_acceptance_id'],
                    )
                    self.update_child_orders(
                        term=term,
                        child_order_cycle=child_order_cycle,
                        related_child_order_acceptance_id=response_json['child_order_acceptance_id'],
                        child_order_acceptance_id=related_buy_order.index[i],
                    )
                    self.line_notify.notify(
                        f"\n{self.product_code}の売り注文を行いました\n"
                        + f"term:\n{term}\n"
                        + f"child_order_cycle:\n{child_order_cycle}\n"
                        + f"order_price:\n{price}\n"
                        + f"price_discount_rate:\n{round(price/self.latest_summary['BUY']['all']['price']['high'],3)}\n"
                        + f"size:\n{size}\n"
                        + f"volume:\n{price*size}\n"
                        + f"child_order_acceptance_id:\n{response_json['child_order_acceptance_id']}"
                    )
                else:
                    response_json = response.json()
                    logger.error(response_json['error_message'])
                    self.line_notify.notify(
                        f"\n【{self.product_code} の売り注文に失敗しました】\n"
                        + f"reason:\n{response_json['error_message']}\n"
                        + f"term:\n{term}\n"
                        + f"child_order_cycle:\n{child_order_cycle}\n"
                        + f"related_child_order_acceptance_id:\n{related_buy_order.index[i]}"
                    )

    def update_unrealized_profit(self, term):
        if not self.child_orders[term].empty:
            self.child_orders[term]['profit'] = self.child_orders[term]['size'] \
                * (self.latest_summary['BUY']['now']['price'] - self.child_orders[term]['price']) \
                - self.child_orders[term]['total_commission_yen']

            self.child_orders[term].loc[self.child_orders[term]
                                        ['child_order_state'] == 'ACTIVE', 'profit'] = 0

            self.child_orders[term]['cumsum_profit'] = self.child_orders[term]['profit'].cumsum()

            # csvファイルを更新
            df_to_csv(str(self.p_child_orders_path[term]), self.child_orders[term], index=True)
            logger.debug(f'{str(self.p_child_orders_path[term])} が更新されました。')

    def long_term(self):
        # 最新情報を取得
        self.update_child_orders(term='long')

        if int(os.environ.get('LONG_DAILY', 0)):
            # daily
            self._buy(
                term='long',
                child_order_cycle='daily',
                local_prices=self.latest_summary['BUY']['1d']['price']
            )

        if int(os.environ.get('LONG_WEEKLY', 1)):
            # weekly
            self._buy(
                term='long',
                child_order_cycle='weekly',
                local_prices=self.latest_summary['BUY']['1w']['price']
            )

        if int(os.environ.get('LONG_MONTHLY', 0)):
            # monthly
            self._buy(
                term='long',
                child_order_cycle='monthly',
                local_prices=self.latest_summary['BUY']['1m']['price']
            )

    def short_term(self):

        # 最新情報を取得
        self.update_child_orders(term='short')

        if int(os.environ.get('SHORT_HOURLY', 1)):
            # hourly
            self._buy(
                term='short',
                child_order_cycle='hourly',
                local_prices=self.latest_summary['BUY']['12h']['price']
            )

            self._sell(
                term='short',
                child_order_cycle='hourly',
                # rate=float(os.environ.get('SELL_RATE_SHORT_HOURLY', 1.10)),
                price=self.latest_summary['SELL']['12h']['price']['high']
            )

        if int(os.environ.get('SHORT_DAILY', 0)):
            # daily
            self._buy(
                term='short',
                child_order_cycle='daily',
                local_prices=self.latest_summary['BUY']['1d']['price']
            )

            self._sell(
                term='short',
                child_order_cycle='daily',
                # rate=float(os.environ.get('SELL_RATE_SHORT_DAILY', 1.10)),
                price=self.latest_summary['SELL']['1d']['price']['high']
            )

        if int(os.environ.get('SHORT_WEEKLY', 0)):
            # weekly
            self._buy(
                term='short',
                child_order_cycle='weekly',
                local_prices=self.latest_summary['BUY']['1w']['price']
            )
            self._sell(
                term='short',
                child_order_cycle='weekly',
                # rate=float(os.environ.get('SELL_RATE_SHORT_WEEKLY', 1.10)),
                price=self.latest_summary['SELL']['1w']['price']['high']
            )

    def dca(self, min_volume, max_volume, st_buy_price_rate=1, price_rate=1, cycle='monthly'):
        """フレキシブルドルコスト平均法(Flexible Dollar Cost Averaging)による積立投資

        Args:
            volume (int)): 購入金額
            st_buy_price_rate: (float, optional): 購入開始となる、過去最高価格に対する現在価格の割引率。 Defaults to 1.
            price_rate (float, optional): 現在価格に対する注文価格の割合。 Defaults to 1.
            cycle (str, optional): 積立頻度。 Defaults to 'monthly'.
        """
        # 最新情報を取得
        self.update_child_orders(term='dca')

        price = int(self.latest_summary['BUY']['now']['price'] * price_rate)

        if price > self.latest_summary['BUY']['all']['price']['high'] * st_buy_price_rate:
            logger.info(
                f"[{self.product_code} DCA {cycle} {price} {self.latest_summary['BUY']['all']['price']['high'] * st_buy_price_rate}] 注文価格が過去最高価格の{st_buy_price_rate}倍以上であるため注文できません。"
            )
            return

        if min_volume < self.latest_summary['BUY']['now']['price'] * self.min_size:
            min_volume = int(self.latest_summary['BUY']['now']['price'] * self.min_size)

        if max_volume < min_volume:
            max_volume = min_volume

        current_price_discount_rate = price / self.latest_summary['BUY']['all']['price']['high']

        # x = current_price_discount_rate
        # coef = (max_volume - min_volume) / (1 - self.max_buy_prices_rate['dca'])**2
        # volume = coef * (1 - current_price_discount_rate)**2 + min_volume
        #        = (max_volume - min_volume) / (1 - self.max_buy_prices_rate['dca'])**2 * (1 - current_price_discount_rate)**2 + min_volume

        x = current_price_discount_rate
        coef = (max_volume - min_volume) / (self.max_buy_prices_rate['dca'] - st_buy_price_rate)**2
        volume = coef * (x - st_buy_price_rate)**2 + min_volume

        if volume > max_volume:
            volume = max_volume

        size = volume / price
        sig_digits = 3
        size = math.floor(size * 10 ** sig_digits) / (10 ** sig_digits)

        if size < self.min_size:
            size = self.min_size

        if not self.child_orders['dca'].empty:
            target_date = self.datetime_references[cycle]
            latest_trade_date = self.child_orders['dca'].query('child_order_date > @target_date')
            if len(latest_trade_date) == 1:
                logger.info(
                    f'[{self.product_code} DCA {cycle}] すでに注文済みです。'
                )
                return

        if size * price > self.df_balance.at['JPY', 'available']:
            logger.info(
                f'[{self.product_code} DCA {cycle} {volume}] JPYが不足しているため新規の買い注文ができません。'
            )
            return

        # ----------------------------------------------------------------
        # 買い注文
        # ----------------------------------------------------------------
        response = send_child_order(
            self.product_code, 'LIMIT', 'BUY', price=price, size=size
        )
        response_json = response.json()
        if response.status_code == 200:
            print('================================================================')
            logger.info(
                f'[{self.product_code} DCA {cycle} {price} {size} '
                + f'{response_json["child_order_acceptance_id"]}] 買い注文に成功しました!!'
            )
            print('================================================================')
            self.update_child_orders(
                term='dca',
                child_order_acceptance_id=response_json['child_order_acceptance_id'],
                child_order_cycle=cycle,
            )
