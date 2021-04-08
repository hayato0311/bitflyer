from bitflyer_api import *
import pandas as pd
from pathlib import Path
from logging import getLogger
from manage import REF_LOCAL, BUCKET_NAME, CHILD_ORDERS_DIR
import os

from utils import path_exists, read_csv, df_to_csv

logger = getLogger(__name__)


if not REF_LOCAL:
    from aws import S3
    s3 = S3()


class AI:
    """自動売買システムのアルゴリズム

    """

    def __init__(self,
                 latest_summary,
                 product_code,
                 min_size_short=0.01,
                 min_size_long=0.1,
                 time_diff=9,
                 region='Asia/Tokyo',
                 bucket_name=''):

        self.product_code = product_code

        p_child_orders_dir = Path(CHILD_ORDERS_DIR)
        p_child_orders_dir = p_child_orders_dir.joinpath(self.product_code)
        self.p_child_orders_path = {
            'long': p_child_orders_dir.joinpath('long_term.csv'),
            'short': p_child_orders_dir.joinpath('short_term.csv')
        }
        self.child_orders = {
            'long': pd.DataFrame(),
            'short': pd.DataFrame()
        }

        self.latest_summary = latest_summary

        for term in ['long', 'short']:
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

        self.min_size = {
            'long': min_size_long,
            'short': min_size_short,
        }

        self.max_buy_prices_rate = {
            'long': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_LONG')),
            'short': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_SHORT')),
        }

    def _delte_order(self, term, child_order_acceptance_id):
        self.child_orders[term].drop(
            index=[child_order_acceptance_id],
            inplace=True
        )
        # csvファイルを更新
        df_to_csv(str(self.p_child_orders_path[term]), self.child_orders[term], index=True)
        logger.debug(f'{str(self.p_child_orders_path[term])} が更新されました。')

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
                self._delte_order(
                    term=term,
                    child_order_acceptance_id=child_order_acceptance_id
                )
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
            self._delte_order(term, child_order_acceptance_id)
            print('================================================================')
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}  {child_order_type} {child_order_acceptance_id}] のキャンセルに成功しました。'
            )
            print('================================================================')
        else:
            response_json = response.json()
            logger.error(response_json['error_message'])
            raise Exception("Cancel of buying order was failed")

    def _buy(self, term, child_order_cycle, local_prices):
        global_prices = self.latest_summary['BUY']['all']['price']
        if 1 - local_prices['low'] / global_prices['high'] > 1 / 2:
            price_rate = 1
        else:
            price_rate = -4 * (1 - self.max_buy_prices_rate[term]) * (
                1 - local_prices['low'] / global_prices['high']) ** 2 + 1
            # price_rate = 2 * (1 - self.max_buy_prices_rate[term]) * (
            #     1 - local_prices['low'] / global_prices['high']) + self.max_buy_prices_rate[term]
            # price_rate = 4 * (1 - self.max_buy_prices_rate[term]) * (
            # 1 - local_prices['low'] / global_prices['high']) ** 2 +
            # self.max_buy_prices_rate[term]

        price = int(local_prices['low'] * price_rate)
        if price >= global_prices['high'] * self.max_buy_prices_rate[term]:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle} {price}] 過去最高価格に近いため、購入できません。'
            )
            return

        size_rate = 100 * (self.max_buy_prices_rate[term] - price / global_prices['high']) ** 2 + 1

        size = self.min_size[term] * size_rate

        size = round(size, 3)

        buy_active_same_price = pd.DataFrame()
        target_buy_history = pd.DataFrame()
        target_buy_history_active = pd.DataFrame()
        target_buy_history_completed = pd.DataFrame()
        same_category_order = pd.DataFrame()
        target_datetime = self.datetime_references[child_order_cycle]
        if not self.child_orders[term].empty:
            buy_active_same_price = self.child_orders[term].query(
                'side == "BUY" and child_order_state == "ACTIVE" and price == @price'
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
            same_category_order = self.child_orders[term].query(
                'side == "BUY" and child_order_state == "ACTIVE" and child_order_cycle == @child_order_cycle'
            ).copy()
        if not buy_active_same_price.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 同じ価格での注文がすでにあるため、購入できません。'
            )
            return
        if not same_category_order.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] すでに注文済みのため、購入できません。'
            )
            return

        if not target_buy_history_completed.empty:
            logger.info(
                f'[{self.product_code} {term} {child_order_cycle}] 約定済みの注文から十分な時間が経過していないため、新規の買い注文はできません。'
            )
            return

        if target_buy_history_active.empty or same_category_order.empty:
            # ----------------------------------------------------------------
            # 同じカテゴリーの注文がすでに存在していた場合、前の注文をキャンセルする。
            # ----------------------------------------------------------------
            if not same_category_order.empty:
                if len(same_category_order) >= 2:
                    logger.error(
                        f'[{term} {child_order_cycle}]同じサイクルを持つACTIVEな買い注文が2つ以上あります。'
                    )
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {same_category_order.index[0]}] 前回の注文からサイクル時間以上の間約定しなかったため、前回の注文をキャンセルし、新規の買い注文を行います。'
                )
                self._cancel(
                    term=term,
                    child_order_cycle=child_order_cycle,
                    child_order_acceptance_id=same_category_order.index[0],
                    child_order_type='buy'
                )
            else:
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle}] 同じサイクルを持つACTIVEな買い注文が存在しないため、買い注文を行います。'
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
                    f'[{self.product_code} {term} {child_order_cycle} {price} {size} {response_json["child_order_acceptance_id"]}] 買い注文に成功しました!!'
                )
                print('================================================================')
                self.update_child_orders(
                    term=term,
                    child_order_acceptance_id=response_json['child_order_acceptance_id'],
                    child_order_cycle=child_order_cycle,
                )

    def _sell(self, term, child_order_cycle, rate):
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
                price = int(int(related_buy_order['price'].values[i]) * rate)
                if price < self.latest_summary['SELL']['6h']['price']['high']:
                    price = self.latest_summary['SELL']['6h']['price']['high']
                size = round(float(related_buy_order['size'].values[i]), 3)
                response = send_child_order(self.product_code, 'LIMIT', 'SELL',
                                            price=price, size=size)
                if response.status_code == 200:
                    response_json = response.json()
                    print('================================================================')
                    logger.info(
                        f'[{self.product_code} {term} {child_order_cycle} {price} {size} {response_json["child_order_acceptance_id"]} {int(int(related_buy_order["price"].values[i]) * (rate-1)) * size}] 売り注文に成功しました！！'
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

    def update_long_term_profit(self):
        if not self.child_orders['long'].empty:
            self.child_orders['long']['profit'] = self.child_orders['long']['size'] \
                * (self.latest_summary['BUY']['now']['price'] - self.child_orders['long']['price']) \
                - self.child_orders['long']['total_commission_yen']

            self.child_orders['long'].loc[self.child_orders['long']
                                          ['child_order_state'] == 'ACTIVE', 'profit'] = 0

            self.child_orders['long']['cumsum_profit'] = self.child_orders['long']['profit'].cumsum()

            # csvファイルを更新
            df_to_csv(str(self.p_child_orders_path['long']), self.child_orders['long'], index=True)
            logger.debug(f'{str(self.p_child_orders_path["long"])} が更新されました。')

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
                local_prices=self.latest_summary['BUY']['6h']['price']
            )

            self._sell(
                term='short',
                child_order_cycle='hourly',
                rate=float(os.environ.get('SELL_RATE_SHORT_HOURLY', 1.10))
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
                rate=float(os.environ.get('SELL_RATE_SHORT_DAILY', 1.10))
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
                rate=float(os.environ.get('SELL_RATE_SHORT_WEEKLY', 1.10))
            )
