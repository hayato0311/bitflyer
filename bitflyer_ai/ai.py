from bitflyer_api import *
import pandas as pd
from pathlib import Path
from logging import getLogger
from manage import REF_LOCAL, BUCKET_NAME

logger = getLogger(__name__)


if not REF_LOCAL:
    from aws import S3
    s3 = S3()


CHILD_ORDERS_DIR = 'child_orders'


class AI:
    """自動売買システムのアルゴリズム

    """

    def __init__(self, latest_summary, product_code,
                 min_size_short=0.01, min_size_long=0.1,  time_diff=9, region='Asia/Tokyo',
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
            if REF_LOCAL and self.p_child_orders_path[term].exists():
                self.child_orders[term] = pd.read_csv(
                    str(self.p_child_orders_path[term])
                )
                self.child_orders[term] = self.child_orders[term].set_index(
                    'child_order_acceptance_id',
                    drop=True
                )

                self.child_orders[term]['child_order_date'] = pd.to_datetime(
                    self.child_orders[term]['child_order_date'])
                self.child_orders[term]['child_order_date'] = self.child_orders[term]['child_order_date'].dt.tz_convert(
                    region)
            elif not REF_LOCAL and s3.key_exists(str(self.p_child_orders_path[term])):
                self.child_orders[term] = s3.read_csv(
                    str(self.p_child_orders_path[term])
                )

                self.child_orders[term] = self.child_orders[term].set_index(
                    'child_order_acceptance_id',
                    drop=True
                )

                self.child_orders[term]['child_order_date'] = pd.to_datetime(
                    self.child_orders[term]['child_order_date'])
                self.child_orders[term]['child_order_date'] = self.child_orders[term]['child_order_date'].dt.tz_convert(
                    region)

        self.datetime = datetime.datetime.now(
            datetime.timezone(datetime.timedelta(hours=9))
        )

        self.datetime_references = {
            'now': datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))),
        }
        self.datetime_references['hourly'] = \
            self.datetime_references['now'] - datetime.timedelta(hours=1)
        self.datetime_references['daily'] = \
            self.datetime_references['now'] - datetime.timedelta(days=1)
        self.datetime_references['weekly'] = \
            self.datetime_references['now'] - datetime.timedelta(days=7)
        self.datetime_references['monthly'] = \
            self.datetime_references['now'] - datetime.timedelta(days=31)

        self.min_size = {
            'long': min_size_long,
            'short': min_size_short,
        }

        self.max_buy_prices_rate = {
            'long': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_LONG')),
            'short': float(os.environ.get('MAX_BUY_PRICE_RATE_IN_SHORT')),
        }

    def delte_order(self, term, child_order_acceptance_id):
        self.child_orders[term].drop(
            index=[child_order_acceptance_id],
            inplace=True
        )
        # csvファイルを更新
        if REF_LOCAL:
            self.child_orders[term].to_csv(
                str(self.p_child_orders_path[term]))
        else:
            s3.to_csv(
                str(self.p_child_orders_path[term]),
                df=self.child_orders[term]
            )

    def load_latest_child_orders(self, term, child_order_cycle, child_order_acceptance_id,
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
                logger.warning(
                    f'{child_order_acceptance_id} はすでに存在しないため、ファイルから削除します。')
                self.delte_order(
                    term=term,
                    child_order_acceptance_id=child_order_acceptance_id
                )
                break
        if not child_orders_tmp.empty:
            child_orders_tmp['child_order_cycle'] = child_order_cycle
            child_orders_tmp['related_child_order_acceptance_id'] = related_child_order_acceptance_id
            child_orders_tmp['profit'] = 0

            if self.child_orders[term].empty:
                self.child_orders[term] = child_orders_tmp
            else:
                self.child_orders[term].loc[child_order_acceptance_id] = child_orders_tmp.loc[child_order_acceptance_id]

            if self.child_orders[term].at[child_order_acceptance_id, 'child_order_state'] == 'COMPLETED':
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {self.child_orders[term].at[child_order_acceptance_id, "child_order_type"]} {child_order_acceptance_id}] 約定しました!'
                )

                if self.child_orders[term].at[child_order_acceptance_id, 'side'] == 'SELL':
                    profit = self.child_orders[term].at[child_order_acceptance_id, 'price'] * \
                        self.child_orders[term].at[child_order_acceptance_id, 'size'] - \
                        self.child_orders[term].at[related_child_order_acceptance_id, 'price'] * \
                        self.child_orders[term].at[related_child_order_acceptance_id, 'size']
                    logger.info(
                        f'{profit}円の利益が発生しました。'
                    )
                    self.child_orders[term].at[child_order_acceptance_id,
                                               'profit'] = profit

        # csvファイルを更新
        if REF_LOCAL:
            self.child_orders[term].to_csv(str(self.p_child_orders_path[term]))
        else:
            s3.to_csv(
                str(self.p_child_orders_path[term]),
                df=self.child_orders[term]
            )

        logger.debug(f'{str(self.p_child_orders_path[term])} が更新されました。')

    def update_child_orders(self, term,
                            child_order_acceptance_id="",
                            child_order_cycle="",
                            related_child_order_acceptance_id="no_id"):

        # --------------------------------
        # 既存の注文における約定状態を更新
        # --------------------------------
        for child_order_acceptance_id_tmp in self.child_orders[term].index.tolist():
            if self.child_orders[term].at[child_order_acceptance_id_tmp, 'child_order_state'] == 'ACTIVE':
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

    def cancel(self, term, child_order_cycle, child_order_acceptance_id, child_order_type='buy'):
        # ----------------------------------------------------------------
        # キャンセル処理
        # ----------------------------------------------------------------
        response = cancel_child_order(product_code=self.product_code,
                                      child_order_acceptance_id=child_order_acceptance_id)
        if response.status_code == 200:
            self.delte_order(term, child_order_acceptance_id)
            print('================================================================')
            logger.info(
                f'[{term} {child_order_cycle}  {child_order_type} {child_order_acceptance_id}] のキャンセルに成功しました。')
            print('================================================================')
        else:
            response_json = response.json()
            logger.error(response_json['error_message'])
            raise Exception("Cancel of buying order was failed")

    def buy(self, term, child_order_cycle, local_prices):
        global_prices = self.latest_summary['BUY']['all']['price']
        if 1 - local_prices['min'] / global_prices['max'] > 1 / 2:
            price_rate = 1
        else:
            price_rate = -4 * (1 - self.max_buy_prices_rate[term]) * (
                1 - local_prices['min'] / global_prices['max']) ** 2 + 1
            # price_rate = 2 * (1 - self.max_buy_prices_rate[term]) * (
            #     1 - local_prices['min'] / global_prices['max']) + self.max_buy_prices_rate[term]
            # price_rate = 4 * (1 - self.max_buy_prices_rate[term]) * (
            #     1 - local_prices['min'] / global_prices['max']) ** 2 + self.max_buy_prices_rate[term]

        price = int(local_prices['min'] * price_rate)
        if price >= global_prices['max'] * self.max_buy_prices_rate[term]:
            logger.info(
                f'[{term} {child_order_cycle} {price}] 過去最高価格に近いため、購入できません。')
            return

        size_rate = 32 * (self.max_buy_prices_rate[term] -
                          price / global_prices['max']) ** 2 + 1

        size = self.min_size[term] * size_rate

        size = round(size, 3)

        target_buy_history = pd.DataFrame()
        same_category_order = pd.DataFrame()
        target_datetime = self.datetime_references[child_order_cycle]
        if not self.child_orders[term].empty:
            target_buy_history = self.child_orders[term].query(
                'child_order_date > @target_datetime  and child_order_cycle == @child_order_cycle')
            same_category_order = self.child_orders[term].query(
                'child_order_state == "ACTIVE" and child_order_cycle == @child_order_cycle').copy()

        if not target_buy_history.empty and not target_buy_history.empty:
            logger.info(
                f'[{term} {child_order_cycle}] すでに注文済みのため、購入できません。'
            )
            return

        if target_buy_history.empty or same_category_order.empty:
            # ----------------------------------------------------------------
            # 同じカテゴリーの注文がすでに存在していた場合、前の注文をキャンセルする。
            # ----------------------------------------------------------------
            if not same_category_order.empty:
                if len(same_category_order) >= 2:
                    logger.error('同じサイクルを持つACTIVEな買い注文が2つ以上あります。')
                logger.info(
                    f'[{term} {child_order_cycle} {same_category_order.index[0]}] 前回の注文からサイクル時間以上の間約定しなかったため、前回の注文をキャンセルし、新規の買い注文を行います。'
                )
                self.cancel(
                    term=term,
                    child_order_cycle=child_order_cycle,
                    child_order_acceptance_id=same_category_order.index[0],
                    child_order_type='buy'
                )
            else:
                logger.info(
                    f'[{term} {child_order_cycle}] 同じサイクルを持つACTIVEな買い注文が存在しないため、買い注文を行います。'
                )
            # ----------------------------------------------------------------
            # 買い注文
            # ----------------------------------------------------------------

            response = send_child_order(self.product_code, 'LIMIT', 'BUY',
                                        price=price, size=size)
            response_json = response.json()
            if response.status_code == 200:
                print('================================================================')
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {price} {size} {response_json["child_order_acceptance_id"]}] 買い注文に成功しました!!')
                print('================================================================')
                self.update_child_orders(
                    term=term,
                    child_order_acceptance_id=response_json['child_order_acceptance_id'],
                    child_order_cycle=child_order_cycle
                )

    def sell(self, term, child_order_cycle, rate):
        related_buy_order = self.child_orders[term].query(
            'side=="BUY" and child_order_state == "COMPLETED" and child_order_cycle == @child_order_cycle and related_child_order_acceptance_id == "no_id"').copy()
        if related_buy_order.empty:
            logger.info(
                f'[{term}, {child_order_cycle}] 約定済みの買い注文がないため、売り注文はできません。')
        else:
            if len(related_buy_order) >= 2:
                logger.error('同じフラグを持つ約定済みの買い注文が2つあります。')

            price = int(int(related_buy_order['price']) * rate)
            size = round(float(related_buy_order['size']), 3)
            response = send_child_order(self.product_code, 'LIMIT', 'SELL',
                                        price=price, size=size)
            if response.status_code == 200:
                response_json = response.json()
                print('================================================================')
                logger.info(
                    f'[{self.product_code} {term} {child_order_cycle} {price} {size} {response_json["child_order_acceptance_id"]} {int(int(related_buy_order["price"]) * (rate-1)) * size}] 売り注文に成功しました！！')
                print('================================================================')

                self.update_child_orders(
                    term=term,
                    child_order_cycle=child_order_cycle,
                    related_child_order_acceptance_id=related_buy_order.index[0],
                    child_order_acceptance_id=response_json['child_order_acceptance_id'],
                )
                self.update_child_orders(
                    term=term,
                    child_order_cycle=child_order_cycle,
                    related_child_order_acceptance_id=response_json['child_order_acceptance_id'],
                    child_order_acceptance_id=related_buy_order.index[0]
                )

    def long_term(self):
        """
        [買い注文]
        絶対条件: 過去最高額の70%範囲内では購入しない。
        条件1: long_termとして、１日間で1回も買っていない、またはlong_daily_activeがない場合、１日での最安値の80%の価格で指値注文を入れる。(10000円以上)
        条件2: long_termとして、１週間で1回も買っていない、またはlong_weekly_activeがない場合、１週間での最安値の70%の価格で指値注文を入れる。(10000円以上)
        条件3: long_termとして、１ヶ月間で1回も買っていない、またはlong_monthly_activeがない場合、１ヶ月間での最安値の75%の価格で指値注文を入れる。(10000円以上)

        ただし、前回の指値注文がACTIVEな場合はその注文を取り消す。
        """
        # 最新情報を取得
        self.update_child_orders(term='long')

        # =================================================================
        # 条件1:
        #   - 周期:daily
        #   - 指値注文価格: １日の最安値の80%
        #   - サイズ: middle
        # =================================================================
        self.buy(
            term='long',
            child_order_cycle='daily',
            local_prices=self.latest_summary['BUY']['1d']['price']
        )

        # =================================================================
        # 条件2:
        #   - 周期: weekly
        #   - 指値注文価格: １週間の最安値の70%
        #   - サイズ: large
        # =================================================================
        self.buy(
            term='long',
            child_order_cycle='weekly',
            local_prices=self.latest_summary['BUY']['1w']['price']
        )

        # =================================================================
        # 条件3:
        #   - 周期: monthly
        #   - 指値注文価格: １ヶ月間の最安値の75%
        #   - サイズ: middle
        # =================================================================
        self.buy(
            term='long',
            child_order_cycle='monthly',
            local_prices=self.latest_summary['BUY']['1m']['price']
        )

    def short_term(self):
        """
        [買い注文]
        絶対条件: 過去最高額の75%範囲内では購入しない。
        条件1: short_termとして、1時間で1回も買っていない、またはshort_hourly_buy_activeがない場合、1時間での最安値の90%の価格で指値注文を入れる。(10000円以上)
        条件2: short_termとして、１日間で1回も買っていない、またはshort_daily_buy_activeがない場合、１日での最安値の95%の価格で指値注文を入れる。(1000円単位)
        条件3: short_termとして、１週間で1回も買っていない、またはshort_weekly_buy_activeがない場合、１週間での最安値の85%の価格で指値注文を入れる。(1000円単位)

        ただし、前回の指値注文がACTIVEな場合はその注文を取り消す。

        [売り注文]
        条件1: short_hourly_buy_complitedがあり、related_child_order_acceptance_idが'no_id'の場合、その指値注文の110%の価格で指値注文を入れる。(10000円以上)
        条件2: short_daily_buy_complitedがあり、related_child_order_acceptance_idが'no_id'の場合、その指値注文の105%上の価格で指値注文を入れる。(1000円単位)
        条件3: short_weekly_buy_complitedがあり、related_child_order_acceptance_idが'no_id'の場合、その指値注文の115%の価格で指値注文を入れる。(1000円単位)


        """

        # 最新情報を取得
        self.update_child_orders(term='short')

        # =================================================================
        # 買い注文
        # =================================================================

        # =================================================================
        # 条件1:
        #   - 周期:hourly
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='hourly',
            local_prices=self.latest_summary['BUY']['1h']['price']
        )

        # =================================================================
        # 条件2:
        #   - 周期:daily
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='daily',
            local_prices=self.latest_summary['BUY']['1d']['price']
        )

        # =================================================================
        # 条件3:
        #   - 周期:weekly
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='weekly',
            local_prices=self.latest_summary['BUY']['1w']['price']
        )

        # =================================================================
        # 売り注文
        # =================================================================

        if self.child_orders['short'].empty:
            logger.info(f'[short] 買い注文がないため、売り注文はできません。')
            return

        # =================================================================
        # 条件1:
        #   - 周期:hourly
        #   - 指値注文価格: 購入価格の180%
        # =================================================================
        self.sell(
            term='short',
            child_order_cycle='hourly',
            rate=1.80
        )

        # =================================================================
        # 条件2:
        #   - 周期:daily
        #   - 指値注文価格: 購入価格の190%
        # =================================================================
        self.sell(
            term='short',
            child_order_cycle='daily',
            rate=1.90
        )

        # =================================================================
        # 条件3:
        #   - 周期:weekly
        #   - 指値注文価格: 購入価格の200%
        # =================================================================

        self.sell(
            term='short',
            child_order_cycle='weekly',
            rate=2.00
        )
