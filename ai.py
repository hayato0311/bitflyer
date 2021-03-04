from bitflyer_api import *
import pandas as pd
from pathlib import Path
from logging import getLogger
from manage import LOCAL
logger = getLogger(__name__)


if not LOCAL:
    import boto3
    from io import StringIO


CHILD_ORDERS_DIR = 'child_orders'


class AI:
    """自動売買システムのアルゴリズム

    """

    def __init__(self, latest_summary, product_code='ETH_JPY', prices={}, small_size=0.01, middle_size=0.1, large_size=0.3, time_diff=9, region='Asia/Tokyo'):
        if not LOCAL:
            bucket_name = 'bitflyer-ai'
            s3_get = boto3.client('s3')
            # objkey = container_name + '/' + filename + '.csv'  # 多分普通のパス
            # obj = s3_get.get_object(Bucket=bucket_name, Key=objkey)
            # body = obj['Body'].read()
            # bodystr = body.decode('utf-8')
            # df = pd.read_csv(StringIO(bodystr))

        self.product_code = product_code

        p_child_orders_dir = Path(CHILD_ORDERS_DIR)
        p_child_orders_dir = p_child_orders_dir.joinpath(product_code)
        self.product_code = product_code

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
            if self.p_child_orders_path[term].exists():
                if LOCAL:
                    self.child_orders[term] = pd.read_csv(
                        str(self.p_child_orders_path[term]), index_col=0)
                else:
                    obj = s3_get.get_object(Bucket=bucket_name, Key=str(
                        self.p_child_orders_path[term]))
                    body = obj['Body'].read()
                    bodystr = body.decode('utf-8')
                    self.child_orders[term] = pd.read_csv(StringIO(bodystr))

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

        # self.size = {
        #     'small': small_size,
        #     'middle': middle_size,
        #     'large': large_size
        # }

        self.min_size = {
            'long': 0.1,
            'short': 0.01,
        }

        if self.latest_summary['BUY']['1w']['price']['max'] < self.latest_summary['BUY']['all']['price']['max'] * 0.25:
            self.small_size = middle_size
            self.middle_size = large_size
            self.large_size = large_size

        self.max_buy_prices_rate = {
            'long': 0.7,
            'short': 0.75
        }

    def load_latest_child_orders(self, term, child_order_cycle, child_order_acceptance_id,
                                 related_child_order_acceptance_id='no_id'):
        print(f'child_order_acceptance_id: {child_order_acceptance_id}')
        # get a child order from api
        child_orders_tmp = pd.DataFrame()
        start_time = time.time()
        while child_orders_tmp.empty:
            child_orders_tmp = get_child_orders(
                region='Asia/Tokyo', child_order_acceptance_id=child_order_acceptance_id)
            if time.time() - start_time > 3:
                print(f'{child_order_acceptance_id} はすでに存在しないため、ファイルから削除します。')
                if child_order_acceptance_id in self.child_orders[term].index.tolist():
                    self.child_orders[term].drop(
                        index=[child_order_acceptance_id],
                        inplace=True
                    )
                break
        if not child_orders_tmp.empty:
            child_orders_tmp['child_order_cycle'] = child_order_cycle
            child_orders_tmp['related_child_order_acceptance_id'] = related_child_order_acceptance_id
            if self.child_orders[term].empty:
                self.child_orders[term] = child_orders_tmp
            else:
                self.child_orders[term].loc[child_order_acceptance_id] = child_orders_tmp.loc[child_order_acceptance_id]

        # csvファイルを更新
        self.child_orders[term].to_csv(str(self.p_child_orders_path[term]))
        print(f'{str(self.p_child_orders_path[term])} file was updated')

    def update_child_orders(self, term,
                            child_order_acceptance_id="",
                            child_order_cycle="",
                            related_child_order_acceptance_id="no_id"):

        # --------------------------------
        # 既存の注文における約定状態を更新
        # --------------------------------
        for child_order_acceptance_id_tmp in self.child_orders[term].index.tolist():
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

    def cancel(self, term, child_order_cycle, child_order_acceptance_id):
        # ----------------------------------------------------------------
        # キャンセル処理
        # ----------------------------------------------------------------
        response = cancel_child_order(product_code=self.product_code,
                                      child_order_acceptance_id=child_order_acceptance_id)
        if response.status_code == 200:
            print('================================================================')
            print(
                f'{term}_term {child_order_cycle} order was successfully canceled!!!')
            print(
                f'child_order_acceptance_id: {child_order_acceptance_id}')
            print('================================================================')
            self.child_orders[term].drop(
                index=[child_order_acceptance_id],
                inplace=True
            )
            self.update_child_orders(term=term)
        else:
            raise Exception("Cancel of buying order was failed")

    def buy(self, term, child_order_cycle, reference_price, rate):
        price = int(reference_price * rate)
        if price >= self.latest_summary['BUY']['all']['price']['max'] * self.max_buy_prices_rate[term]:
            print(f'[{term}, {child_order_cycle}] 過去最高価格に近いため、購入できません。')
            return

        size = self.min_size[term] * (1 / (1 - self.max_buy_prices_rate[term]) * (
            1 - (price / self.latest_summary['BUY']['all']['price']['max']))) ** 2

        size = round(size, 3)

        target_buy_history = pd.DataFrame()
        same_category_order = pd.DataFrame()
        target_datetime = self.datetime_references[child_order_cycle]
        if not self.child_orders[term].empty:
            target_buy_history = self.child_orders[term].query(
                'child_order_date > @target_datetime')
            same_category_order = self.child_orders[term].query(
                'child_order_state == "ACTIVE" and child_order_cycle == @child_order_cycle').copy()

        if target_buy_history.empty or same_category_order.empty:
            # ----------------------------------------------------------------
            # 同じカテゴリーの注文がすでに存在していた場合、前の注文をキャンセルする。
            # ----------------------------------------------------------------
            if not same_category_order.empty:
                self.cancel(
                    term=term,
                    child_order_cycle=child_order_cycle,
                    child_order_acceptance_id=same_category_order.index[0]
                )
            # ----------------------------------------------------------------
            # 買い注文
            # ----------------------------------------------------------------

            response = send_child_order(self.product_code, 'LIMIT', 'BUY',
                                        price=price, size=size)
            if response.status_code == 200:
                print('================================================================')
                print('買い注文に成功しました!!')
                print(f'product_code     : {self.product_code}')
                print(f'price            : {price}')
                print(f'size             : {size}')
                print(f'term             : {term}')
                print(f'child_order_cycle: {child_order_cycle}')
                print('================================================================')
                response_json = response.json()
                self.update_child_orders(
                    term=term,
                    child_order_acceptance_id=response_json['child_order_acceptance_id'],
                    child_order_cycle=child_order_cycle
                )
            else:
                raise Exception(
                    f"{term}_term {child_order_cycle} buying order was failed")

    def sell(self, term, child_order_cycle, rate):
        related_buy_order = self.child_orders[term].query(
            'side=="BUY" and child_order_state == "COMPLETED" and child_order_cycle == @child_order_cycle and related_child_order_acceptance_id == "no_id"').copy()
        if related_buy_order.empty:
            print(f'[{term}, {child_order_cycle}] 約定済みの買い注文がないため、売り注文はできません。')
        else:
            if len(related_buy_order) >= 2:
                raise ValueError(
                    '同じフラグを持つ約定済みの買い注文が2つあります。')

            price = int(int(related_buy_order['price']) * rate)
            size = round(float(related_buy_order['size']), 3)
            response = send_child_order(self.product_code, 'LIMIT', 'SELL',
                                        price=price, size=size)
            if response.status_code == 200:
                #  TODO:利益を保存するファイルを生成
                print('================================================================')
                print(f'売り注文に成功しました！！')
                print(
                    f'profit: {int(int(related_buy_order["price"]) * (rate-1)) * size}円')
                print(f'product_code: {self.product_code}')
                print(f'price       : {price}')
                print(f'size        : {size}')
                print(f'term        : {term}')
                print('================================================================')
                response_json = response.json()
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
            else:
                raise Exception(
                    f"{term}_term {child_order_cycle} selling order was failed")

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
            reference_price=self.latest_summary['BUY']['1d']['price']['min'],
            rate=0.80
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
            reference_price=self.latest_summary['BUY']['1w']['price']['min'],
            rate=0.70
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
            reference_price=self.latest_summary['BUY']['1m']['price']['min'],
            rate=0.75
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
        #   - 指値注文価格: 1時間の最安値の95%
        #   - サイズ: small
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='hourly',
            reference_price=self.latest_summary['BUY']['1h']['price']['min'],
            rate=0.95
        )

        # =================================================================
        # 条件2:
        #   - 周期:daily
        #   - 指値注文価格: １日の最安値の95%
        #   - サイズ: small
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='daily',
            reference_price=self.latest_summary['BUY']['1d']['price']['min'],
            rate=0.95
        )

        # =================================================================
        # 条件3:
        #   - 周期:weekly
        #   - 指値注文価格: １週間の最安値の85%
        #   - サイズ: small
        # =================================================================
        self.buy(
            term='short',
            child_order_cycle='weekly',
            reference_price=self.latest_summary['BUY']['1w']['price']['min'],
            rate=0.85
        )

        # =================================================================
        # 売り注文
        # =================================================================

        if self.child_orders['short'].empty:
            print(f'[short] 買い注文がないため、売り注文はできません。')
            return

        # =================================================================
        # 条件1:
        #   - 周期:hourly
        #   - 指値注文価格: 購入価格の110%
        #   - サイズ: small
        # =================================================================
        self.sell(
            term='short',
            child_order_cycle='hourly',
            rate=1.10
        )

        # =================================================================
        # 条件2:
        #   - 周期:daily
        #   - 指値注文価格: 購入価格の190%
        #   - サイズ: small
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
        #   - サイズ: small
        # =================================================================

        self.sell(
            term='short',
            child_order_cycle='weekly',
            rate=2.00
        )
