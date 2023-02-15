import datetime
import os
from logging import (DEBUG, INFO, FileHandler, StreamHandler, basicConfig,
                     getLogger)
from pathlib import Path

import pandas as pd
from ai import AI
from bitflyer_api import get_board_state
from dateutil.relativedelta import relativedelta
from manage import PROFIT_DIR, REF_LOCAL, VOLUME_DIR
from preprocess import (delete_row_data, gen_execution_summaries,
                        get_executions_history, obtain_latest_summary)
from utils import df_to_csv, path_exists, read_csv

if REF_LOCAL:
    sh = StreamHandler()
    fh = FileHandler('./logs/bitflyer_ai.log')
    format = '{asctime} {levelname:5} {filename} {funcName} {lineno}: {message}'

    basicConfig(
        handlers=[sh, fh],
        level=DEBUG,
        format=format, style='{'
    )
else:
    from aws import S3
    s3 = S3()
    # 既存のハンドラーを削除
    root = getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)

    format = '{levelname:5} {filename} {funcName} {lineno}: {message}'
    basicConfig(
        level=INFO,
        format=format, style='{'
    )

logger = getLogger(__name__)


def calc_profit(product_code, child_orders, latest_summary):
    """利益を計算する関数

    必要な取引価格
    - 現在価格
    - 前日の終値
    - 前月の終値
    - 前年の終値

    Args:
        product_code ([type]): [description]
        child_orders ([type]): [description]
        current_datetime ([type]): [description]
    """
    p_profit_dir = Path(PROFIT_DIR)
    p_daily_profit_path = p_profit_dir.joinpath('daily_profit.csv')
    p_monthly_profit_path = p_profit_dir.joinpath('monthly_profit.csv')
    p_yearly_profit_path = p_profit_dir.joinpath('yearly_profit.csv')

    current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

    current_month_start_datetime = current_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    current_month_end_datetime = current_month_start_datetime + relativedelta(months=+1)
    current_month_start_datetime = pd.to_datetime(current_month_start_datetime)
    current_month_end_datetime = pd.to_datetime(current_month_end_datetime)

    current_year_start_datetime = current_datetime.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    current_year_end_datetime = current_year_start_datetime + relativedelta(years=+1)
    current_year_start_datetime = pd.to_datetime(current_year_start_datetime)
    current_year_end_datetime = pd.to_datetime(current_year_end_datetime)

    current_date = current_datetime.strftime('%Y/%m/%d')
    current_month = current_datetime.strftime('%Y/%m')
    current_year = current_datetime.strftime('%Y')

    if path_exists(p_daily_profit_path):
        df_daily_profit = read_csv(str(p_daily_profit_path))
        df_daily_profit = df_daily_profit.set_index('date')
        rearlized_profit_all = 0
        unrealized_profit_all = 0
        if not child_orders['long'].empty:
            unrealized_profit_all += float(child_orders['long']['profit'].sum())
        if not child_orders['dca'].empty:
            unrealized_profit_all += float(child_orders['long']['profit'].sum())
        if not child_orders['short'].empty:
            rearlized_profit_all += float(child_orders['short']['profit'].sum())

            df_active_sell_order = child_orders['short'].query('side == "SELL" and child_order_state == "ACTIVE"')
            if not df_active_sell_order.empty:
                child_order_acceptance_id_list = df_active_sell_order['related_child_order_acceptance_id'].values.tolist()
                for child_order_acceptance_id in child_order_acceptance_id_list:
                    unrealized_profit_all += (latest_summary['SELL']['now']['price'] - child_orders['short'].at[child_order_acceptance_id, 'price']) \
                        * child_orders['short'].at[child_order_acceptance_id, 'size'] \
                        - child_orders['short'].at[child_order_acceptance_id, 'total_commission_yen']

        rearlized_profit = rearlized_profit_all
        unrealized_profit = unrealized_profit_all

        if len(df_daily_profit) >= 2:
            rearlized_profit -= float(df_daily_profit.loc[df_daily_profit.index != current_date, f'{product_code}_realized_profit'].sum())
            unrealized_profit -= float(df_daily_profit.loc[df_daily_profit.index != current_date, f'{product_code}_unrealized_profit'].sum())

        rearlized_profit = round(rearlized_profit, 1)
        unrealized_profit = round(unrealized_profit, 1)

        df_daily_profit.at[current_date, f'{product_code}_total_profit'] = rearlized_profit + unrealized_profit
        df_daily_profit.at[current_date, f'{product_code}_realized_profit'] = rearlized_profit
        df_daily_profit.at[current_date, f'{product_code}_unrealized_profit'] = unrealized_profit

        unrealized_profit_list = []
        realized_profit_list = []
        total_profit_list = []
        for col_num in df_daily_profit.columns.tolist():
            if col_num.endswith('_unrealized_profit'):
                unrealized_profit_list.append(col_num)
            elif col_num.endswith('_realized_profit'):
                realized_profit_list.append(col_num)
            elif col_num.endswith('_total_profit'):
                total_profit_list.append(col_num)

        df_daily_profit = df_daily_profit.fillna(0)
        unrealized_profit_sum = df_daily_profit.loc[current_date, unrealized_profit_list].values.sum()
        realized_profit_sum = df_daily_profit.loc[current_date, realized_profit_list].values.sum()
        total_profit_sum = df_daily_profit.loc[current_date, total_profit_list].values.sum()
        df_daily_profit.at[current_date, 'total_profit'] = round(total_profit_sum, 1)
        df_daily_profit.at[current_date, 'realized_profit'] = round(realized_profit_sum, 1)
        df_daily_profit.at[current_date, 'unrealized_profit'] = round(unrealized_profit_sum, 1)

        df_to_csv(str(p_daily_profit_path), df_daily_profit, index=True)
    else:
        rearlized_profit = 0
        unrealized_profit = 0

        if not child_orders['long'].empty:
            unrealized_profit = child_orders['long']['cumsum_profit'].values[-1]

        if not child_orders['dca'].empty:
            unrealized_profit += child_orders['dca']['cumsum_profit'].values[-1]

        if not child_orders['short'].empty:
            rearlized_profit = child_orders['short']['cumsum_profit'].max()

            df_active_sell_order = child_orders['short'].query('side == "SELL" and child_order_state == "ACTIVE"')
            if not df_active_sell_order.empty:
                child_order_acceptance_id_list = df_active_sell_order['related_child_order_acceptance_id'].values.tolist()
                for child_order_acceptance_id in child_order_acceptance_id_list:
                    unrealized_profit += (latest_summary['SELL']['now']['price'] - child_orders['short'][child_order_acceptance_id, 'price']) \
                        * child_orders['short'][child_order_acceptance_id, 'size'] \
                        - child_orders['short'][child_order_acceptance_id, 'total_commission_yen']

        rearlized_profit = round(rearlized_profit, 1)
        unrealized_profit = round(unrealized_profit, 1)

        daily_profit = [
            {
                'date': current_date,
                'total_profit': rearlized_profit + unrealized_profit,
                'realized_profit': rearlized_profit,
                'unrealized_profit': unrealized_profit,
                f'{product_code}_total_profit': rearlized_profit + unrealized_profit,
                f'{product_code}_realized_profit': rearlized_profit,
                f'{product_code}_unrealized_profit': unrealized_profit,
            },
        ]
        df_daily_profit = pd.DataFrame(daily_profit)
        df_daily_profit = df_daily_profit.set_index('date')
        df_to_csv(str(p_daily_profit_path), df_daily_profit, index=True)

    df_daily_profit.index = pd.to_datetime(df_daily_profit.index)
    df_daily_profit.index = df_daily_profit.index.tz_localize('Asia/Tokyo')

    df_daily_profit_current_month = df_daily_profit[current_month_start_datetime: current_month_end_datetime]
    df_daily_profit_current_month_sum = df_daily_profit_current_month.sum()

    if path_exists(p_monthly_profit_path):
        df_monthly_profit = read_csv(str(p_monthly_profit_path))
        df_monthly_profit = df_monthly_profit.set_index('date')

        if current_month in df_monthly_profit.index.tolist():
            current_month_sum_dict = df_daily_profit_current_month_sum.to_dict()
            for col_name, val in current_month_sum_dict.items():
                if col_name in df_monthly_profit.columns:
                    df_monthly_profit.at[current_month, col_name] = val
                else:
                    df_monthly_profit[col_name] = val

        else:
            current_month_sum_dict = df_daily_profit_current_month_sum.to_dict()
            current_month_profit = []
            for col_name in df_monthly_profit.columns.tolist():
                if col_name in current_month_sum_dict.keys():
                    current_month_profit.append(current_month_sum_dict[col_name])
                else:
                    current_month_profit.append(0)
            df_monthly_profit.loc[current_month] = current_month_profit

        df_to_csv(str(p_monthly_profit_path), df_monthly_profit, index=True)
    else:
        current_month_profit_dict = {'date': current_month}
        current_month_profit_dict.update(df_daily_profit_current_month_sum.to_dict())
        df_monthly_profit = pd.DataFrame([current_month_profit_dict])
        df_monthly_profit = df_monthly_profit.set_index('date')
        df_to_csv(str(p_monthly_profit_path), df_monthly_profit, index=True)

    df_monthly_profit.index = pd.to_datetime(df_monthly_profit.index)
    df_monthly_profit.index = df_monthly_profit.index.tz_localize('Asia/Tokyo')

    df_monthly_profit_current_year = df_monthly_profit[current_year_start_datetime: current_year_end_datetime]
    df_monthly_profit_current_year_sum = df_monthly_profit_current_year.sum()

    if path_exists(p_yearly_profit_path):
        df_yearly_profit = read_csv(str(p_yearly_profit_path))
        df_yearly_profit['date'] = df_yearly_profit['date'].astype(str)
        df_yearly_profit = df_yearly_profit.set_index('date')

        if current_year in df_yearly_profit.index.tolist():
            current_year_sum_dict = df_monthly_profit_current_year_sum.to_dict()
            for col_name, val in current_year_sum_dict.items():
                if col_name in df_yearly_profit.columns:
                    df_yearly_profit.at[current_year, col_name] = val
                else:
                    df_yearly_profit[col_name] = val

        else:
            current_year_sum_dict = df_monthly_profit_current_year_sum.to_dict()
            current_year_profit = []
            for col_name in df_yearly_profit.columns.tolist():
                if col_name in current_year_sum_dict.keys():
                    current_year_profit.append(current_year_sum_dict[col_name])
                else:
                    current_year_profit.append(0)
            df_yearly_profit.loc[current_year] = current_year_profit

        df_to_csv(str(p_yearly_profit_path), df_yearly_profit, index=True)
    else:
        current_year_profit_dict = {'date': current_year}
        current_year_profit_dict.update(df_monthly_profit_current_year_sum.to_dict())
        df_yearly_profit = pd.DataFrame([current_year_profit_dict])
        df_yearly_profit = df_yearly_profit.set_index('date')
        df_to_csv(str(p_yearly_profit_path), df_yearly_profit, index=True)


def calc_volume(product_code, child_orders):
    """取引量を計算する関数

    Args:
        product_code ([type]): [description]
        child_orders ([type]): [description]
        current_datetime ([type]): [description]
    """
    p_volume_dir = Path(VOLUME_DIR)
    p_daily_volume_path = p_volume_dir.joinpath('daily_volume.csv')
    p_monthly_volume_path = p_volume_dir.joinpath('monthly_volume.csv')
    p_yearly_volume_path = p_volume_dir.joinpath('yearly_volume.csv')

    current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))

    current_month_start_datetime = current_datetime.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    current_month_end_datetime = current_month_start_datetime + relativedelta(months=+1)
    current_month_start_datetime = pd.to_datetime(current_month_start_datetime)
    current_month_end_datetime = pd.to_datetime(current_month_end_datetime)

    current_year_start_datetime = current_datetime.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    current_year_end_datetime = current_year_start_datetime + relativedelta(years=+1)
    current_year_start_datetime = pd.to_datetime(current_year_start_datetime)
    current_year_end_datetime = pd.to_datetime(current_year_end_datetime)

    current_date = current_datetime.strftime('%Y/%m/%d')
    current_month = current_datetime.strftime('%Y/%m')
    current_year = current_datetime.strftime('%Y')

    buy_volume_all = 0
    sell_volume_all = 0
    if not child_orders['short'].empty:
        df_buy_volume = child_orders['short'].loc[child_orders['short']['side'] == 'BUY', 'volume']
        df_sell_volume = child_orders['short'].loc[child_orders['short']['side'] == 'SELL', 'volume']
        if not df_buy_volume.empty:
            buy_volume_all += float(df_buy_volume.sum())
        if not df_sell_volume.empty:
            sell_volume_all += float(df_sell_volume.sum())

    if not child_orders['long'].empty:
        df_buy_volume = child_orders['long']['volume']
        if not df_buy_volume.empty:
            buy_volume_all += float(df_buy_volume.sum())

    if path_exists(p_daily_volume_path):
        df_daily_volume = read_csv(str(p_daily_volume_path))
        df_daily_volume = df_daily_volume.set_index('date')

        buy_volume = buy_volume_all
        sell_volume = sell_volume_all
        if len(df_daily_volume) >= 2:
            buy_volume -= float(df_daily_volume.loc[df_daily_volume.index != current_date, f'{product_code}_buy_volume'].sum())
            sell_volume -= float(df_daily_volume.loc[df_daily_volume.index != current_date, f'{product_code}_sell_volume'].sum())

        buy_volume = round(buy_volume, 1)
        sell_volume = round(sell_volume, 1)

        df_daily_volume.at[current_date, f'{product_code}_total_volume'] = buy_volume + sell_volume
        df_daily_volume.at[current_date, f'{product_code}_buy_volume'] = buy_volume
        df_daily_volume.at[current_date, f'{product_code}_sell_volume'] = sell_volume

        buy_volume_list = []
        sell_volume_list = []
        total_volume_list = []
        for col_num in df_daily_volume.columns.tolist():
            if col_num.endswith('_buy_volume'):
                buy_volume_list.append(col_num)
            elif col_num.endswith('_sell_volume'):
                sell_volume_list.append(col_num)
            elif col_num.endswith('_total_volume'):
                total_volume_list.append(col_num)

        df_daily_volume = df_daily_volume.fillna(0)
        total_volume_sum = df_daily_volume.loc[current_date, total_volume_list].values.sum()
        buy_volume_sum = df_daily_volume.loc[current_date, buy_volume_list].values.sum()
        sell_volume_sum = df_daily_volume.loc[current_date, sell_volume_list].values.sum()
        df_daily_volume.at[current_date, 'total_volume'] = round(total_volume_sum, 1)
        df_daily_volume.at[current_date, 'buy_volume'] = round(buy_volume_sum, 1)
        df_daily_volume.at[current_date, 'sell_volume'] = round(sell_volume_sum, 1)

        df_to_csv(str(p_daily_volume_path), df_daily_volume, index=True)
    else:
        buy_volume_all = round(buy_volume_all, 1)
        sell_volume_all = round(sell_volume_all, 1)

        daily_volume = [
            {
                'date': current_date,
                'total_volume': buy_volume_all + sell_volume_all,
                'buy_volume': buy_volume_all,
                'sell_volume': sell_volume_all,
                f'{product_code}_total_volume': buy_volume_all + sell_volume_all,
                f'{product_code}_buy_volume': buy_volume_all,
                f'{product_code}_sell_volume': sell_volume_all,
            },
        ]
        df_daily_volume = pd.DataFrame(daily_volume)
        df_daily_volume = df_daily_volume.set_index('date')
        df_to_csv(str(p_daily_volume_path), df_daily_volume, index=True)

    df_daily_volume.index = pd.to_datetime(df_daily_volume.index)
    df_daily_volume.index = df_daily_volume.index.tz_localize('Asia/Tokyo')

    df_daily_volume_current_month = df_daily_volume[current_month_start_datetime: current_month_end_datetime]
    df_daily_volume_current_month_sum = df_daily_volume_current_month.sum()

    if path_exists(p_monthly_volume_path):
        df_monthly_volume = read_csv(str(p_monthly_volume_path))
        df_monthly_volume = df_monthly_volume.set_index('date')

        if current_month in df_monthly_volume.index.tolist():
            current_month_sum_dict = df_daily_volume_current_month_sum.to_dict()
            for col_name, val in current_month_sum_dict.items():
                if col_name in df_monthly_volume.columns:
                    df_monthly_volume.at[current_month, col_name] = val
                else:
                    df_monthly_volume[col_name] = val

        else:
            current_month_sum_dict = df_daily_volume_current_month_sum.to_dict()
            current_month_volume = []
            for col_name in df_monthly_volume.columns.tolist():
                if col_name in current_month_sum_dict.keys():
                    current_month_volume.append(current_month_sum_dict[col_name])
                else:
                    current_month_volume.append(0)
            df_monthly_volume.loc[current_month] = current_month_volume

        df_to_csv(str(p_monthly_volume_path), df_monthly_volume, index=True)
    else:
        current_month_volume_dict = {'date': current_month}
        current_month_volume_dict.update(df_daily_volume_current_month_sum.to_dict())
        df_monthly_volume = pd.DataFrame([current_month_volume_dict])
        df_monthly_volume = df_monthly_volume.set_index('date')
        df_to_csv(str(p_monthly_volume_path), df_monthly_volume, index=True)

    df_monthly_volume.index = pd.to_datetime(df_monthly_volume.index)
    df_monthly_volume.index = df_monthly_volume.index.tz_localize('Asia/Tokyo')

    df_monthly_volume_current_year = df_monthly_volume[current_year_start_datetime: current_year_end_datetime]
    df_monthly_volume_current_year_sum = df_monthly_volume_current_year.sum()

    if path_exists(p_yearly_volume_path):
        df_yearly_volume = read_csv(str(p_yearly_volume_path))
        df_yearly_volume['date'] = df_yearly_volume['date'].astype(str)
        df_yearly_volume = df_yearly_volume.set_index('date')

        if current_year in df_yearly_volume.index.tolist():
            current_year_sum_dict = df_monthly_volume_current_year_sum.to_dict()
            for col_name, val in current_year_sum_dict.items():
                if col_name in df_yearly_volume.columns:
                    df_yearly_volume.at[current_year, col_name] = val
                else:
                    df_yearly_volume[col_name] = val

        else:
            current_year_sum_dict = df_monthly_volume_current_year_sum.to_dict()
            current_year_volume = []
            for col_name in df_yearly_volume.columns.tolist():
                if col_name in current_year_sum_dict.keys():
                    current_year_volume.append(current_year_sum_dict[col_name])
                else:
                    current_year_volume.append(0)
            df_yearly_volume.loc[current_year] = current_year_volume

        df_to_csv(str(p_yearly_volume_path), df_yearly_volume, index=True)
    else:
        current_year_volume_dict = {'date': current_year}
        current_year_volume_dict.update(df_monthly_volume_current_year_sum.to_dict())
        df_yearly_volume = pd.DataFrame([current_year_volume_dict])
        df_yearly_volume = df_yearly_volume.set_index('date')
        df_to_csv(str(p_yearly_volume_path), df_yearly_volume, index=True)


def trading(product_code):
    board_state = get_board_state(product_code)
    if board_state['state'] != 'RUNNING':
        logger.info(
            f'[{product_code} {board_state["state"]}] 現在取引所は稼働していません。')
        return

    current_datetime = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9)))

    logger.info(f'[{product_code}] 不必要な生データを削除中...')
    delete_row_data(
        product_code=product_code,
        current_datetime=current_datetime,
        days=7
    )
    logger.info(f'[{product_code}] 不必要な生データの削除完了')

    logger.info(f'[{product_code}] 取引情報更新中...')
    latest_summary = obtain_latest_summary(
        product_code=product_code
    )
    logger.info(f'[{product_code}] 取引情報更新完了')

    ai = AI(
        product_code=product_code,
        min_size=float(os.environ.get(f'{product_code}_MIN_SIZE', 0)),
        min_volume_short=float(os.environ.get(f'{product_code}_SHORT_MIN_VOLUME', 1000)),
        max_volume_short=float(os.environ.get(f'{product_code}_SHORT_MAX_VOLUME', 10000)),
        min_volume_long=float(os.environ.get(f'{product_code}_LONG_MIN_VOLUME', 10000)),
        max_volume_long=float(os.environ.get(f'{product_code}_LONG_MAX_VOLUME', 30000)),
        min_reward_rate=float(os.environ.get(f'{product_code}_MIN_REWARD_RATE', 0.01)),
        min_local_price_gap_rate=float(os.environ.get(f'{product_code}_MIN_LOCAL_PRICE_GAP_RATE', 0.03)),
        time_diff=9,
        latest_summary=latest_summary
    )

    logger.info(f'[{product_code}] 注文中...')
    if int(os.environ.get(f'{product_code}_LONG', 0)):
        ai.long_term()
    if int(os.environ.get(f'{product_code}_SHORT', 0)):
        ai.short_term()

    if int(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_MONTHLY', 0)) != 0:
        ai.dca(
            min_volume=float(os.environ.get(f'{product_code}_DCA_MIN_VOLUME_MONTHLY', 0)),
            max_volume=float(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_MONTHLY', 0)),
            st_buy_price_rate=float(os.environ.get(f'{product_code}_DCA_ST_BUY_PRICE_RATE', 1)),
            price_rate=float(os.environ.get(f'{product_code}_DCA_PRICE_RATE_MONTHLY', 1)),
            cycle='monthly'
        )
    if int(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_WEEKLY', 0)) != 0:
        ai.dca(
            min_volume=float(os.environ.get(f'{product_code}_DCA_MIN_VOLUME_WEEKLY', 0)),
            max_volume=float(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_WEEKLY', 0)),
            st_buy_price_rate=float(os.environ.get(f'{product_code}_DCA_ST_BUY_PRICE_RATE', 1)),
            price_rate=float(os.environ.get(f'{product_code}_DCA_PRICE_RATE_WEEKLY', 1)),
            cycle='weekly'
        )
    if int(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_DAILY', 0)) != 0:
        ai.dca(
            min_volume=float(os.environ.get(f'{product_code}_DCA_MIN_VOLUME_DAILY', 0)),
            max_volume=float(os.environ.get(f'{product_code}_DCA_MAX_VOLUME_DAILY', 0)),
            st_buy_price_rate=float(os.environ.get(f'{product_code}_DCA_ST_BUY_PRICE_RATE', 1)),
            price_rate=float(os.environ.get(f'{product_code}_DCA_PRICE_RATE_DAILY', 1)),
            cycle='daily'
        )

    logger.info(f'[{product_code}] 注文完了')

    ai.update_child_orders(term='long')
    ai.update_child_orders(term='short')
    ai.update_child_orders(term='dca')

    logger.info(f'[{product_code}] 利益集計中...')
    ai.update_unrealized_profit(term='long')
    ai.update_unrealized_profit(term='dca')
    calc_profit(product_code, ai.child_orders, latest_summary)
    logger.info(f'[{product_code}] 利益集計完了')

    logger.info(f'[{product_code}] 取引量集計中...')
    calc_volume(product_code, ai.child_orders)
    logger.info(f'[{product_code}] 取引量集計完了')


def lambda_handler(event, context):

    product_code_list = [
        'BTC_JPY',
        # 'ETH_JPY',
        # 'XLM_JPY',
        # 'XRP_JPY',
        # 'MONA_JPY',
    ]
    for product_code in product_code_list:
        trading(product_code=product_code)

        # load data
        # current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
        # before_15d_datetime = current_datetime - datetime.timedelta(days=15)
        # before_16d_datetime = current_datetime - datetime.timedelta(days=16)

        # get_executions_history(
        #     product_code=product_code,
        #     start_date=before_16d_datetime,
        #     end_date=before_15d_datetime,
        #     return_df=False
        # )
        # gen_execution_summaries(
        #     product_code=product_code,
        #     year=2022,
        #     month=3,
        # )

    # =============================================================

    # start_date = end_date - datetime.timedelta(days=1)
    # df = get_executions_history(
    # start_date=start_date, end_date=end_date, product_code='ETH_JPY',
    # count=500)

    # from bitflyer_api import get_balance
    # df_balance = get_balance()
    # print(df_balance)

    # # get_ticker()
    # p_balance_log_dir = Path(BALANCE_LOG_DIR)
    # p_balance_log_save_path = p_balance_log_dir.joinpath(
    #     current_datetime.strftime('%Y'), current_datetime.strftime('%m'),
    # )
    # df_balance.to_csv()
    # df_child_orders = get_child_orders(
    # region='Asia/Tokyo',
    # child_order_acceptance_id='JRF20210302-153421-352775')
    # df_child_orders.to_csv('child_orders/ETH_JPY/all.csv')
    # print(df_child_orders)
    # print(df_child_orders['child_order_date'])
    # send_child_order('ETH_JPY', 'LIMIT', 'BUY', price=75000, size=0.08)

    # gen summary
    # current_datetime = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9)))
    # for i in range(12):
    #     target_datetime = current_datetime - datetime.timedelta(days=i)
    #     gen_execution_summaries(
    #         product_code=product_code,
    #         year=int(target_datetime.strftime('%Y')),
    #         month=int(target_datetime.strftime('%m')),
    #         day=int(target_datetime.strftime('%d'))
    #     )


if __name__ == '__main__':
    lambda_handler('', '')
