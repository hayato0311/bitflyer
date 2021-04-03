import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from logging import basicConfig, StreamHandler, FileHandler, getLogger, Formatter, DEBUG, INFO

from bitflyer_api import *
from ai import *
from preprocess import *
from manage import REF_LOCAL, PROFIT_DIR, CHILD_ORDERS_DIR

from utils import path_exists, read_csv, df_to_csv


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


def calc_profit(product_code, child_orders, current_datetime, latest_summary):
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
    before_1d_datetime = current_datetime + relativedelta(days=-1)

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

    before_1d_date = before_1d_datetime.strftime('%Y/%m/%d')
    before_1d_month = before_1d_datetime.strftime('%Y/%m')
    before_1d_year = before_1d_datetime.strftime('%Y')

    if path_exists(p_daily_profit_path):
        df_daily_profit = read_csv(str(p_daily_profit_path))
        df_daily_profit = df_daily_profit.set_index('date')
        rearlized_profit = 0
        unrealized_profit = 0
        if child_orders['short'].empty:
            if not child_orders['long'].empty:
                if before_1d_date in df_daily_profit.index.tolist() and f'{product_code}_unrealized' in df_daily_profit.columns.tolist():
                    unrealized_profit = child_orders['long']['cumsum_profit'].values[-1] - df_daily_profit[before_1d_date, f'{product_code}_unrealized']
                else:
                    unrealized_profit = child_orders['long']['cumsum_profit'].values[-1]
        else:
            if before_1d_date in df_daily_profit.index.tolist() and f'{product_code}_realized' in df_daily_profit.columns.tolist():
                rearlized_profit = child_orders['short']['cumsum_profit'].max() - df_daily_profit[before_1d_date, f'{product_code}_realized']
            else:
                rearlized_profit = child_orders['short']['cumsum_profit'].max()

            if not child_orders['long'].empty:
                if before_1d_date in df_daily_profit.index.tolist() and f'{product_code}_unrealized' in df_daily_profit.columns.tolist():
                    unrealized_profit = child_orders['long']['cumsum_profit'].values[-1] - df_daily_profit[before_1d_date, f'{product_code}_unrealized']
                else:
                    unrealized_profit = child_orders['long']['cumsum_profit'].values[-1]

                df_active_sell_order = child_orders['short'].query('side == "SELL" and child_order_state == "ACTIVE"')
                if not df_active_sell_order.empty:
                    child_order_acceptance_id_list = df_active_sell_order['related_child_order_acceptance_id'].values.tolist()
                    for child_order_acceptance_id in child_order_acceptance_id_list:
                        unrealized_profit += (latest_summary['SELL']['now']['price'] - child_orders['short'][child_order_acceptance_id, 'price']) \
                            * child_orders['short'][child_order_acceptance_id, 'size'] \
                            - child_orders['short'][child_order_acceptance_id, 'total_commission_yen']

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
        unrealized_profit_sum = df_daily_profit.loc[current_date, unrealized_profit_list].values.sum()
        realized_profit_sum = df_daily_profit.loc[current_date, realized_profit_list].values.sum()
        total_profit_sum = df_daily_profit.loc[current_date, total_profit_list].values.sum()
        df_daily_profit.at[current_date, 'total_profit'] = total_profit_sum
        df_daily_profit.at[current_date, 'realized_profit'] = realized_profit_sum
        df_daily_profit.at[current_date, 'unrealized_profit'] = unrealized_profit_sum

        df_to_csv(str(p_daily_profit_path), df_daily_profit, index=True)
    else:
        rearlized_profit = 0
        unrealized_profit = 0
        if child_orders['short'].empty:
            if not child_orders['long'].empty:
                unrealized_profit = child_orders['long']['cumsum_profit'].values[-1]
        else:
            rearlized_profit = child_orders['short']['cumsum_profit'].max()
            if not child_orders['long'].empty:
                unrealized_profit = child_orders['long']['cumsum_profit'].values[-1]

                df_active_sell_order = child_orders['short'].query('side == "SELL" and child_order_state == "ACTIVE"')
                if not df_active_sell_order.empty:
                    child_order_acceptance_id_list = df_active_sell_order['related_child_order_acceptance_id'].values.tolist()
                    for child_order_acceptance_id in child_order_acceptance_id_list:
                        unrealized_profit += (latest_summary['SELL']['now']['price'] - child_orders['short'][child_order_acceptance_id, 'price']) \
                            * child_orders['short'][child_order_acceptance_id, 'size'] \
                            - child_orders['short'][child_order_acceptance_id, 'total_commission_yen']
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
                if col_name in current_year_sum_dict.keys():
                    current_month_profit.append(current_year_sum_dict[col_name])
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
        min_size_short=float(os.environ.get(f'{product_code}_SHORT_MIN_SIZE', 0.001)),
        min_size_long=float(os.environ.get(f'{product_code}_LONG_MIN_SIZE', 0.001)),
        time_diff=9,
        latest_summary=latest_summary
    )

    if int(os.environ.get(product_code, 0)):
        logger.info(f'[{product_code}] 注文中...')
        if int(os.environ.get(f'{product_code}_LONG', 0)):
            ai.long_term()
        if int(os.environ.get(f'{product_code}_SHORT', 0)):
            ai.short_term()
        logger.info(f'[{product_code}] 注文完了')

    calc_profit(product_code, ai.child_orders, current_datetime, latest_summary)


def lambda_handler(event, context):

    product_code_list = [
        'BTC_JPY',
        'ETH_JPY',
        'XLM_JPY',
        'XRP_JPY',
        'MONA_JPY',
    ]
    for product_code in product_code_list:
        trading(product_code=product_code)

    # =============================================================

    # start_date = end_date - datetime.timedelta(days=1)
    # df = get_executions_history(
    # start_date=start_date, end_date=end_date, product_code='ETH_JPY',
    # count=500)

    # # get_ticker()
    # df_balance = get_balance()
    # p_balance_log_dir = Path(BALANCE_LOG_DIR)
    # p_balance_log_save_path = p_balance_log_dir.joinpath(
    #     current_datetime.strftime('%Y'), current_datetime.strftime('%m'),
    # )
    # df_balance.to_csv()
    # print(df_balance)

    # df_child_orders = get_child_orders(
    # region='Asia/Tokyo',
    # child_order_acceptance_id='JRF20210302-153421-352775')

    # df_child_orders.to_csv('child_orders/ETH_JPY/all.csv')
    # print(df_child_orders)
    # print(df_child_orders['child_order_date'])

    # send_child_order('ETH_JPY', 'LIMIT', 'BUY', price=75000, size=0.08)


if __name__ == '__main__':
    lambda_handler('', '')
