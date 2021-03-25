import datetime
import pandas as pd
from logging import basicConfig, StreamHandler, FileHandler, getLogger, Formatter, DEBUG, INFO

from bitflyer_api import *
from ai import *
from preprocess import *
from manage import REF_LOCAL


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


def lambda_handler(event, context):

    # =============================================================
    # BTC_JPY
    # =============================================================
    product_code = 'BTC_JPY'
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

    if int(os.environ.get(product_code, 0)):
        logger.info(f'[{product_code}] 注文中...')
        ai = AI(
            product_code=product_code,
            min_size_short=float(os.environ.get('MIN_SIZE_SHORT_BTC', 0.001)),
            min_size_long=float(os.environ.get('MIN_SIZE_LONG_BTC', 0.001)),
            time_diff=9,
            latest_summary=latest_summary
        )
        ai.long_term()
        ai.short_term()
        logger.info(f'[{product_code}] 注文完了')

    # =============================================================
    # ETH_JPY
    # =============================================================
    product_code = 'ETH_JPY'
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
    if int(os.environ.get(product_code, 0)):

        logger.info(f'[{product_code}] 注文中...')
        ai = AI(
            product_code=product_code,
            min_size_short=float(os.environ.get('MIN_SIZE_SHORT_ETH', 0.01)),
            min_size_long=float(os.environ.get('MIN_SIZE_LONG_ETH', 0.01)),
            time_diff=9,
            latest_summary=latest_summary
        )
        ai.long_term()
        ai.short_term()
        logger.info(f'[{product_code}] 注文完了')

    # =============================================================

    # start_date = end_date - datetime.timedelta(days=1)
    # df = get_executions_history(
    #     start_date=start_date, end_date=end_date, product_code='ETH_JPY', count=500)

    # # get_ticker()
    # df_balance = get_balance()
    # p_balance_log_dir = Path(BALANCE_LOG_DIR)
    # p_balance_log_save_path = p_balance_log_dir.joinpath(
    #     current_datetime.strftime('%Y'), current_datetime.strftime('%m'),
    # )
    # df_balance.to_csv()
    # print(df_balance)

    # df_child_orders = get_child_orders(
    #     region='Asia/Tokyo', child_order_acceptance_id='JRF20210302-153421-352775')

    # df_child_orders.to_csv('child_orders/ETH_JPY/all.csv')
    # print(df_child_orders)
    # print(df_child_orders['child_order_date'])

    # send_child_order('ETH_JPY', 'LIMIT', 'BUY', price=75000, size=0.08)


if __name__ == '__main__':
    lambda_handler('', '')
