import datetime
import pandas as pd

from bitflyer_api import *
from ai import *
from preprocess import *


def main():
    current_datetime = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9)))

    latest_summary = obtain_latest_summary(product_code='ETH_JPY', daily=False)

    gen_execution_summaries(year=current_datetime.strftime('%Y'), month=-1, day=-1,
                            product_code='ETH_JPY')

    ai = AI(product_code='ETH_JPY', small_size=0.01,
            middle_size=0.1, large_size=0.3, time_diff=9, latest_summary=latest_summary)
    ai.long_term()
    ai.short_term()

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
    main()
