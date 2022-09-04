import datetime
import time
from logging import getLogger
from pathlib import Path

import pandas as pd
from dateutil.relativedelta import relativedelta

from bitflyer_api import get_executions
from manage import EXECUTION_HISTORY_DIR, REF_LOCAL
from utils import df_to_csv, path_exists, read_csv

logger = getLogger(__name__)

if not REF_LOCAL:
    from aws import S3
    s3 = S3()


def get_executions_history(
        product_code,
        start_date,
        end_date,
        region='Asia/Tokyo',
        count=500,
        return_df=False):
    logger.debug(
        f'[{start_date} - {end_date}] 取引履歴ダウンロード中...')

    p_save_base_dir = Path(EXECUTION_HISTORY_DIR)

    start_date_tmp = start_date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )
    end_date_tmp = end_date.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )

    loop_start_time = time.time()
    day_count = 0

    if return_df:
        df_history = pd.DataFrame()

    while start_date_tmp < end_date_tmp:
        day_count += 1
        target_date_start = end_date_tmp
        target_date_end = end_date_tmp + datetime.timedelta(days=1)
        logger.debug(target_date_start)

        p_save_dir = p_save_base_dir.joinpath(
            product_code,
            end_date_tmp.strftime('%Y'),
            end_date_tmp.strftime('%m'),
            end_date_tmp.strftime('%d'))

        p_save_dir_row = p_save_dir.joinpath('row')
        p_save_dir_1h = p_save_dir.joinpath('1h')
        p_save_dir_1m = p_save_dir.joinpath('1m')
        p_save_dir_10m = p_save_dir.joinpath('10m')

        p_save_path_row_all = p_save_dir_row.joinpath('all.csv')
        p_save_path_row_buy = p_save_dir_row.joinpath('buy.csv')
        p_save_path_row_sell = p_save_dir_row.joinpath('sell.csv')

        if REF_LOCAL:
            if not p_save_dir_row.exists():
                p_save_dir_row.mkdir(parents=True)
            if not p_save_dir_1h.exists():
                p_save_dir_1h.mkdir(parents=True)
            if not p_save_dir_1m.exists():
                p_save_dir_1m.mkdir(parents=True)
            if not p_save_dir_10m.exists():
                p_save_dir_10m.mkdir(parents=True)

        before = 0
        after = 0
        if path_exists(p_save_path_row_all):
            df = read_csv(str(p_save_path_row_all))
            df['exec_date'] = pd.to_datetime(df['exec_date'])
            df = df.set_index('exec_date')
            df = df.tz_convert(region)
            before = int(df.head(1)['id'])
            after = int(df.tail(1)['id'])
        else:
            df = get_executions(product_code, count,
                                before=before, region=region)
            if df.empty:
                df = get_executions(product_code, count,
                                    after=after, region=region)
            df = df.sort_index()
            before = int(df.head(1)['id'])
            after = int(df.tail(1)['id'])

        while df.tail(1).index[0] < target_date_end:
            df_new = get_executions(product_code, count, after=after)
            if df_new.empty:
                break
            df_new = df_new.tz_convert(region)
            df = pd.concat([df, df_new])
            df = df.sort_index()
            after = int(df.tail(1)['id'])
            if REF_LOCAL:
                time.sleep(0.25)

        while target_date_start < df.head(1).index[0]:
            df_new = get_executions(product_code, count, before=before)
            if df_new.empty:
                break
            df_new = df_new.tz_convert(region)
            df = pd.concat([df, df_new])
            df = df.sort_index()
            before = int(df.head(1)['id'])
            if REF_LOCAL:
                time.sleep(0.25)

        while df.tail(1).index[0] < target_date_end:
            df_new = get_executions(product_code, count, after=after)
            if df_new.empty:
                break
            df_new = df_new.tz_convert(region)
            df = pd.concat([df, df_new])
            df = df.sort_index()
            after = int(df.tail(1)['id'])
            if REF_LOCAL:
                time.sleep(0.25)

        df = df.query('@target_date_start <= index < @target_date_end')

        if return_df:
            if df_history.empty:
                df_history = df.copy()
            else:
                df_history = pd.concat([df_history, df])

        if not df.empty:
            df_buy = df.query('side == "BUY"')
            df_sell = df.query('side == "SELL"')

            # if REF_LOCAL:
            #     df.to_csv(str(p_save_path_row_all))
            #     df_buy.to_csv(str(p_save_path_row_buy))
            #     df_sell.to_csv(str(p_save_path_row_sell))
            # else:
            logger.debug(f'[{target_date_start}] 取引履歴データ保存中...')
            df_to_csv(str(p_save_path_row_all), df, index=True)
            df_to_csv(str(p_save_path_row_buy), df_buy, index=True)
            df_to_csv(str(p_save_path_row_sell), df_sell, index=True)
            logger.debug(f'[{target_date_start}] 取引履歴データ保存完了')
            # s3.to_csv(
            #     str(p_save_path_row_all),
            #     df=df
            # )
            # s3.to_csv(
            #     str(p_save_path_row_buy),
            #     df=df_buy
            # )
            # s3.to_csv(
            #     str(p_save_path_row_sell),
            #     df=df_sell
            # )

            p_save_dir_1h = p_save_dir.joinpath('1h')
            p_save_dir_1m = p_save_dir.joinpath('1m')
            p_save_dir_10m = p_save_dir.joinpath('10m')

            df_buy_resample = df_buy[['price', 'size']]
            df_sell_resample = df_sell[['price', 'size']]

            logger.debug(f'[{target_date_start}] リサンプリング中...')

            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_1h, 'H')
            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_1m, 'T')
            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_10m, '10T')
            logger.debug(f'[{target_date_start}] リサンプリング完了')

            logger.debug(f'[{target_date_start}] 取引履歴ダウンロード完了')
        if day_count == 5:
            process_time = datetime.timedelta(
                seconds=time.time() - loop_start_time)
            wait_time = datetime.timedelta(
                minutes=5) - process_time
            logger.debug('waiting...')
            if wait_time.total_seconds() > 0:
                time.sleep(wait_time.total_seconds())
            day_count = 0
            loop_start_time = time.time()

        end_date_tmp -= datetime.timedelta(days=1)

    logger.debug(f'[{start_date} - {end_date}] 取引履歴ダウンロード完了')

    if return_df:
        return df_history


def resampling(df_buy, df_sell, p_save_dir='', freq='T'):

    df_buy_price = df_buy[['price']]
    df_buy_size = df_buy[['size']]
    df_buy_price_ohlc = df_buy_price.resample(freq).ohlc()
    df_buy_price_ohlc.columns = [
        f'{col_name[1]}_{col_name[0]}' for col_name in df_buy_price_ohlc.columns.tolist()]
    df_buy_price_ohlc.interpolate()
    df_buy_price_ohlc.dropna(how='any')

    df_buy_size = df_buy_size.resample(freq).sum()
    df_buy_size.columns = ['total_size']
    df_buy_resampled = pd.concat([df_buy_price_ohlc, df_buy_size], axis=1)
    if not p_save_dir == '':
        # if REF_LOCAL:
        #     df_buy_resampled.to_csv(str(p_save_dir.joinpath('buy.csv')))
        # else:
        # s3.to_csv(
        #     str(p_save_dir.joinpath('buy.csv')),
        #     df=df_buy_resampled
        # )
        df_to_csv(str(p_save_dir.joinpath('buy.csv')), df_buy_resampled, index=True)

    df_sell_price = df_sell[['price']]
    df_sell_size = df_sell[['size']]

    df_sell_price_ohlc = df_sell_price.resample(freq).ohlc()
    df_sell_price_ohlc.columns = [
        f'{col_name[1]}_{col_name[0]}' for col_name in df_sell_price_ohlc.columns.tolist()]
    df_sell_size = df_sell_size.resample(freq).sum()
    df_sell_size.columns = ['total_size']
    df_sell_resampled = pd.concat([df_sell_price_ohlc, df_sell_size], axis=1)
    if not p_save_dir == '':
        # if REF_LOCAL:
        #     df_sell_resampled.to_csv(str(p_save_dir.joinpath('sell.csv')))
        # else:
        #     s3.to_csv(
        #         str(p_save_dir.joinpath('sell.csv')),
        #         df=df_sell_resampled
        #     )
        df_to_csv(str(p_save_dir.joinpath('sell.csv')), df_sell_resampled, index=True)
    return df_buy_resampled, df_sell_resampled


def make_summary_from_scratch(p_dir):
    logger.debug(f'[{p_dir}] 集計データ作成中...')
    p_buy_path = p_dir.joinpath('buy.csv')
    p_sell_path = p_dir.joinpath('sell.csv')
    p_summary_path = p_dir.parent.joinpath('summary.csv')

    df_buy = pd.DataFrame()
    df_sell = pd.DataFrame()

    # if REF_LOCAL:
    #     if p_buy_path.exists() and p_sell_path.exists():
    #         df_buy = pd.read_csv(str(p_buy_path))
    #         df_sell = pd.read_csv(str(p_sell_path))
    # else:
    if path_exists(p_buy_path) and path_exists(p_sell_path):
        df_buy = read_csv(str(p_buy_path))
        df_sell = read_csv(str(p_sell_path))

    if df_buy.empty or df_sell.empty:
        logger.debug(f'[{p_dir}] データが存在しなかったため集計データ作成を中断します。')
        return pd.DataFrame()

    df_summary = pd.DataFrame(
        [
            {
                'CATEGORY': 'open_price',
                'BUY': float(df_buy['open_price'].values[0]),
                'SELL': float(df_sell['open_price'].values[0])
            },
            {
                'CATEGORY': 'high_price',
                'BUY': int(df_buy['high_price'].max()),
                'SELL': int(df_sell['high_price'].max())
            },
            {
                'CATEGORY': 'low_price',
                'BUY': int(df_buy['low_price'].min()),
                'SELL': int(df_sell['low_price'].min())
            },
            {
                'CATEGORY': 'close_price',
                'BUY': float(df_buy['close_price'].values[-1]),
                'SELL': float(df_sell['close_price'].values[-1])
            },
            {
                'CATEGORY': 'total_size',
                'BUY': float(df_buy['total_size'].sum()),
                'SELL': float(df_sell['total_size'].sum())
            }
        ]
    )

    # if REF_LOCAL:
    #     df_summary.to_csv(str(p_summary_path), index=False)
    # else:
    #     s3.to_csv(
    #         str(p_summary_path),
    #         df=df_summary,
    #         index=False
    #     )
    df_to_csv(str(p_summary_path), df_summary, index=False)
    logger.debug(f'[{p_dir}] 集計データ作成完了')
    return df_summary


def make_summary_from_csv(
        product_code,
        p_dir='',
        summary_path_list=[],
        save=True):
    if p_dir != '':
        logger.debug(f'[{p_dir}] 集計データ更新中...')
        p_summary_save_path = p_dir.joinpath('summary.csv')
    else:
        if len(summary_path_list) == 0:
            logger.debug('対象となる集計データが存在しないため更新を終了します。')
            return
        elif len(summary_path_list) == 1:
            logger.debug(
                f'[{summary_path_list[0]}] 集計データ更新中...'
            )
        else:
            summary_path_list = sorted(summary_path_list)
            logger.debug(
                f'[{summary_path_list[0]} - {summary_path_list[-1]}] 集計データ更新中...'
            )
    df_summary = pd.DataFrame()
    if p_dir != '':
        # if REF_LOCAL:
        #     if p_summary_save_path.exists():
        #         df_summary = s3.read_csv(str(p_summary_save_path))
        #         df_summary = df_summary.set_index('CATEGORY', drop=True)
        # else:
        #     if s3.key_exists(str(p_summary_save_path)):
        #         df_summary = s3.read_csv(str(p_summary_save_path))
        #         df_summary = df_summary.set_index('CATEGORY', drop=True)
        if path_exists(p_summary_save_path):
            df_summary = read_csv(str(p_summary_save_path))
            df_summary = df_summary.set_index('CATEGORY', drop=True)
    if summary_path_list == [] and p_dir != '':
        if REF_LOCAL:
            p_summary_path_list = p_dir.glob('*/summary.csv')
            p_summary_path_list = sorted(p_summary_path_list)
            if len(p_summary_path_list) == 1:
                summary_path_list = [
                    str(p_summary_path_list[0])
                ]
            else:
                summary_path_list = [
                    str(p_summary_path_list[-2]), str(p_summary_path_list[-1])
                ]
        else:
            day_dir_list = s3.listdir(str(p_dir))
            if len(day_dir_list) <= 2:
                target_day_dir_list = day_dir_list
            else:
                target_day_dir_list = day_dir_list[-2:]
            for day_dir in target_day_dir_list:
                summary_path_tmp = day_dir + 'summary.csv'
                if path_exists(summary_path_tmp):
                    summary_path_list.append(summary_path_tmp)

    for summary_path in summary_path_list:
        if product_code not in summary_path:
            logger.warning(
                f'[{summary_path}] 対象のproduct_codeとは違うパスが含まれているため、読み込み対象外にします。')
            continue

        df_summary_child = read_csv(summary_path)
        df_summary_child = df_summary_child.set_index('CATEGORY', drop=True)
        if df_summary.empty:
            df_summary = df_summary_child.copy()
        else:
            if len(summary_path_list) == 1:
                df_summary.at['open_price',
                              'BUY'] = df_summary_child.at['open_price', 'BUY']
                df_summary.at['open_price',
                              'SELL'] = df_summary_child.at['open_price',
                                                            'SELL']

            if df_summary.at['high_price',
                             'BUY'] < df_summary_child.at['high_price',
                                                          'BUY']:
                df_summary.at['high_price',
                              'BUY'] = df_summary_child.at['high_price', 'BUY']
            if df_summary.at['high_price',
                             'SELL'] < df_summary_child.at['high_price',
                                                           'SELL']:
                df_summary.at['high_price',
                              'SELL'] = df_summary_child.at['high_price',
                                                            'SELL']

            if df_summary.at['low_price',
                             'BUY'] > df_summary_child.at['low_price',
                                                          'BUY']:
                df_summary.at['low_price',
                              'BUY'] = df_summary_child.at['low_price', 'BUY']
            if df_summary.at['low_price',
                             'SELL'] > df_summary_child.at['low_price',
                                                           'SELL']:
                df_summary.at['low_price',
                              'SELL'] = df_summary_child.at['low_price',
                                                            'SELL']

            df_summary.at['close_price',
                          'BUY'] = df_summary_child.at['close_price', 'BUY']
            df_summary.at['close_price',
                          'SELL'] = df_summary_child.at['close_price', 'SELL']

    if save and p_dir != '':
        # if REF_LOCAL:
        #     df_summary.to_csv(str(p_summary_save_path))
        # else:
        #     s3.to_csv(
        #         str(p_summary_save_path),
        #         df=df_summary,
        #     )
        df_to_csv(str(p_summary_save_path), df_summary)

    if p_dir != '':
        logger.debug(f'[{p_dir}] 集計データ更新完了')
    else:
        if len(summary_path_list) == 1:
            logger.debug(
                f'[{summary_path_list[0]}] 集計データ更新完了'
            )
        else:
            logger.debug(
                f'[{summary_path_list[0]} - {summary_path_list[-1]}] 集計データ更新完了'
            )
    return df_summary


def make_summary(product_code, p_dir, daily=False):
    if daily:
        p_1m_dir = p_dir.joinpath('1m')
        df_summary = make_summary_from_scratch(p_1m_dir)
    else:
        df_summary = make_summary_from_csv(
            product_code=product_code,
            p_dir=p_dir,
            summary_path_list=[],
            save=True
        )
    return not df_summary.empty


# def estimate_trends(latest_summary):
#     return latest_summary


def obtain_latest_summary(product_code):
    logger.debug(f'[{product_code}] AI用集計データ取得中...')
    p_exe_history_dir = Path(EXECUTION_HISTORY_DIR)
    p_all_summary_path = p_exe_history_dir.joinpath(
        product_code, 'summary.csv')

    # daily summary
    current_datetime = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9)))

    before_1d_datetime = current_datetime - datetime.timedelta(days=1)
    before_32d_datetime = current_datetime - datetime.timedelta(days=32)
    before_1m_datetime = current_datetime + relativedelta(months=-1)
    before_1y_datetime = current_datetime + relativedelta(years=-1)

    df = get_executions_history(
        product_code=product_code,
        start_date=before_1d_datetime,
        end_date=current_datetime,
        return_df=True
    )

    df_buy = df.query('side == "BUY"')
    df_sell = df.query('side == "SELL"')

    df_buy_resampled, df_sell_resampled = resampling(df_buy[['price', 'size']], df_sell[['price', 'size']], freq='S')

    # before_1h_datetime = current_datetime - datetime.timedelta(hours=1)
    # df_buy_1h = df_buy_resampled.query('index > @before_1h_datetime')
    # df_sell_1h = df_sell_resampled.query('index > @before_1h_datetime')

    before_6h_datetime = current_datetime - datetime.timedelta(hours=6)
    df_buy_6h = df_buy_resampled.query('index > @before_6h_datetime')
    df_sell_6h = df_sell_resampled.query('index > @before_6h_datetime')

    df_buy_1d = df_buy_resampled.query('index > @before_1d_datetime')
    df_sell_1d = df_sell_resampled.query('index > @before_1d_datetime')

    gen_execution_summaries(
        product_code=product_code,
        year=int(before_1d_datetime.strftime('%Y')),
        month=int(before_1d_datetime.strftime('%m')),
        day=int(before_1d_datetime.strftime('%d'))
    )

    gen_execution_summaries(
        product_code=product_code,
        year=int(current_datetime.strftime('%Y')),
        month=int(current_datetime.strftime('%m')),
        day=int(current_datetime.strftime('%d'))
    )

    # monthly summary
    p_target_monthly_summary_path_list = [
        p_exe_history_dir.joinpath(
            product_code,
            current_datetime.strftime('%Y'),
            current_datetime.strftime('%m'),
            'summary.csv'
        ),
        p_exe_history_dir.joinpath(
            product_code,
            before_32d_datetime.strftime('%Y'),
            before_32d_datetime.strftime('%m'),
            'summary.csv'
        )
    ]

    target_monthly_summary_path_list = []
    for p_target_monthly_summary_path in p_target_monthly_summary_path_list:
        if path_exists(p_target_monthly_summary_path):
            target_monthly_summary_path_list.append(
                str(p_target_monthly_summary_path)
            )
    df_monthly_summary = make_summary_from_csv(
        product_code=product_code,
        p_dir='',
        summary_path_list=target_monthly_summary_path_list,
        save=False
    )

    # weekly summary
    target_weekly_summary_path_list = []
    for i in range(8):
        target_datetime = current_datetime - datetime.timedelta(days=i)
        p_target_weekly_summary_path = p_exe_history_dir.joinpath(
            product_code,
            target_datetime.strftime('%Y'),
            target_datetime.strftime('%m'),
            target_datetime.strftime('%d'),
            'summary.csv'
        )
        if path_exists(p_target_weekly_summary_path):
            target_weekly_summary_path_list.append(
                str(p_target_weekly_summary_path)
            )
    df_weekly_summary = make_summary_from_csv(
        product_code=product_code,
        p_dir='',
        summary_path_list=target_weekly_summary_path_list,
        save=False
    )

    # yearly summary
    target_yearly_summary_path_list = []
    for i in range(13):
        target_datetime = current_datetime + relativedelta(months=-i)
        p_target_yearly_summary_path = p_exe_history_dir.joinpath(
            product_code,
            target_datetime.strftime('%Y'),
            target_datetime.strftime('%m'),
            'summary.csv'
        )
        if path_exists(p_target_yearly_summary_path):
            target_yearly_summary_path_list.append(
                str(p_target_yearly_summary_path)
            )

    df_yearly_summary = make_summary_from_csv(
        product_code=product_code,
        p_dir='',
        summary_path_list=target_yearly_summary_path_list,
        save=False
    )

    df_all_summary = read_csv(str(p_all_summary_path))

    df_all_summary = df_all_summary.set_index('CATEGORY', drop=True)

    latest_summary = {
        'BUY': {
            'now': {
                'price': df_buy_1d['close_price'].values[-1],
            },
            '6h': {
                'price': {
                    'open': df_buy_6h['open_price'].values[0],
                    'high': df_buy_6h['high_price'].max(),
                    'low': df_buy_6h['low_price'].min(),
                    'close': df_buy_6h['close_price'].values[-1],
                },
                'trend': 'DOWN',
            },
            '1d': {
                'price': {
                    'open': df_buy_1d['open_price'].values[0],
                    'high': df_buy_1d['high_price'].max(),
                    'low': df_buy_1d['low_price'].min(),
                    'close': df_buy_1d['close_price'].values[-1],
                },
                'trend': 'DOWN',
            },
            '1w': {
                'price': {
                    'open': df_weekly_summary.at['open_price', 'BUY'],
                    'high': df_weekly_summary.at['high_price', 'BUY'],
                    'low': df_weekly_summary.at['low_price', 'BUY'],
                    'close': df_weekly_summary.at['close_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            '1m': {
                'price': {
                    'open': df_monthly_summary.at['open_price', 'BUY'],
                    'high': df_monthly_summary.at['high_price', 'BUY'],
                    'low': df_monthly_summary.at['low_price', 'BUY'],
                    'close': df_monthly_summary.at['close_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            '1y': {
                'price': {
                    'open': df_yearly_summary.at['open_price', 'BUY'],
                    'high': df_yearly_summary.at['high_price', 'BUY'],
                    'low': df_yearly_summary.at['low_price', 'BUY'],
                    'close': df_yearly_summary.at['close_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            'all': {
                'price': {
                    'open': df_all_summary.at['open_price', 'BUY'],
                    'high': df_all_summary.at['high_price', 'BUY'],
                    'low': df_all_summary.at['low_price', 'BUY'],
                    'close': df_all_summary.at['close_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
        },
        'SELL': {
            'now': {
                'price': df_sell_1d['close_price'].values[-1],
            },
            '6h': {
                'price': {
                    'open': df_sell_6h['open_price'].values[0],
                    'high': df_sell_6h['high_price'].max(),
                    'low': df_sell_6h['low_price'].min(),
                    'close': df_sell_6h['close_price'].values[-1],
                },
                'trend': 'DOWN',
            },
            '1d': {
                'price': {
                    'open': df_sell_1d['open_price'].values[0],
                    'high': df_sell_1d['high_price'].max(),
                    'low': df_sell_1d['low_price'].min(),
                    'close': df_sell_1d['close_price'].values[-1],
                },
                'trend': 'DOWN',
            },
            '1w': {
                'price': {
                    'open': df_weekly_summary.at['open_price', 'SELL'],
                    'high': df_weekly_summary.at['high_price', 'SELL'],
                    'low': df_weekly_summary.at['low_price', 'SELL'],
                    'close': df_weekly_summary.at['close_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
            '1m': {
                'price': {
                    'open': df_monthly_summary.at['open_price', 'SELL'],
                    'high': df_monthly_summary.at['high_price', 'SELL'],
                    'low': df_monthly_summary.at['low_price', 'SELL'],
                    'close': df_monthly_summary.at['close_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
            '1y': {
                'price': {
                    'open': df_yearly_summary.at['open_price', 'SELL'],
                    'high': df_yearly_summary.at['high_price', 'SELL'],
                    'low': df_yearly_summary.at['low_price', 'SELL'],
                    'close': df_yearly_summary.at['close_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
            'all': {
                'price': {
                    'open': df_all_summary.at['open_price', 'SELL'],
                    'high': df_all_summary.at['high_price', 'SELL'],
                    'low': df_all_summary.at['low_price', 'SELL'],
                    'close': df_all_summary.at['close_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
        }
    }

    # load summaries
    p_yesterday_summary_path = p_exe_history_dir.joinpath(
        product_code,
        before_1d_datetime.strftime('%Y'),
        before_1d_datetime.strftime('%m'),
        before_1d_datetime.strftime('%d'),
        'summary.csv'
    )
    p_last_month_summary_path = p_exe_history_dir.joinpath(
        product_code,
        before_1m_datetime.strftime('%Y'),
        before_1m_datetime.strftime('%m'),
        'summary.csv'
    )
    p_last_year_summary_path = p_exe_history_dir.joinpath(
        product_code,
        before_1y_datetime.strftime('%Y'),
        'summary.csv'
    )
    df_yesterday_summary = pd.DataFrame()
    df_last_month_summary = pd.DataFrame()
    df_last_year_summary = pd.DataFrame()

    if path_exists(p_yesterday_summary_path):
        df_yesterday_summary = read_csv(str(p_yesterday_summary_path))
        df_yesterday_summary = df_yesterday_summary.set_index('CATEGORY')

    if path_exists(p_last_month_summary_path):
        df_last_month_summary = read_csv(str(p_last_month_summary_path))
        df_last_month_summary = df_last_month_summary.set_index('CATEGORY')

    if path_exists(p_last_year_summary_path):
        df_last_year_summary = read_csv(str(p_last_year_summary_path))
        df_last_year_summary = df_last_year_summary.set_index('CATEGORY')

    if not df_yesterday_summary.empty:
        latest_summary['BUY']['yesterday'] = {
            'open': df_yesterday_summary.at['open_price', 'BUY'],
            'high': df_yesterday_summary.at['high_price', 'BUY'],
            'low': df_yesterday_summary.at['low_price', 'BUY'],
            'close': df_yesterday_summary.at['close_price', 'BUY'],
        }
        latest_summary['SELL']['yesterday'] = {
            'open': df_yesterday_summary.at['open_price', 'SELL'],
            'high': df_yesterday_summary.at['high_price', 'SELL'],
            'low': df_yesterday_summary.at['low_price', 'SELL'],
            'close': df_yesterday_summary.at['close_price', 'SELL'],
        }

    if not df_last_month_summary.empty:
        latest_summary['BUY']['last_month'] = {
            'open': df_last_month_summary.at['open_price', 'BUY'],
            'high': df_last_month_summary.at['high_price', 'BUY'],
            'low': df_last_month_summary.at['low_price', 'BUY'],
            'close': df_last_month_summary.at['close_price', 'BUY'],
        }
        latest_summary['SELL']['last_month'] = {
            'open': df_last_month_summary.at['open_price', 'SELL'],
            'high': df_last_month_summary.at['high_price', 'SELL'],
            'low': df_last_month_summary.at['low_price', 'SELL'],
            'close': df_last_month_summary.at['close_price', 'SELL'],
        }

    if not df_last_year_summary.empty:
        latest_summary['BUY']['last_year'] = {
            'open': df_last_year_summary.at['open_price', 'BUY'],
            'high': df_last_year_summary.at['high_price', 'BUY'],
            'low': df_last_year_summary.at['low_price', 'BUY'],
            'close': df_last_year_summary.at['close_price', 'BUY'],
        }
        latest_summary['SELL']['last_year'] = {
            'open': df_last_year_summary.at['open_price', 'SELL'],
            'high': df_last_year_summary.at['high_price', 'SELL'],
            'low': df_last_year_summary.at['low_price', 'SELL'],
            'close': df_last_year_summary.at['close_price', 'SELL'],
        }

    logger.debug(f'[{product_code}] AI用集計データ取得完了')

    return latest_summary


def gen_execution_summaries(product_code, year=2021, month=-1, day=-1):
    logger.debug(f'[{product_code} {year} {month} {day}] 集計データ作成開始')
    p_save_base_dir = Path(EXECUTION_HISTORY_DIR)
    p_product_dir = p_save_base_dir.joinpath(product_code)
    p_year_dir = p_product_dir.joinpath(str(year))
    if month == -1:
        if REF_LOCAL:
            for p_target_month_dir in p_year_dir.glob('*'):
                if not p_target_month_dir.is_dir():
                    continue
                p_month_dir = p_year_dir.joinpath(str(p_target_month_dir.name))
                if day == -1:
                    for p_target_day_dir in p_month_dir.glob('*'):
                        if p_target_day_dir.is_dir():
                            p_day_dir = p_month_dir.joinpath(
                                str(p_target_day_dir.name))
                            success = make_summary(product_code, p_day_dir, daily=True)
                            if not success:
                                logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                                return
                else:
                    p_day_dir = p_month_dir.joinpath(str(day))
                    success = make_summary(product_code, p_day_dir, daily=True)
                    if not success:
                        logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                        return
                make_summary(product_code, p_month_dir)
        else:
            target_month_dir_list = s3.listdir(str(p_year_dir))
            for target_month_dir in target_month_dir_list:
                if target_month_dir.endswith('summary.csv'):
                    continue
                dir_list = target_month_dir.split('/')
                dir_list.remove('')
                p_month_dir = p_year_dir.joinpath(
                    dir_list[-1]
                )
                if day == -1:
                    target_day_dir_list = s3.listdir(str(p_month_dir))
                    for target_day_dir in target_day_dir_list:
                        if not target_day_dir.endswith('summary.csv'):
                            dir_list = target_day_dir.split('/')
                            dir_list.remove('')
                            p_day_dir = p_month_dir.joinpath(
                                dir_list[-1]
                            )
                            success = make_summary(product_code, p_day_dir, daily=True)
                            if not success:
                                logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                                return
                else:
                    p_day_dir = p_month_dir.joinpath(format(int(day), '02'))
                    success = make_summary(product_code, p_day_dir, daily=True)
                    if not success:
                        logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                        return
                make_summary(product_code, p_month_dir)

    else:
        p_month_dir = p_year_dir.joinpath(format(int(month), '02'))
        if day == -1:
            if REF_LOCAL:
                for p_target_day_dir in p_month_dir.glob('*'):
                    if p_target_day_dir.is_dir():
                        p_day_dir = p_month_dir.joinpath(
                            str(p_target_day_dir.name)
                        )
                        success = make_summary(product_code, p_day_dir, daily=True)
                        if not success:
                            logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                            return
            else:
                target_day_dir_list = s3.listdir(str(p_month_dir))
                for target_day_dir in target_day_dir_list:
                    if not target_day_dir.endswith('summary.csv'):
                        p_day_dir = p_month_dir.joinpath(
                            target_day_dir.split('/')[-2]
                        )
                        success = make_summary(product_code, p_day_dir, daily=True)
                        if not success:
                            logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                            return
        else:
            p_day_dir = p_month_dir.joinpath(str(day))
            success = make_summary(product_code, p_day_dir, daily=True)
            if not success:
                logger.debug(f'[{p_day_dir}] データが存在しないため、集計を作成できませんでした。')
                return
        success = make_summary(product_code, p_month_dir)
    success = make_summary(product_code, p_year_dir)
    success = make_summary(product_code, p_product_dir)

    logger.debug(f'[{product_code} {year} {month} {day}] 集計データ作成終了')


def delete_row_data(product_code, current_datetime, days):
    before_7d_datetime = current_datetime - datetime.timedelta(days=days)
    p_dir = Path(EXECUTION_HISTORY_DIR)
    p_target_dir = p_dir.joinpath(
        product_code,
        before_7d_datetime.strftime('%Y'),
        before_7d_datetime.strftime('%m'),
        before_7d_datetime.strftime('%d'),
        'row'
    )

    if REF_LOCAL:
        if p_target_dir.is_dir():
            for filename in ['all.csv', 'buy.csv', 'sell.csv']:
                p_target_path = p_target_dir.joinpath(filename)
                p_target_path.unlink()
            p_target_dir.rmdir()
    else:
        s3.delete_dir(str(p_target_dir))
