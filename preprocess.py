import json
import datetime
import time
import pandas as pd
from pathlib import Path
from pprint import pprint
from logging import getLogger

from bitflyer_api import *
from ai import *
from manage import LOCAL, BUCKET_NAME


logger = getLogger(__name__)

if not LOCAL:
    from aws import S3

BALANCE_LOG_DIR = 'balance_log'
EXECUTION_HISTORY_DIR = 'execute_history'


def get_executions_history(start_date, end_date, region='Asia/Tokyo', product_code='ETH_JPY', count=500, return_df=False):
    logger.info(
        f'[{start_date} - {end_date}] 取引履歴をダウンロード中...')

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
            end_date_tmp.strftime('%Y'), end_date_tmp.strftime('%m'), end_date_tmp.strftime('%d'))

        p_save_dir_row = p_save_dir.joinpath('row')
        p_save_dir_1h = p_save_dir.joinpath('1h')
        p_save_dir_1m = p_save_dir.joinpath('1m')
        p_save_dir_10m = p_save_dir.joinpath('10m')

        p_save_path_row_all = p_save_dir_row.joinpath('all.csv')
        p_save_path_row_buy = p_save_dir_row.joinpath('buy.csv')
        p_save_path_row_sell = p_save_dir_row.joinpath('sell.csv')

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

        if p_save_path_row_all.exists():
            if LOCAL:
                df = pd.read_csv(str(p_save_path_row_all))
            else:
                s3 = S3(BUCKET_NAME)
                df = s3.read_csv(str(p_save_path_row_all))

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

        while target_date_start < df.head(1).index[0]:
            df_new = get_executions(product_code, count, before=before)
            if df_new.empty:
                break
            df_new = df_new.tz_convert(region)
            df = pd.concat([df, df_new])
            df = df.sort_index()
            before = int(df.head(1)['id'])
            if LOCAL:
                time.sleep(0.25)

        while df.tail(1).index[0] < target_date_end:
            df_new = get_executions(product_code, count, after=after)
            if df_new.empty:
                break
            df_new = df_new.tz_convert(region)
            df = pd.concat([df, df_new])
            df = df.sort_index()
            after = int(df.tail(1)['id'])
            if LOCAL:
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

            if LOCAL:
                df.to_csv(str(p_save_path_row_all))
                df_buy.to_csv(str(p_save_path_row_buy))
                df_sell.to_csv(str(p_save_path_row_sell))
            else:
                s3 = S3(BUCKET_NAME)
                s3.to_csv(
                    str(p_save_path_row_all),
                    df=df
                )
                s3.to_csv(
                    str(p_save_path_row_buy),
                    df=df_buy
                )
                s3.to_csv(
                    str(p_save_path_row_sell),
                    df=df_sell
                )

            p_save_dir_1h = p_save_dir.joinpath('1h')
            p_save_dir_1m = p_save_dir.joinpath('1m')
            p_save_dir_10m = p_save_dir.joinpath('10m')

            df_buy_resample = df_buy[['price', 'size']]
            df_sell_resample = df_sell[['price', 'size']]

            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_1h, 'H')
            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_1m, 'T')
            resampling(df_buy_resample, df_sell_resample,
                       p_save_dir_10m, '10T')

            logger.info(f'{target_date_start} のデータ更新が完了しました。')
        if day_count == 3:
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

    logger.info(f'[{start_date} - {end_date}] 取引履歴のダウンロード終了')

    if return_df:
        return df_history


def resampling(df_buy, df_sell, p_save_dir='', freq='T'):
    df_buy_price = df_buy[['price']]
    df_buy_size = df_buy[['size']]
    df_buy_price = df_buy_price.resample(freq).mean()
    df_buy_size = df_buy_size.resample(freq).sum()
    df_buy_resampled = pd.concat([df_buy_price, df_buy_size], axis=1)
    if not p_save_dir == '':
        if LOCAL:
            df_buy_resampled.to_csv(str(p_save_dir.joinpath('buy.csv')))
        else:
            s3 = S3(BUCKET_NAME)
            s3.to_csv(
                str(p_save_dir.joinpath('buy.csv')),
                df=df_buy_resampled
            )

    df_sell_price = df_sell[['price']]
    df_sell_size = df_sell[['size']]
    df_sell_price = df_sell_price.resample(freq).mean()
    df_sell_size = df_sell_size.resample(freq).sum()
    df_sell_resampled = pd.concat([df_sell_price, df_sell_size], axis=1)
    if not p_save_dir == '':
        if LOCAL:
            df_sell_resampled.to_csv(str(p_save_dir.joinpath('sell.csv')))
        else:
            s3 = S3(BUCKET_NAME)
            s3.to_csv(
                str(p_save_dir.joinpath('sell.csv')),
                df=df_sell_resampled
            )

    return df_buy_resampled, df_sell_resampled


def make_summary_from_scratch(p_dir):
    p_buy_path = p_dir.joinpath('buy.csv')
    p_sell_path = p_dir.joinpath('sell.csv')
    p_summary_path = p_dir.joinpath('..', 'summary.csv')

    if LOCAL:
        df_buy = pd.read_csv(str(p_buy_path))
        df_sell = pd.read_csv(str(p_sell_path))
    else:
        s3 = S3(BUCKET_NAME)
        df_buy = s3.read_csv(str(p_buy_path))
        df_sell = s3.read_csv(str(p_sell_path))
    df_summary = pd.DataFrame(
        [
            {
                'CATEGORY': 'max_price',
                'BUY': int(df_buy['price'].max()),
                'SELL': int(df_sell['price'].max())
            },
            {
                'CATEGORY': 'mean_price',
                'BUY': float(df_buy['price'].mean()),
                'SELL': float(df_sell['price'].mean())
            },
            {
                'CATEGORY': 'median_price',
                'BUY': float(df_buy['price'].median()),
                'SELL': float(df_sell['price'].median())
            },
            {
                'CATEGORY': 'min_price',
                'BUY': int(df_buy['price'].min()),
                'SELL': int(df_sell['price'].min())
            },
            {
                'CATEGORY': 'size',
                'BUY': float(df_buy['size'].sum()),
                'SELL': float(df_sell['size'].sum())
            },
        ]
    )

    if LOCAL:
        df_summary.to_csv(str(p_summary_path), index=False)
    else:
        s3 = S3(BUCKET_NAME)
        s3.to_csv(
            str(p_summary_path),
            df=df_summary,
            index=False
        )

    return df_summary


def make_summary_from_csv(p_dir='', p_summary_path_list=[], save=True):
    df_summary = pd.DataFrame()
    if p_summary_path_list == [] and p_dir != '':
        p_summary_path_list = p_dir.glob('*/summary.csv')

    for p_summary_path in p_summary_path_list:
        if not p_summary_path.exists():
            continue

        if LOCAL:
            df_summary_child = pd.read_csv(str(p_summary_path))
        else:
            aws = S3(BUCKET_NAME)
            df_summary_child = aws.read_csv(str(p_summary_path))

        df_summary_child = df_summary_child.set_index('CATEGORY', drop=True)

        if df_summary.empty:
            df_summary = df_summary_child.copy()
        else:
            if df_summary.at['max_price', 'BUY'] < df_summary_child.at['max_price', 'BUY']:
                df_summary.at['max_price',
                              'BUY'] = df_summary_child.at['max_price', 'BUY']
            if df_summary.at['max_price', 'SELL'] < df_summary_child.at['max_price', 'SELL']:
                df_summary.at['max_price',
                              'SELL'] = df_summary_child.at['max_price', 'SELL']

            if df_summary.at['min_price', 'BUY'] > df_summary_child.at['min_price', 'BUY']:
                df_summary.at['min_price',
                              'BUY'] = df_summary_child.at['min_price', 'BUY']
            if df_summary.at['min_price', 'SELL'] > df_summary_child.at['min_price', 'SELL']:
                df_summary.at['min_price',
                              'SELL'] = df_summary_child.at['min_price', 'SELL']

            df_summary.at['size',
                          'BUY'] += df_summary_child.at['size', 'BUY']

            df_summary.at['size',
                          'SELL'] += df_summary_child.at['size', 'SELL']

    if save and p_dir != '':
        p_summary_save_path = p_dir.joinpath('summary.csv')

        if LOCAL:
            df_summary.to_csv(str(p_summary_save_path))
        else:
            s3 = S3(BUCKET_NAME)
            s3.to_csv(
                str(p_summary_save_path),
                df=df_summary,
            )

    return df_summary


def make_summary(p_dir, daily=False):
    if daily:
        p_1h_dir = p_dir.joinpath('1h')
        make_summary_from_scratch(p_1h_dir)
    else:
        make_summary_from_csv(p_dir)


def estimate_trends(latest_summary):
    # TODO: need to fix
    side_list = ['BUY', 'SELL']
    time_list = ['1m', '1w', '1d', '1h', '10min', '1min']

    for i, target_time in enumerate(time_list):
        up_count = 0
        ref_count = 0
        for ref_time in time_list[i:]:
            if latest_summary['BUY'][target_time]['price']['max'] > latest_summary['BUY'][ref_time]['price']['max']:
                up_count += 1
            if latest_summary['BUY'][target_time]['price']['min'] > latest_summary['BUY'][ref_time]['price']['min']:
                up_count += 1
            if latest_summary['BUY'][target_time]['price']['max'] < latest_summary['BUY'][ref_time]['price']['max']:
                up_count -= 1
            if latest_summary['BUY'][target_time]['price']['min'] < latest_summary['BUY'][ref_time]['price']['min']:
                up_count -= 1
            ref_count += 1
            if ref_count == 2:
                break
        ref_count -= 1
        for side in side_list:
            if ref_count <= 1:
                up_count_norm = up_count
            else:
                up_count_norm = up_count / ref_count
            if 0.5 <= up_count_norm:
                latest_summary[side][target_time]['trend'] = 'UP'
            elif up_count_norm <= -0.5:
                latest_summary[side][target_time]['trend'] = 'DOWN'
            else:
                latest_summary[side][target_time]['trend'] = 'EVEN'

    return latest_summary


def obtain_latest_summary(product_code, daily=False):

    p_exe_history_dir = Path(EXECUTION_HISTORY_DIR)
    p_all_summary_path = p_exe_history_dir.joinpath(
        product_code, 'summary.csv')

    # daily summary
    current_datetime = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=9)))
    before_1m_datetime = current_datetime - datetime.timedelta(minutes=1)
    before_10m_datetime = current_datetime - datetime.timedelta(minutes=10)
    before_1h_datetime = current_datetime - datetime.timedelta(hours=1)
    before_1d_datetime = current_datetime - datetime.timedelta(days=1)
    before_2d_datetime = current_datetime - datetime.timedelta(days=2)
    before_32d_datetime = current_datetime - datetime.timedelta(days=32)

    df = get_executions_history(
        start_date=before_2d_datetime, end_date=current_datetime, product_code='ETH_JPY', return_df=True)

    df_buy = df.query('side == "BUY"')
    df_sell = df.query('side == "SELL"')

    df_buy_resample = df_buy[['price', 'size']]
    df_sell_resample = df_sell[['price', 'size']]

    df_buy_resampled, df_sell_resampled = resampling(
        df_buy[['price', 'size']], df_sell[['price', 'size']], freq='S')

    df_buy_1m = df_buy_resampled.query('index > @before_1m_datetime')
    df_sell_1m = df_sell_resampled.query('index > @before_1m_datetime')

    df_buy_10m = df_buy_resampled.query('index > @before_10m_datetime')
    df_sell_10m = df_sell_resampled.query('index > @before_10m_datetime')

    df_buy_1h = df_buy_resampled.query('index > @before_1h_datetime')
    df_sell_1h = df_sell_resampled.query('index > @before_1h_datetime')

    df_buy_1d = df_buy_resampled.query('index > @before_1d_datetime')
    df_sell_1d = df_sell_resampled.query('index > @before_1d_datetime')

    gen_execution_summaries(year=current_datetime.strftime('%Y'), month=-1, day=-1,
                            product_code='ETH_JPY')

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
        ),
    ]
    df_monthly_summary = make_summary_from_csv(
        p_summary_path_list=p_target_monthly_summary_path_list,
        save=False
    )
    # weekly summary
    p_target_weekly_summary_path_list = [
        p_exe_history_dir.joinpath(
            product_code,
            current_datetime.strftime('%Y'),
            current_datetime.strftime('%m'),
            current_datetime.strftime('%d'),
            'summary.csv'
        )
    ]
    for i in range(1, 8):
        target_datetime = current_datetime - datetime.timedelta(days=i)
        p_target_weekly_summary_path_list.append(
            p_exe_history_dir.joinpath(
                product_code,
                target_datetime.strftime('%Y'),
                target_datetime.strftime('%m'),
                target_datetime.strftime('%d'),
                'summary.csv'
            )
        )

    df_weekly_summary = make_summary_from_csv(
        p_summary_path_list=p_target_weekly_summary_path_list,
        save=False
    )

    # yearly summary
    before_365d_datetime = current_datetime - datetime.timedelta(days=365)
    p_target_yearly_summary_path_list = [
        p_exe_history_dir.joinpath(
            product_code,
            current_datetime.strftime('%Y'),
            current_datetime.strftime('%m'),
            current_datetime.strftime('%d'),
            'summary.csv'
        ),
        p_exe_history_dir.joinpath(
            product_code,
            before_365d_datetime.strftime('%Y'),
            before_365d_datetime.strftime('%m'),
            before_365d_datetime.strftime('%d'),
            'summary.csv'
        )
    ]

    df_yearly_summary = make_summary_from_csv(
        p_summary_path_list=p_target_yearly_summary_path_list,
        save=False
    )

    if LOCAL:
        df_all_summary = pd.read_csv(str(p_all_summary_path))
    else:
        s3 = S3(BUCKET_NAME)
        df_all_summary = s3.read_csv(str(p_all_summary_path))

    df_all_summary = df_all_summary.set_index('CATEGORY', drop=True)

    latest_summary = {
        'BUY': {
            '1min': {
                'price': {
                    'max': df_buy_1m['price'].max(),
                    'min': df_buy_1m['price'].min(),
                },
                'trend': 'DOWN',
            },
            '10min': {
                'price': {
                    'max': df_buy_10m['price'].max(),
                    'min': df_buy_10m['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1h': {
                'price': {
                    'max': df_buy_1h['price'].max(),
                    'min': df_buy_1h['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1d': {
                'price': {
                    'max': df_buy_1d['price'].max(),
                    'min': df_buy_1d['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1w': {
                'price': {
                    'max': df_weekly_summary.at['max_price', 'BUY'],
                    'min': df_weekly_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            '1m': {
                'price': {
                    'max': df_monthly_summary.at['max_price', 'BUY'],
                    'min': df_monthly_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            '1y': {
                'price': {
                    'max': df_yearly_summary.at['max_price', 'BUY'],
                    'min': df_yearly_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            'all': {
                'price': {
                    'max': df_all_summary.at['max_price', 'BUY'],
                    'min': df_all_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
        },
        'SELL': {
            '1min': {
                'price': {
                    'max': df_sell_1m['price'].max(),
                    'min': df_sell_1m['price'].min(),
                },
                'trend': 'DOWN',
            },
            '10min': {
                'price': {
                    'max': df_sell_10m['price'].max(),
                    'min': df_sell_10m['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1h': {
                'price': {
                    'max': df_sell_1h['price'].max(),
                    'min': df_sell_1h['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1d': {
                'price': {
                    'max': df_sell_1d['price'].max(),
                    'min': df_sell_1d['price'].min(),
                },
                'trend': 'DOWN',
            },
            '1w': {
                'price': {
                    'max': df_weekly_summary.at['max_price', 'SELL'],
                    'min': df_weekly_summary.at['min_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
            '1m': {
                'price': {
                    'max': df_monthly_summary.at['max_price', 'SELL'],
                    'min': df_monthly_summary.at['min_price', 'SELL'],
                },
                'trend': 'DOWN',
            },
            '1y': {
                'price': {
                    'max': df_yearly_summary.at['max_price', 'BUY'],
                    'min': df_yearly_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
            'all': {
                'price': {
                    'max': df_all_summary.at['max_price', 'BUY'],
                    'min': df_all_summary.at['min_price', 'BUY'],
                },
                'trend': 'DOWN',
            },
        }
    }
    latest_summary = estimate_trends(latest_summary)

    return latest_summary


def gen_execution_summaries(year=2021, month=-1, day=-1, product_code='ETH_JPY'):

    p_save_base_dir = Path(EXECUTION_HISTORY_DIR)
    p_product_dir = p_save_base_dir.joinpath(product_code)
    p_year_dir = p_product_dir.joinpath(str(year))
    if month == -1:
        for p_target_month_dir in p_year_dir.glob('*'):
            if not p_target_month_dir.is_dir():
                continue
            p_month_dir = p_year_dir.joinpath(str(p_target_month_dir.name))
            if day == -1:
                for p_target_day_dir in p_month_dir.glob('*'):
                    if p_target_day_dir.is_dir():
                        p_day_dir = p_month_dir.joinpath(
                            str(p_target_day_dir.name))
                        make_summary(p_day_dir, daily=True)
            else:
                p_day_dir = p_month_dir.joinpath(str(day))
                make_summary(p_day_dir, daily=True)
            make_summary(p_month_dir)

    else:
        p_month_dir = p_year_dir.joinpath(str(month))
        if day == -1:
            for p_target_day_dir in p_month_dir.glob('*'):
                if p_target_day_dir.is_dir():
                    p_day_dir = p_month_dir.joinpath(
                        str(p_target_day_dir.name))
                    make_summary(p_day_dir, daily=True)
        else:
            p_day_dir = p_month_dir.joinpath(str(day))
            make_summary(p_day_dir, daily=True)
        make_summary(p_month_dir)

    make_summary(p_year_dir)
    make_summary(p_product_dir)
