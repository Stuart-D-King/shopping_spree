import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import scipy.stats as scs


def prep_data():
    df = pd.read_csv('Final_Output_StuartKing.csv')
    df['created_at_mst'] = pd.to_datetime(df['created_at_mst'])
    df['gender'] = df['gender'].replace(' ', np.nan)
    df = df[df['age'] < 90]

    for col in ['flag_price_imputed', 'flag_qty_imputed']:
        df[col] = np.where(df[col] == 'yes',1,0)
        df[col] = df[col].astype(bool)

    df['month'] = df.created_at_mst.dt.month
    df['dayofweek'] = df.created_at_mst.dt.dayofweek
    df['date'] = df.created_at_mst.dt.date
    df['hour'] = df.created_at_mst.dt.hour

    return df


def time_series(df, state=None, retailer_type=None):
    '''
    Create time series line graph of the number of purchases each day

    :param df: dataframe from which data is pulled
    :param state: a particular state to evaluate
    :param retailer_type: a particular retail industry to evaluate
    '''
    if state:
        df = df[df['state'] == state]

    if retailer_type:
        df = df[df['retailer_type'] == retailer_type]

    fig = plt.figure(figsize=(10,6))
    ax = fig.add_subplot(111)

    sep = df[df['month'] == 9]
    oct = df[df['month'] == 10]
    nov = df[df['month'] == 11]
    dec = df[df['month'] == 12]
    sep_dec = df[df['month'].isin([9,10,11,12])]

    date_count_sep = sep.groupby('date')['receipt_id'].agg({'count': len})
    date_count_oct = oct.groupby('date')['receipt_id'].agg({'count': len})
    date_count_nov = nov.groupby('date')['receipt_id'].agg({'count': len})
    date_count_dec = dec.groupby('date')['receipt_id'].agg({'count': len})
    date_count_sep_dec = sep_dec.groupby('date')['receipt_id'].agg({'count': len})

    mu = date_count_sep_dec['count'].mean()
    stdev = date_count_sep_dec['count'].std()

    ax.plot(date_count_sep.index.values, date_count_sep.values,'red', marker = 'o', label='Sep')
    ax.plot(date_count_oct.index.values, date_count_oct.values,'green', marker = 'o', label='Oct')
    ax.plot(date_count_nov.index.values, date_count_nov.values,'yellow', marker = 'o', label='Nov')
    ax.plot(date_count_dec.index.values, date_count_dec.values,'blue', marker = 'o', label='Dec')

    ax.axhline(y = mu, color='black')
    ax.axhline(y = mu + 1.5*stdev, linestyle='--', color='black')
    ax.axhline(y = mu - 1.5*stdev, linestyle='--', color='black')

    ax.set_ylabel('Number of Purchases', fontsize=16)
    ax.set_title('Daily Shopping', fontsize=20)

    ax.legend(loc='lower center', ncol=4, fontsize=16)
    plt.tight_layout()
    plt.savefig('img/time_series.png', dpi=200)


def weekday_weekend_daily(df, state=None, retailer_type=None):
    '''
    Create weekday and weekend histograms of the number of daily purchases

    :param df: dataframe from which data is pulled
    :param state: a particular state to evaluate
    :param retailer_type: a particular retail industry to evaluate
    '''
    if state:
        df = df[df['state'] == state]

    if retailer_type:
        df = df[df['retailer_type'] == retailer_type]

    fig = plt.figure(figsize=(8,6))
    ax = fig.add_subplot(111)

    weekday = df[df['created_at_mst'].dt.weekday <= 4]
    weekend = df[df['created_at_mst'].dt.weekday > 4]
    weekday_count = weekday.groupby(weekday['created_at_mst'].dt.date)['receipt_id'].count()
    weekend_count = weekend.groupby(weekend['created_at_mst'].dt.date)['receipt_id'].count()

    ax.hist(weekday_count.values, color='b', alpha=0.5, bins=20, label='weekday', normed=True)

    density = scs.kde.gaussian_kde(weekday_count.values)
    x_vals = np.linspace(weekday_count.values.min(), weekday_count.values.max(), 100)
    kde_vals = density(x_vals)
    ax.plot(x_vals, kde_vals, 'b-')

    ax.hist(weekend_count.values, color='g', alpha=0.5, bins=20, label='weekend', normed=True)

    density = scs.kde.gaussian_kde(weekend_count.values)
    x_vals = np.linspace(weekend_count.values.min(), weekend_count.values.max(), 100)
    kde_vals = density(x_vals)
    ax.plot(x_vals, kde_vals, 'g-')

    ax.set_xlabel('Purchases', fontsize=16)
    ax.set_ylabel('Frequency', fontsize=16)
    ax.set_title('Weekday and Weekend Shopping', fontsize=20)
    ax.legend(fontsize=16)

    plt.tight_layout()
    plt.savefig('img/wkday_wkend_daily.png', dpi=200)


def weekday_weekend_hourly(df, state=None, retailer_type=None):
    '''
    Create hourly weekday and weekend boxplots of the number of purchases

    :param df: dataframe from which data is pulled
    :param state: a particular state to evaluate
    :param retailer_type: a particular retail industry to evaluate
    '''
    if state:
        df = df[df['state'] == state]

    if retailer_type:
        df = df[df['retailer_type'] == retailer_type]

    fig = plt.figure(figsize=(8,8))
    ax1 = fig.add_subplot(211)
    ax2 = fig.add_subplot(212)

    weekday = df[(df['dayofweek'] <= 4)]
    weekend = df[(df['dayofweek'] > 4)]

    weekday_hour_count = weekday.groupby(['date', 'hour'])['receipt_id'].agg({'count': len})
    weekday_hour_count = weekday_hour_count.unstack(level='hour')

    weekend_hour_count = weekend.groupby(['date', 'hour'])['receipt_id'].agg({'count': len})
    weekend_hour_count = weekend_hour_count.unstack(level='hour')

    weekday_hour_count.plot(kind='box', ax=ax1).set_xticklabels(range(24));
    weekend_hour_count.plot(kind='box', ax=ax2).set_xticklabels(range(24));

    ax1.set_title('Weekday Shopping By Hour', fontsize=18)
    ax2.set_title('Weekend Shopping By Hour', fontsize=18)
    ax1.set_ylabel('Number of Purchases', fontsize=14)
    ax2.set_ylabel('Number of Purchases', fontsize=14)

    plt.tight_layout()
    plt.savefig('img/wkday_wkend_boxplots.png', dpi=200)


if __name__ == '__main__':
    plt.close('all')
    df = prep_data()
