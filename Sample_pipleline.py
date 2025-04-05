import pandas as pd
from os import chdir

chdir(r'./Teaching_Pandas/Sample pipeline')

def data_cleaning(input_csv):
    df = (pd.read_csv(input_csv, parse_dates=['date', 'transaction_date'])   # basic read file. It is highly recommended to use `read_csv` features.  
            .rename(columns=str.lower) # rename columns or change all to lower or upper
            .rename(columns= {'date':'date_key', 'customer_msisdn':'msisdn_nsk'}) # rename columns or change all to lower or upper
            .drop('payment_id', axis=1) # drop column or drop row
            .assign(date_key=lambda x: x['date_key'].dt.strftime('%Y%m%d'),  # make new columns from existintg - useful for calculation, data_time, change to categorical
                    msisdn_nsk_clean=lambda x: x['msisdn_nsk'].astype(str).str[2:],
                    msisdn_lastdigit=lambda x: x['msisdn_nsk'].astype(str).str[-1],
                    temp_hour=lambda x: x['transaction_date'].dt.hour)
            .query('delearname in ("SNAPCAB", "SNAPFOOD", "SNAPMARKET")')
            .sort_values(by = ['granted_gift_irr'], ascending=False)
            .assign(rank=lambda x: x.sort_values(['granted_gift_irr','msisdn_lastdigit','temp_hour'],
                                                 ascending=(False, True, True)
                                                 )
                    .groupby(['date_key'])
                    .cumcount()
                    + 1    
                    )
            .query("rank < 3")
            .sort_values(["date_key", "rank"])
          )
    return df
    
temp_df = data_cleaning('marketplace_cashback_20perc_20220517.csv')



def data_cleaning(input_csv):
    df = (
        pl.read_csv(input_csv, dtypes={'date': pl.Datetime, 'transaction_date': pl.Datetime})
        .pipe(lambda df: df.rename({col: col.lower() for col in df.columns}))
        .rename({'date': 'date_key', 'customer_msisdn': 'msisdn_nsk'})
        .drop('payment_id')
        .with_columns(
            pl.col('date_key').dt.strftime('%Y%m%d').alias('date_key'),
            pl.col('msisdn_nsk').cast(pl.Utf8).str.slice(2).alias('msisdn_nsk_clean'),
            pl.col('msisdn_nsk').cast(pl.Utf8).str.slice(-1).alias('msisdn_lastdigit'),
            pl.col('transaction_date').dt.hour().alias('temp_hour')
        )
        .filter(pl.col('delearname').is_in(["SNAPCAB", "SNAPFOOD", "SNAPMARKET"]))
        .with_columns(
            pl.int_range(1, pl.len() + 1)
            .over(
                'date_key',
                order_by=[
                    pl.col('granted_gift_irr').desc(),
                    'msisdn_lastdigit',
                    'temp_hour'
                ]
            )
            .alias('rank')
        )
        .sort(['date_key', 'rank'])
    )
    return df

temp_df = data_cleaning('marketplace_cashback_20perc_20220517.csv')


###################################################################
    
import datetime
import jdatetime 

def p_to_g(d):
    # d = jdatetime.datetime.now(d).strftime("%Y/%m/%d").split('/')
    d = d.split('/')
    # pd.to_datetime(jdatetime.date(int(d[0]), int(d[1]),int(d[2])).togregorian(),format="%Y/%m/%d")
    # jdatetime.date(int(d[0]), int(d[1]),int(d[2])).togregorian()
    return pd.to_datetime(jdatetime.date(int(d[0]), int(d[1]),int(d[2])).togregorian(),\
         utc=True, format="%Y/%m/%d") #tzinfo= 'Iran Standard Time',

df['gdate'] = df.apply(lambda x: p_to_g(x['pdate']), axis =1)

# sample functions to be used with df.pipe()
def extract_city_name(df):
    '''
    Chicago, IL -> Chicago for origin_city_name and dest_city_name
    '''
    cols = ['origin_city_name', 'dest_city_name']
    city = df[cols].apply(lambda x: x.str.extract("(.*), \w{2}", expand=False))
    df = df.copy()
    df[['origin_city_name', 'dest_city_name']] = city
    return df


### 

def time_to_datetime(df, columns):
    '''
    Combine all time items into datetimes.
    2014-01-01,0914 -> 2014-01-01 09:14:00
    '''
    df = df.copy()
    def converter(col):
        timepart = (col.astype(str)
                       .str.replace('\.0$', '')  # NaNs force float dtype
                       .str.pad(4, fillchar='0'))
        return pd.to_datetime(df['fl_date'] + ' ' +
                               timepart.str.slice(0, 2) + ':' +
                               timepart.str.slice(2, 4),
                               errors='coerce')
    df[columns] = df[columns].apply(converter)
    return df

