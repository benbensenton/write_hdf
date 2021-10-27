########################################################################################################################
########################################### Script to create and store hdf5 data########################################
# in main.py:
# from hdf import write_hdf as hdf
# import time
#
# t0 = time.monotonic()
#
# hdf()
#
# t1 = time.monotonic()
# elapsed = t1-t0
# print(f'...done, executed in: {elapsed}s')


# case 1: new data starts at the day/time where store ends --> just append
# case 2: new data starts at the day/time way after store ends --> we just append and give back what dates are missing
# to check for holiday etc
# case 3: new data starts at the day/time before store ends --> we want to drop all new data including the last store
# data and then append

# get current last date, check if new data has store last date

#      yes--> drop all data before, including store last date and append the rest
#      no --> is new date larger than store date
#              yes--> determine how many days are missing, give back to user and append new data
#              no--> new data has not the last store date and doesnt have larger dates, ergo dont do anything since new
#              data has no information value
#
# Intended structure of HDF Storage
# Tickersymbol -->(Price_Volume)-->Daily
#                               -->Minute
#                               -->ITCH Messages
#              -->(Optionchain)
#              -->(Fundamental) --> BalanceSheet, Equity/Debt
#              -->(Sentiment)   --> InsiderTransaction, Reddit/Twitter
#              -->(AlternativeData)
########################################################################################################################
########################################################################################################################


import pandas as pd
from pathlib import Path



# Entry Point-import this module and call write_hdf()
# Setup Container
# define path for storage from homedir 'C:/Users/x'
def write_hdf(store_path='Desktop\Stock Market\HDFStore.h5', csv_path='Desktop\AlphaVantage'):
    store_pathfile = Path.home().joinpath(store_path)

    # create empty pandas hdf5 file
    if store_pathfile.is_file():
        with pd.HDFStore(store_pathfile, 'r') as store:
            print('HDFStore.h5 ready at:', store)
    else:
        with pd.HDFStore(store_pathfile, 'a') as store:
            print('created Store at:', store)

    ########## Save CSV to Container ###########
    # define csv directory
    csv_pathfile = Path.home().joinpath(csv_path)

    # make list over all file paths in directory
    pathlist = sorted(csv_pathfile.glob('**/*.csv'))


    # check if key exist in store and get the timestamp from the last entrie
    # or create the missing key and write straight to disk
    def getLastStoreEntry(timetype, symbol, new_data, store_pathfile=store_pathfile, datatype='Price_Volume'):
        try:
            with pd.HDFStore(store_pathfile, 'r') as store:
                # get all store index entries in a list-chunk read?
                store_index = store.select_column(f'/{symbol}/{datatype}/{timetype}', 'index', start=0)
                # select rows in new_data with a bolean series which is returned by '.isin' operation, where the
                # datetime index value is not (~). (~ reverses the bolean array returned from '.isin()', true becomes
                # false and false becomes true) in the store index value
                unique_new_data = new_data[~new_data.index.isin(store_index)]
                # if unique_new_data has a value and the first datetimeindex > the last store index append unique data
            if (unique_new_data.values.any()) and (unique_new_data.index[0] > store_index.iloc[-1]):
                message2, error2 = hdf_append(timetype, symbol, unique_new_data, datatype)
                message1 = f'Store key exist for Symbol: {symbol},appended unique new data greater than last entry in HDF5 Store: Price_Volume {timetype}'
                error1 = 0
                return message1, error1, message2, error2

            else:
                # do nothing since no unique data
                message = f'nothing to Store appended, store key exist for Symbol {symbol}, no new unique Data in Dataframe found'
                q = 0; q1 = 0; q2 = 0
                return message, q, q1, q2

            # check if key exist and if, get last index datetime from store
            #store_last_entry = pd.read_hdf(store_pathfile, key=f'/{symbol}/Price_Volume/{timetype}', mode='r', start=-1,
                                           #columns=['timestamp'])
            #store_index_value = store_last_entry.index[0] if store_last_entry.index[0] else None
            # check if store_index_value is found in new_data Dataframe
            #if store_index_value in new_data.index.values:
                # case 2 key exist in store and last store entry exist in new data
                # slice the new dataframe to include only new data starting from (store_index_value +1)
                #p = new_data.index.get_loc(store_index_value)
                #start_position = p + 1
                #df_slice = new_data.iloc[start_position::, :]
                # it can happen that df index runs out of range with +1, that only 1 more item is written or that many
                # more are written
                #if not df_slice.empty:
                    #message2, error2 = hdf_append(timetype, symbol, df_slice)
                    #message1 = 'store key exist, found store Indexvalue in Dataframe'
                    #error1 = 0
                    #return message1, error1, message2, error2
                #else:
                    #message = f'store key exist for Symbol {symbol}, no new Data in Dataframe, last Timestamp in Store == last Timestamp in Dataframe'
                    #q = 0; q1 = 0; q2 = 0
                    #return message, q, q1, q2
            #else:
                # case 3, key exist but last store value is not found in dataframe
                # append all new data without slicing
                #message2, error2 = hdf_append(timetype, symbol, new_data)
                # time difference between last store timestamp
                #message1 = 'store key exist but did not find store Indexvalue in Dataframe'
                #error1 = 0
                #return message1, error1, message2, error2
        except KeyError as e:
            # case 1 KeyError raised when key not found in store, ergo dir does not exists ergo new data is uniquely new,
            # ergo writting all data
            message, ee = hdf_create(timetype, symbol, new_data, datatype)
            return message, symbol, e, ee


    # hdf append writer function
    def hdf_append(timetype, symbol, df, datatype, store_pathfile=store_pathfile):
        try:
            with pd.HDFStore(store_pathfile, 'a') as store:
                store.append(f'/{symbol}/{datatype}/{timetype}', df, axes=None, index=True)
                message = f'appended data for Symbol: {symbol} in HDF5 Store: Price_Volume {timetype}'
                e = 0
                return message, e
        except Exception as e:
            message = f'error occurred for Symbol: {symbol} while appending new entry in HDF5 Store: Price_Volume {timetype}'
            return message, e


    # hdf create writer function
    def hdf_create(timetype, symbol, df, datatype, store_pathfile=store_pathfile):
        try:
            with pd.HDFStore(store_pathfile,
                             'a') as store:  # need to open the store in append otherwise whole store gets overwritten
                store.put(f'/{symbol}/{datatype}/{timetype}', df, index=False, append=True, format='table',
                          data_columns=True)
                store.flush(fsync=True)
                message = f'new Symbol: {symbol} included in HDF5 Store: Price_Volume {timetype}'
                e = 0
                return message, e
        except Exception as e:
            message = f'error occurred for Symbol: {symbol} while creating new entry in HDF5 Store: Price_Volume {timetype}'
            return message, e


    # load csv from path
    if pathlist:
        for p in pathlist:
            # strip p to symbol only
            s1 = str(p)
            s2 = str(csv_pathfile)
            s2 = s2 + '\\'
            if s1.startswith(s2):
                s3 = s1.replace(s2, '')
            s4 = s3.split('_', 1)
            symbol = s4[0]

            # read csv, convert type, round
            dfi = pd.read_csv(p)
            dfii = dfi.astype({'open': 'float32', 'high': 'float32', 'low': 'float32', 'close': 'float32',
                               'adjusted_close': 'float32', 'dividend_amount': 'float32',
                               'split_coefficient': 'float32'})
            df = dfii.round({'open': 2, 'high': 2, 'low': 2, 'close': 2, 'adjusted_close': 2, 'dividend_amount': 2,
                             'split_coefficient': 2})

            # make some adjustments
            df.sort_index(ascending=False, inplace=True)  # oldest on top, newest on bottom bc hdf5 only append to EOF
            df.dropna(inplace=True)
            df.reset_index(drop=True, inplace=True)
            df.index = pd.to_datetime(df['timestamp'])
            df.drop(['timestamp'], axis=1, inplace=True)

            # get the type of data: message, minute, daily
            time_type = (df.index[1] - df.index[0]) / pd.Timedelta("1s")
            # select the right group to check and update the storage
            if 60 <= time_type < 86400:
                timetype = 'Minute'  # minute data
                x, y, e, ee = getLastStoreEntry(timetype, symbol, df)
                print(f'last Indexvalue: {x}, Symbol: {y}, Error 1: {e}, Error 2: {ee}\n')
            elif time_type >= 86400:
                timetype = 'Daily'  # daily data
                x, y, e, ee = getLastStoreEntry(timetype, symbol, df)
                print(f'last Indexvalue: {x}, Symbol: {y}, Error 1: {e}, Error 2: {ee}\n')
            else:
                timetype = 'ITCH'  # message data
                x, y, e, ee = getLastStoreEntry(timetype, symbol, df)
                print(f'last Indexvalue: {x}, Symbol: {y}, Error 1: {e}, Error 2: {ee}\n')
    else:
        return 'no files in directory'

