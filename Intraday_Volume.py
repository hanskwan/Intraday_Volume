#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 14 22:26:47 2022

@author: Hans
"""

import pandas as pd
import numpy as np

# Daiwa electronic trading strategy with asia markets intraday volume

# terminology
# th = trading hours, oa = open auction, ca = close auction

# hardcode exchange and index
exchange_dic = {'Australia':'AS51 Index', 
            "China":"SHSZ300 Index", 
            "Hong Kong":"HSI Index", 
            "India":"NIFTY Index", 
            "Indonesia":"MXID Index", 
            "Japan":"TPX Index", 
            "Korea":"MXKR Index", 
            "Malaysia":"MXMY Index", 
            "Philippines":"MXPH Index", 
            "Singapore":"MXSG Index",  
            "Taiwan":"TWSE Index", 
            "Thailand":"MXTH Index" 
            }

# Open auction start time
open_time_dic = {'AS51 Index': '10:00', 'SHSZ300 Index' : '09:15',
                 'HSI Index':"09:00",'NIFTY Index':'09:00',
                 'MXID Index':'08:45', 'TPX Index':'08:00',
                 'MXKR Index':'08:30', 'MXMY Index':'08:30',
                 'MXPH Index':'09:00', 'MXSG Index':'08:30',
                 'TWSE Index':'08:30', 'MXTH Index':'09:30'
                 }

# close auction start time
close_time_dic = {'AS51 Index': '16:00', 'SHSZ300 Index' : '14:57',
                 'HSI Index':"16:00",'NIFTY Index':'15:40',
                 'MXID Index':'14:50', 'TPX Index':'15:00',
                 'MXKR Index':'15:20', 'MXMY Index':'16:45',
                 'MXPH Index':'12:45', 'MXSG Index':'17:00',
                 'TWSE Index':'13:25', 'MXTH Index':'16:30'
                 }

# handling the open auction volume and transform to output format
def oa_volume(df): # open auction volume
    # dataframe rearrange
    df1 = df.iloc[1:] # remove first row
    
    df1_col_all = df1.columns.tolist()
    index_name = df1_col_all[0] # save index name
    df1_col_nan = df1_col_all[1:]
    df1_col_nan # get all columns names apart from first col
    
    df2 = df1.dropna(subset=df1_col_nan, how='all')
    df2 # Remove all rows that with all NaN starting from Column 2
    
    # open auction time
    open_hour = open_time_dic[index_name][0:2]
    open_minute = open_time_dic[index_name][3:5]
    
    # Filter out month, hour and minute from first column timestamp 
    df2['Year'] = pd.to_datetime(df2.iloc[:,0]).dt.year
    df2['Month'] = pd.to_datetime(df2.iloc[:,0]).dt.month
    df2['Hour'] = pd.to_datetime(df2.iloc[:,0]).dt.hour + int(open_hour)
    df2['Minute'] = pd.to_datetime(df2.iloc[:,0]).dt.minute + int(open_minute)
    
    # remove first column timestamp and replace by month, hour and minute
    df3 = df2.drop(df2.columns[0], axis = 1)
    df3_cols = df3.columns.tolist()
    df3_cols = df3_cols[-4:] + df3_cols[:-4]
    df3 = df3[df3_cols]# reorder df3
    
    # Sum all the volume for each row
    df4 = df3
    df4['Total Volume'] = df4.iloc[:, 6:].sum(axis=1)
    
    # rearrange Total Volume to front and remove the individual volume columns
    df5 = df4
    df5_cols = df5.columns.tolist()
    df5_cols = df5_cols[0:4] + df5_cols[-1:] + df5_cols[4:-1]
    df5 = df5[df5_cols]
    df5 = df5.drop(df5.columns[5:], axis = 1)
    
    # Sum the volume for each unique time period in the month
    df6 = df5
    df7 = df6.groupby(['Year', 'Month', 'Hour', 'Minute'])["Total Volume"].apply(lambda x : x.astype(int).sum())
    df8 = df7.to_frame()
    df9 = df8.rename({'Total Volume': 'Open Auction Volume'}, axis = 1)
    return df9



# handling the close auction volume and transform to output format
def ca_volume(df): # close auction volume
    # dataframe rearrange
    df1 = df.iloc[1:] # remove first row
    
    df1_col_all = df1.columns.tolist()
    index_name = df1_col_all[0] # save index name
    df1_col_nan = df1_col_all[1:]
    df1_col_nan # get all columns names apart from first col
    
    df2 = df1.dropna(subset=df1_col_nan, how='all')
    df2 # Remove all rows that with all NaN starting from Column 2
    
    #close auction time
    close_hour = close_time_dic[index_name][0:2]
    close_minute = close_time_dic[index_name][3:5]
    
    # Filter out month, hour and minute from first column timestamp 
    df2['Year'] = pd.to_datetime(df2.iloc[:,0]).dt.year
    df2['Month'] = pd.to_datetime(df2.iloc[:,0]).dt.month
    df2['Hour'] = pd.to_datetime(df2.iloc[:,0]).dt.hour + int(close_hour)
    df2['Minute'] = pd.to_datetime(df2.iloc[:,0]).dt.minute + int(close_minute)
    
    # remove first column timestamp and replace by month, hour and minute
    df3 = df2.drop(df2.columns[0], axis = 1)
    df3_cols = df3.columns.tolist()
    df3_cols = df3_cols[-4:] + df3_cols[:-4]
    df3 = df3[df3_cols]# reorder df3
    
    # Sum all the volume for each row
    df4 = df3
    df4['Total Volume'] = df4.iloc[:, 6:].sum(axis=1)
    
    # rearrange Total Volume to front and remove the individual volume columns
    df5 = df4
    df5_cols = df5.columns.tolist()
    df5_cols = df5_cols[0:4] + df5_cols[-1:] + df5_cols[4:-1]
    df5 = df5[df5_cols]
    df5 = df5.drop(df5.columns[5:], axis = 1)
    
    # Sum the volume for each unique time period in the month
    df6 = df5
    df7 = df6.groupby(['Year', 'Month', 'Hour', 'Minute'])["Total Volume"].apply(lambda x : x.astype(int).sum())
    df8 = df7.to_frame()
    df9 = df8.rename({'Total Volume': 'Close Auction Volume'}, axis = 1)
    return df9



# handling the trading hours volume and transform to output format
# trading hours volume add in both open and close auction price
def th_volume(df, df_open, df_close): 
    # dataframe rearrange
    df1 = df.iloc[1:] # remove first row
    
    df1_col_all = df1.columns.tolist()
    index_name = df1_col_all[0] # save index name
    df1_col_nan = df1_col_all[1:]
    df1_col_nan # get all columns names apart from first col
    
    df2 = df1.dropna(subset=df1_col_nan, how='all')
    df2 # Remove all rows that with all NaN starting from Column 2
    
    # Filter out month, hour and minute from first column timestamp 
    df2['Year'] = pd.to_datetime(df2.iloc[:,0]).dt.year
    df2['Month'] = pd.to_datetime(df2.iloc[:,0]).dt.month
    df2['Hour'] = pd.to_datetime(df2.iloc[:,0]).dt.hour
    df2['Minute'] = pd.to_datetime(df2.iloc[:,0]).dt.minute
    
    # remove first column timestamp and replace by month, hour and minute
    df3 = df2.drop(df2.columns[0], axis = 1)
    df3_cols = df3.columns.tolist()
    df3_cols = df3_cols[-4:] + df3_cols[:-4]
    df3_cols
    df3 = df3[df3_cols]# reorder df3
    
    # Sum all the volume for each row
    df4 = df3
    df4['Total Volume'] = df4.iloc[:, 4:].sum(axis=1)
    df4
    # rearrange Total Volume to front and remove the individual volume columns
    df5 = df4
    df5_cols = df5.columns.tolist()
    df5_cols = df5_cols[0:4] + df5_cols[-1:] + df5_cols[4:-1]
    df5_cols
    df5 = df5[df5_cols]
    df5 = df5.drop(df5.columns[5:], axis = 1)
    
    # Sum the volume for each unique time period in the month
    df6 = df5
    df7 = df6.groupby(['Year', 'Month', 'Hour', 'Minute'])["Total Volume"].apply(lambda x : x.astype(int).sum())
    df8 = df7.to_frame()
    df8
    # df8 has the actual volume for each time period
    
    df_oa = oa_volume(df_open)
    df_ca = ca_volume(df_close)
    df81 = df8.merge(df_oa, how='outer',left_index=True, right_index=True)
    df82 = df81.merge(df_ca, how='outer',left_index=True, right_index=True)
    df82['Total Volume'] = df82.iloc[:, :].sum(axis=1)
    df82 = df82.drop(df82.columns[1:], axis = 1)
    
    # transform volume amount into %
    df9 = df82.groupby(['Year', 'Month'])["Total Volume"].apply(lambda x : x.astype(int).sum())
    df10 = df9.to_frame()
    df10 = df10.rename({'Total Volume': 'TV of Month'}, axis = 1)
    df10
    df82["Volume"] = (df82['Total Volume'] / df10['TV of Month'].groupby(['Year', 'Month']).sum())*100 # change volume to percentage
    df11 = df82.drop("Total Volume", axis = 1)
    
    # Get the quarter
    df12 = df11.groupby(['Year', 'Month'])["Volume"].count()
    df13 = df12.to_frame()
    df13 = df13.rename({'Volume': 'quarter'}, axis = 1)
    
    df14 = df11.merge(df13, how='outer',left_index=True, right_index=True)
    df14['quarter'] = 1/df14['quarter']*100
    
    # Export to excel
    country = [key for key, value in exchange_dic.items() if value == index_name]
    df14.to_excel( country[0] + " Volume" + '.xlsx', index = True)



#Input file
xlsx = pd.ExcelFile('/Users/Hans/Desktop/Daiwa/Chartbook/Intraday Volume/Master_Intraday_Volume_April22.xlsx')

# function for output
def volume_export(country, country_open, country_close):
    ws_volume = pd.read_excel(xlsx, country)
    ws_open = pd.read_excel(xlsx, country_open)
    ws_close = pd.read_excel(xlsx, country_close)
    th_volume(ws_volume, ws_open, ws_close)


# Export output
country_list = ["Australia",'China', "Hong Kong", 'India', 'Indonesia', 
                'Japan', 'Korea', 'Malaysia', 'Philippines', 'Singapore',
                'Taiwan', 'Thailand']


for i in country_list:
    volume_export(i, i + "_open", i + "_close")









