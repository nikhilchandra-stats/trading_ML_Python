import pandas
import numpy

#example.groupby('name')['number'].fillna(method='ffill')

def read_macro_data(url = 'https://raw.githubusercontent.com/nikhilchandra-stats/macrodatasetsraw/master/data/daily_fx_macro_data.csv', 
                    encoding_var = 'cp1252'):
    return pandas.read_csv(url, encoding=encoding_var)

def read_yahoo_finance(url = 'https://query1.finance.yahoo.com/v7/finance/download/AUDUSD=X?period1=1550448000&period2=1708214400&interval=1d&events=history&includeAdjustedClose=true', 
                       start_date = '2019-01-01',
                       end_date = '2024-02-16',
                       asset_symbol = 'AUDUSD'):
    
    if '.' in asset_symbol:
        symbol_string = asset_symbol + '?'
    else:
        symbol_string = asset_symbol + '=X?'         
    base_url = 'https://query1.finance.yahoo.com/v7/finance/download/'
    start_date_as_int = int(pandas.to_datetime(start_date).timestamp())
    start_date_string = 'period1=' + str(start_date_as_int) + '&'
    end_date_as_int = int(pandas.to_datetime(end_date).timestamp())
    end_date_string = 'period2=' + str(end_date_as_int) + '&interval=1d&events=history&includeAdjustedClose=true'
    complete_url = base_url + symbol_string + start_date_string + end_date_string
    returned_data = pandas.read_csv(complete_url) 
    return returned_data

# test = ['jj', 'S&P', 'RBA']
# final = [x for x in test if '(S&P|RBA)' in test]

# Build a data set that can be joined to the asset, This example looked at 2 macro features for 2 assets
# PMI for USD and RBA CPI for AUD

def join_asset_to_macro(macro_data = read_macro_data(),
                        macro_vars = ['S&P Global Manufacturing PMI', 'RBA Trimmed Mean CPI \(QoQ\)'],
                        symbol_vars = ['USD','AUD'],
                        asset_data = read_yahoo_finance(
                            start_date = '2019-01-01',
                            end_date = '2024-02-16',
                            asset_symbol = 'AUDUSD')
                        ):
    
    #Clean Input Variables
    macro_data_vars = "|".join(macro_vars)
    symbol_data_vars = "|".join(symbol_vars) 
    macro_data['event'] = macro_data['event'].str.replace('(?i)(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)','', regex=True)
    macro_dat_filt = macro_data[macro_data['event'].str.contains(macro_data_vars, regex=True)]
    macro_dat_filt2 =macro_dat_filt[macro_data['symbol'].str.contains(symbol_data_vars, regex=True)]

    asset_data['Date'] = pandas.to_datetime(asset_data['Date'])
    macro_dat_filt2['date'] = pandas.to_datetime(macro_dat_filt2['date'])
    asset_data = asset_data.rename(columns={'Date':'date'}) 
    
    for i in  range(len(symbol_vars)):
        temp_data = macro_dat_filt2[macro_dat_filt2['event'].str.contains(macro_vars[i])]
        temp_data2 = temp_data[temp_data['symbol'].str.contains(symbol_vars[i])]
        temp_data3 = temp_data2[['date','actual']]
        new_col_name = macro_vars[i] + " "+ symbol_vars[i]
        temp_data4 = temp_data3.rename(columns={'actual':new_col_name})
        # merge_asset_macro['actual'] = merge_asset_macro.groupby('symbol')['actual'].fillna(method = "ffill")
        asset_data = pandas.merge( asset_data, temp_data4 ,
                                    on="date", how = "left" )
        asset_data[new_col_name] = asset_data[new_col_name].fillna(method='ffill')
        
    return asset_data
    