import time
import ccxt
import config
import pandas as pd
import numpy as np

exchange = ccxt.delta({
    'apiKey': config.api_key,
    'secret': config.secret,
})
exchange.urls['api']['public'] = 'https://testnet-api.delta.exchange'
exchange.urls['api']['private']= 'https://testnet-api.delta.exchange'

# print(exchange.fetch_ohlcv("BTCUSDT",params={'urls':'test'}))

def ask_bid(symbol):
    ob = exchange.fetch_order_book(symbol=symbol)
    bid = ob['bids'][0][0]
    ask = ob['asks'][0][0]
    # print(bid," ",ask)
    return ask,bid

# ask_bid('BTCUSDT')

def df_sma(symbol,timeframe,num_bars,sma):
    print("starting calculations")
    bars = exchange.fetch_ohlcv(symbol=symbol,timeframe=timeframe,limit=num_bars)
    df_d = pd.DataFrame(bars,columns=['timestamp','open','high','low','close','volume'])
    df_d['timestamp'] = pd.to_datetime(df_d['timestamp'],unit ='ms')
    df_d['timestamp'] = df_d['timestamp'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata')
    df_d[f'sma{sma}_{timeframe}'] = df_d.close.rolling(sma).mean()
    bid = ask_bid(symbol)[1]
    df_d.loc[df_d[f'sma{sma}_{timeframe}'] > bid, 'sig'] = 'SELL'
    df_d.loc[df_d[f'sma{sma}_{timeframe}'] < bid, 'sig'] = 'BUY'

    print(df_d)
    return df_d

# df_sma('BTCUSDT','30m',200,9)


def open_position(index = 0):
    params = {'path':'https://testnet-api.delta.exchange','type':'swap','code':'USDT'}
    # print(f"Request URL: {exchange.urls['private']}/v2/wallet/balances?type=swap&code=USDT")
    del_bal = exchange.fetch_positions(params=params)
    open_position = del_bal[index]['info']
    openpos_size = int(open_position['size'])

    if openpos_size > 0:
        openpos_bool = True
        long = True
    elif openpos_size < 0:
        openpos_bool = True
        long = False
    else:
        openpos_bool = False
        long = False

    return open_position, openpos_bool, openpos_size, long

# def kill_switch():
#     print('Starting the kill switch')
#     positions = open_positions()
#     openpos = positions[1] # true or false
#     long = positions[3]
#     kill_size = positions[2]
#
#     print(f'Open Positions {openpos}, long {long}, size = {kill_size}')
#     while openpos == True:
#         print("Starting kill switch loop untill all dead..")
#         temp_df = pd.DataFrame()
#         positions = open_positions()
#         openpos = positions[1]  # true or false
#         long = positions[3]
#         kill_size = positions[2]
#         kill_size = int(kill_size)
#         symbol = f'{positions[0]["product"]["spot_index"]["config"]["underlying_asset"]}{positions[0]["product"]["spot_index"]["config"]["quoting_asset"]}'
#         print(symbol)
#         ask = ask_bid(symbol)[0]
#         bid = ask_bid(symbol)[1]
#
#         if long == False:
#             print(f'just made a buy to close order of {kill_size} {symbol}')
#             exchange.close_all_positions()
#             print('waiting to fill')
#             time.sleep(30)
#         elif long == True:
#             print(f'just made a sell to close order of {kill_size} {symbol}')
#             exchange.close_all_positions()
#             print('waiting to fill')
#             time.sleep(30)
#         else:
#             print('++++++++ERRORR KILLING')
# # print(df_sma(symbol="BTCUSDT", timeframe="30m", num_bars=200, sma=40))
def kill_switch():
    while True:
        positions_response, is_position_open, open_position_size, is_long = open_position()
        print(positions_response,is_position_open,open_position_size,is_long)
        if is_position_open:
            symbol = f'{positions_response["product"]["spot_index"]["config"]["underlying_asset"]}{positions_response["product"]["spot_index"]["config"]["quoting_asset"]}'  # Assuming symbol is directly accessible
            quantity = abs(int(open_position_size))  # Ensure positive quantity
            side = 'buy' if not is_long else 'sell'  # Determine order side based on position direction

            try:
                order = exchange.create_order(symbol, 'market', side, quantity)
                print(f"Kill switch activated: Placed {side} order for {quantity} {symbol}")
            except Exception as e:
                print(f"Error placing order: {e}")

        time.sleep(30)  # Check periodically


def ob(symbol='ETHUSDT'):
    print('Fetching order book data...')
    df = pd.DataFrame()
    temp_df = pd.DataFrame()
    ob = exchange.fetch_order_book(symbol=symbol)
    print(ob)
    bids = ob['bids']
    ask = ob['asks']
    first_bids = bids[0]
    first_ask = ask[0]
    bid_vol_list = []
    ask_vol_list = []

    for x in range(11):
        for set in bids:
            price = set[0]
            volume = set[1]
            bid_vol_list.append(volume)
            sum_bid_vol = sum(bid_vol_list)
            temp_df['bid_vol'] = [sum_bid_vol]

        for set in ask:
            price = set[0]
            volume = set[1]
            ask_vol_list.append(volume)
            sum_ask_vol = sum(ask_vol_list)
            temp_df['ask_vol'] = [sum_ask_vol]

        time.sleep(5)
        df = pd.concat([df, temp_df], ignore_index=True)
        print(df)
        print(' ')
        print('------')
        print(' ')
    print("collected all volume data")
    print("calculation sums...")
    total_bidvol = df['bid_vol'].sum()
    total_askvol = df['ask_vol'].sum()
    print(f'Last 1 min this is the total Bid volume: {total_bidvol} | ask_vol: {total_askvol}')

    if total_bidvol > total_askvol:
        control = (total_askvol/total_bidvol)
        print(f"Bulls are in control by {control}")
        bullish = True
    else:
        control = (total_bidvol / total_askvol)
        print(f"Bears are in control by {control}")
        bullish = False

ob()