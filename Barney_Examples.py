import krakenex

from pykrakenapi import pykrakenapi as pyk
from pykrakenapi import websocketapi as wapi

api = krakenex.API(key=key,
                   secret=secret)
kapi = pyk.KrakenAPI(api=api)

ws_client = wapi.WssClient(key=key, secret=secret)

# print(dir(pyk.KrakenAPI))

# -----------------------------------------------------------------------------
# list of api methods

# 1) execution

'''
add_standard_order
cancel_open_order
'''

# 2) data

'''
get_asset_info
get_ohlc_data
get_order_book
get_recent_spread_data
get_ticker_information
get_tradable_asset_pairs
get_trade_volume
'''

# 3) account data

'''
get_account_balance
get_closed_orders
get_ledgers_info
get_open_orders
get_open_positions
get_recent_trades
get_trade_balance
get_trades_history
'''

# 4) general

'''
get_server_time
'''


# -----------------------------------------------------------------------------
# websocket examples:

def my_handler(msg):
    # Here you can do stuff with the messages
    print(msg)


# ws_client.subscribe_public(
#     subscription={
#         #'name': 'trade'
#         'name': 'subscribe'
#     },
#     pair=['XRP/USD'],
#     callback=my_handler
# )


ws_client.subscribe_public(
    subscription={"name": "ticker", "event": "subscribe"},
    pair=["XBT/USD", "XBT/EUR"],
    # {"name": "ticker"},
    callback=my_handler)

ws_client.start()
# -----------------------------------------------------------------------------
# 1) execution examples
# 2) data examples
# 3) account data examples
# -----------------------------------------------------------------------------
