import logging
from kiteconnect import KiteTicker
from kiteconnect import KiteConnect


def place_order(symbols, strike, buy_sell):
    logging.basicConfig(level=logging.DEBUG)

    # Initialise

    from configparser import ConfigParser
    parser = ConfigParser()
    parser1 = ConfigParser()

    filename = 'database.ini'
    section = 'zerodha'
    # read config file
    parser.read(filename)
    parser1.read('trades.ini')

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]



    kite = KiteConnect(api_key=db['api_key'])

    # data = kite.generate_session(request_token, api_secret=api_secret)
    kite.set_access_token(db['access_token'])
    
    for symbol,bs in zip(symbols, buy_sell):
        if(bs == 'sell'):
            order_id = kite.place_order(tradingsymbol=symbol,
                                        price = 1500,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_SELL,
                                            quantity=25,
                                            variety=kite.VARIETY_AMO,
                                            order_type=kite.ORDER_TYPE_LIMIT,
                                            product=kite.PRODUCT_MIS)
        else:
            order_id = kite.place_order(tradingsymbol=symbol,
                                        price = 1500,
                                            exchange=kite.EXCHANGE_NFO,
                                            transaction_type=kite.TRANSACTION_TYPE_BUY,
                                            quantity=25,
                                            variety=kite.VARIETY_AMO,
                                            order_type=kite.ORDER_TYPE_LIMIT,
                                            product=kite.PRODUCT_MIS)
    
        order = kite.order_history(order_id = order_id )
        size = len(order)
        
        trade_count = 0
        try:
            trade_count = parser1.getint('orders', 'trade_count')
        except:
            pass
        
        print(order[size - 1]['status'])
        if(order[size - 1]['status'] == 'AMO REQ RECEIVED' and buy_sell[0] == 'sell'):
            parser1.set('orders','price_' + symbol[-2:], str(order[size -1]['price']))
            parser1.set('orders','order_id_' + symbol[-2:], str(order_id))
            parser1.set('orders', 'strike', str(strike))
            parser1.set('orders', 'trade_count', str(trade_count+1))
                
        with open('trades.ini', 'w') as configfile:
            parser1.write(configfile)
            
        print(order_id)
    
    return strike, trade_count