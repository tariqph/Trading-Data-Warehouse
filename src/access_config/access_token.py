from kiteconnect import KiteConnect
from configparser import ConfigParser
parser = ConfigParser()

def access_token():
    '''
    This function sets the access token generated for the day and returns a kiteconnect object 
    '''
    filename = 'database.ini'
    section = 'zerodha'
    # read config file
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]

    kite = KiteConnect(api_key=db['api_key'])

    # data = kite.generate_session(request_token, api_secret=api_secret)
    kite.set_access_token(db['access_token'])
    
    return kite
 
