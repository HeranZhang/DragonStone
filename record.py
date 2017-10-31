import gdax
import time
from datetime import datetime

product_id = 'BTC-USD'
record_time = 36000 # seconds
order_book_data_file = 'order_book_data.pickle'
trade_data_file = 'trade_data.pickle'
log_report = 'log_reprot.txt'

# Clear file contents
open(order_book_data_file, 'wb').close()
open(trade_data_file, 'wb').close()
log_order_book_to = open(order_book_data_file, 'wb')
log_trade_to = open(trade_data_file, 'wb')
n_levels = 5

order_book = gdax.OrderBook(product_id, log_order_book_to, log_trade_to, n_levels)

with open(log_report, 'w') as f:
    f.write(str(datetime.now()))
    f.write(' Data record start.\n')

order_book.start()
time.sleep(record_time)
order_book.close()
log_order_book_to.close()
log_trade_to.close()

with open(log_report, 'a') as f:
    f.write(str(datetime.now()))
    f.write(' Data record end. ')
