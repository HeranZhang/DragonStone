import pickle as pkl
import pandas as pd
import numpy as np
from pandas import DataFrame
import datetime
import os

product_id = 'BTC-USD'
order_book_data_file = 'order_book_data.pickle'
trade_data_file = 'trade_data.pickle'
log_file = 'parser_log.log'
h5_store = 'store.h5'
n_levels = 5

message_log_order_book = []
message_log_trades = []
order_book = []
trades = []
bid_ask_price_size_level_column_name = []
current_sequence = -1
store = pd.HDFStore(h5_store)

for i in range(n_levels):
    bid_ask_price_size_level_column_name += [
        'bid_price_level_' + str(i+1),
        'bid_size_level_' + str(i+1),
        'ask_price_level_' + str(i+1),
        'ask_size_level_' + str(i+1),
    ]

order_book_column_names = [
    'timestamp',
    'msg_sequence',
    'product_id'
] + bid_ask_price_size_level_column_name

message_keys = [
    'bids',
    'asks',
    'time',
    'sequence',
    'product_id',
]

trades_column_names = [
    'timestamp',
    'sequence',
    'product_id',
    'price',
    'size',
    'trade_id',
    'side',
]


def clear_log():
    os.remove(log_file)


def clear_h5_store():
    os.remove(h5_store)


def on_start():
    clear_log()
    clear_h5_store()


def log_debug(message):
    debug = '[Debug]'
    timestamp = str(datetime.datetime.now())
    with open(log_file, 'a') as f:
        f.write('{} {} {}\n'.format(debug, timestamp, message))


def log_error(message):
    error = '[Error]'
    timestamp = str(datetime.datetime.now())
    with open(log_file, 'a') as f:
        f.write('{} {} {}\n'.format(error, timestamp, message))


def validate_message_key(message):
    if not all(key_name in message for key_name in message_keys):
        log_error('Message content missing')


def validate_message_length(message):
    if not all(len(message[key_name]) == n_levels for key_name in message_keys[:2]):
        log_error('Message length mismatch.')


def validate_message_sequence(message):
    global current_sequence
    message_sequence = message['sequence']
    if current_sequence == -1:
        current_sequence = message_sequence
    elif message_sequence > current_sequence + 1:
        log_error('Message missing ({} - {}).'.format(message_sequence, current_sequence))
    current_sequence = message_sequence


def validate(message):
    validate_message_key(message)
    validate_message_length(message)
    validate_message_sequence(message)


def process_order_book(message):
    message_log_order_book.append(message)
    order_book.append(
        np.append(
            np.asarray([
                datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),
                message['sequence'],
                message['product_id'],]),
            transform(message)
        )
    )


def process_trades(message):
    message_log_trades.append(message)
    trades.append(
        np.asarray([
            datetime.datetime.strptime(message['time'], "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),
            int(message['sequence']),
            str(message['product_id']),
            np.float(message['price']),
            np.float(message['size']),
            str(message['trade_id']),
            str(message['side']),
        ])
    )



def transform(message):
    bids = message['bids']
    asks = message['asks']
    current_book = []
    for i in range(n_levels):
        current_book += [bids[i][0], bids[i][1], asks[i][0], asks[i][1]]
    return np.asarray(current_book, dtype=np.float32)


def pickle_loader(pklFile):
    try:
        while True:
            yield pkl.load(pklFile)
    except EOFError:
        pass


on_start()


with open(order_book_data_file, 'rb') as f:
    log_debug('Opening {}'.format(order_book_data_file))
    log_debug('Processing order book records.')
    for message in pickle_loader(f):
        validate(message)
        process_order_book(message)
    log_debug('Closing {}'.format(order_book_data_file))

order_book = np.array(order_book)
order_book_df = DataFrame.from_records(order_book, columns=order_book_column_names)
log_debug('Parsed {} order book records'.format(str(len(order_book))))

with open(trade_data_file, 'rb') as f:
    log_debug('Opening {}'.format(trade_data_file))
    log_debug('Processing trades records.')
    for message in pickle_loader(f):
        process_trades(message)
    log_debug('Closing {}'.format(trade_data_file))

trades = np.array(trades)
trades_df = DataFrame.from_records(trades, columns=trades_column_names)
log_debug('Parsed {} trades records'.format(str(len(trades))))

order_book_df.to_hdf(h5_store, 'order_book')
log_debug('Order book reocrds saved to HDF5 Store.')
trades_df.to_hdf(h5_store, 'trades')
log_debug('Trades reocrds saved to HDF5 Store.')
