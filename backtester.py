import sys
import constants
import validation as vd
import copy
import pandas as pd
from strategy import *


class Backtest(object):
    def __init__(self, strategy, data,
                 initial_capital=constants.DEFAULT_INITIAL_CAPITAL,
                 transaction_fee=constants.DEFAULT_TRANSACTION_FEE):
        vd.validate_initial_data(data)
        self.strategy = copy.deepcopy(strategy)
        self.data = data
        self.initial_capital = initial_capital
        self.transaction_fee = transaction_fee

    def run(self):
        pass


def getopts(argv):
    opts = {}  # Empty dictionary to store key-value pairs.
    while argv:  # While there are arguments left to parse...
        if argv[0][0] == '-':  # Found a "-name value" pair.
            opts[argv[0]] = argv[1]  # Add key and value to the dictionary.
        argv = argv[1:]  # Reduce the argument list by copying it starting from index 1.
    return opts


if __name__ == '__main__':
    data = pd.read_csv(sys.argv[1])

