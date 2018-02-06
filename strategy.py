import validation as vd

class BaseStrategy(object):
    def __init__(self, name, cash, value, price, positions):
        self.name = name
        self.cash = cash
        self.value = value
        self.price = price
        self.positions = positions
        self.child = None


class Strategy(BaseStrategy):
    def __init__(self, name, cash, value, price, positions, product):
        BaseStrategy.__init__(name, cash, value, price, positions)
        self.product = product

    def update(self):
        pass

