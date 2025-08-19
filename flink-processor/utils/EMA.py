class EMA:
    def __init__(self, j, last):
        self.j = j
        self.close = last
        self.alpha = 2 / (j + 1)

    def calculate(self, prev=None):
        if prev is None:
            return self.close * self.alpha
        return self.close * self.alpha + prev * (1 - self.alpha)