class Stock:
    def __init__(self, name, price):
        self.name = name
        self.price = price

    # Convert the Person object to a dictionary
    def to_dict(self):
        return {
            "name": self.name,
            "price": self.price
        }