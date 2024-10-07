class StockDTO:
    def __init__(self, name: str, price: str):
        self.name = name
        self.price = price

    # Convert the Person object to a dictionary
    def to_dict(self):
        return {
            "name": self.name,
            "price": self.price
        }