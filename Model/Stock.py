class Stock:
    def __init__(self, kodeName, name, price):
        self.kodeName = kodeName
        self.name = name
        self.price = price

    # Convert the Person object to a dictionary
    def to_dict(self):
        return {
            "kodeName": self.kodeName,
            "name": self.name,
            "price": self.price
        }