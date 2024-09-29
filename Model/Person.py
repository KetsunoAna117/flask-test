class Person:
  def __init__(self, name, age):
    self.name = name
    self.age = age

    # Convert the Person object to a dictionary
    def to_dict(self):
        return {
            "name": self.name,
            "age": self.age
        }