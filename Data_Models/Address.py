class Address:
    def __init__(self, city: str, state: str, zip_code: str, country: str = None):
        self.city: str = city
        self.state: str = state
        self.zip_code: str = zip_code
        self.country: str = country

    def __repr__(self):
        return self.city, self.state, self.zip_code, self.country
