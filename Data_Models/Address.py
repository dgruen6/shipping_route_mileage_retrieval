from typing import Optional


class Address:
    def __init__(self, city: str, state: str, zip_code: str, country: str = None):
        self.city: str = city
        self.state: str = state
        self.zip_code: str = zip_code
        self.zip_code_modified: Optional[str] = None
        self.country: str = country

    def __repr__(self):
        return f"{self.city}, {self.state}, {self.zip_code}, {self.country}"
