from typing import Iterator, Any

class Country:
    alpha_2: str
    alpha_3: str
    name: str

class Countries:
    def __iter__(self) -> Iterator[Country]: ...
    def __getattr__(self, name: str) -> Any: ...

countries: Countries
