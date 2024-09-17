from typing import List, Any, Optional

class Table:
    df: Any

def read_pdf(
    filepath: str,
    pages: str = "1",
    password: Optional[str] = None,
    flavor: str = "lattice",
    suppress_stdout: bool = False,
    layout_kwargs: Optional[dict[Any, Any]] = None,
    **kwargs: Any
) -> List[Table]: ...
