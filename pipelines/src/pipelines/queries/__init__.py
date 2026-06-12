from importlib.resources import files


def load(name: str) -> str:
    """Return the contents of a .sql file from this directory.

    Usage:
        from pipelines.queries import load
        sql = load("my_query.sql")
    """
    return files(__name__).joinpath(name).read_text(encoding="utf-8")
