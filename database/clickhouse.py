"""ClickHouse database connection module."""

from clickhouse_connect import get_client
from clickhouse_connect.driver.client import Client


def get_clickhouse_client(
    host: str = "localhost",
    port: int = 8123,
    username: str = "cheakf",
    password: str = "Swq8855830.",
    database: str = "default",
) -> Client:
    """Get a ClickHouse client connection.
    
    Args:
        host: ClickHouse server host, default is localhost.
        port: ClickHouse HTTP port, default is 8123.
        username: ClickHouse username.
        password: ClickHouse password.
        database: Default database to use.
    
    Returns:
        Client: ClickHouse client instance.
    """
    client = get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )
    return client


def test_connection() -> bool:
    """Test the ClickHouse connection.
    
    Returns:
        bool: True if connection is successful, False otherwise.
    """
    try:
        client = get_clickhouse_client()
        result = client.command("SELECT version()")
        print(f"ClickHouse version: {result}")
        print("Connection successful!")
        return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False


if __name__ == "__main__":
    test_connection()
