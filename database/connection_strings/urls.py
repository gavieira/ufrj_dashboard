def create_db_url(params: dict) -> str:
    """
    Automatically generates a PostgreSQL database URL from a dictionary of parameters.

    Args:
        params (dict): Dictionary with optional keys:
            - db_name (str): Name of the database (required).
            - username (str): Database username (required).
            - password (str, optional): Password for the username.
            - host (str, optional): Host address (default: 'localhost').
            - port (int, optional): Port number (default: 5432).

    Returns:
        str: A valid SQLAlchemy PostgreSQL connection URL.

    Raises:
        ValueError: If required keys are missing.
    """
    db_name = params.get("db_name")
    username = params.get("username")
    password = params.get("password")
    host = params.get("host", "localhost")
    port = params.get("port", 5432)

    if not db_name or not username:
        raise ValueError("Both 'db_name' and 'username' are required to build the database URL.")

    if password:
        return f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{db_name}"
    return f"postgresql+psycopg2://{username}@{host}:{port}/{db_name}"


###########################


##Add your postgresql db information here:

base_url_elements = {
    "username": "postgres",
    "password": "postgres"
} #No need for host and port, as we're using localhost and default port

openalex_url_elements = {**base_url_elements, 'db_name': 'openalex_db'}

admin_url_elements = {**base_url_elements, 'db_name': 'postgres'}


openalex_db_url = create_db_url(openalex_url_elements)
admin_db_url = create_db_url(admin_url_elements)

if __name__ == '__main__':
    print(openalex_db_url)
    print(admin_db_url)