from abc import ABC, abstractmethod
from sqlalchemy import create_engine, MetaData, select, insert
import sqlalchemy
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from schemas.openalex import define_openalex_tables


def create_db_url(db_name, username, password=None, host="localhost", port=5432):
    """
    Automatically generates the database URL based on the given parameters.

    Args:
        db_name (str): The name of the database.
        username (str): The username to connect to the database.
        password (str, optional): The password for the username. Default is None.
        host (str, optional): The host of the database. Default is 'localhost'.
        port (int, optional): The port of the database. Default is 5432.

    Returns:
        str: The complete database URL.
    """
    if password:
        return f"postgresql://{username}:{password}@{host}:{port}/{db_name}"
    return f"postgresql://{username}@{host}:{port}/{db_name}"

def replace_db_name_in_url(db_url, new_db_name):
    """
    Replaces the database name in an existing db_url with a new one using string methods.

    Args:
        db_url (str): The existing database connection URL.
        new_db_name (str): The new database name to replace the old one.

    Returns:
        str: The modified db_url with the new database name.
    """
    # Find the position of the last '/' in the URL
    last_slash_index = db_url.rfind('/')
    
    # Remove everything after the last '/' and append the new database name
    new_db_url = db_url[:last_slash_index + 1] + new_db_name
    
    return new_db_url


class DatabaseHandler(ABC):
    """
    Abstract base class for managing PostgreSQL databases with SQLAlchemy.

    This class sets up the engine, metadata, and provides shared functionality 
    such as conditional inserts and query execution. Subclasses must define 
    their own tables and insertion order by implementing the `define_tables` method.

    Args:
        db_url (str): SQLAlchemy URL for the target database.
        create_if_missing (bool): Whether to create the database if it doesn't exist.
        admin_db_url (str): SQLAlchemy URL for a superuser connection (needed if creating databases).
        db_name (str): The name of the target database to create if missing.

    Attributes:
        engine: SQLAlchemy engine for the target database.
        metadata: SQLAlchemy MetaData instance for table management.
        insert_order (list): Optional list of table names to enforce insert order.
    """

    def __init__(self, db_url, create_target_db=False, admin_db_url=None):
        self.db_url = db_url
        self.db_name = db_url.rsplit('/', 1)[1] #Gets db_name from tdb_url (string after last "/")
        self.metadata = MetaData()
        self.insert_order = []

        if create_target_db and admin_db_url:
            self._create_target_database(admin_db_url)

        self.engine = create_engine(self.db_url)
        self.define_tables()
        self.metadata.create_all(self.engine)

    def _create_target_database(self, admin_db_url):
        """Creates the target database if it doesn't exist using a superuser connection."""
        try:
            admin_engine = create_engine(admin_db_url)
            with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                existing = conn.execute(
                    sqlalchemy.text("SELECT 1 FROM pg_database WHERE datname = :name"),
                    {"name": self.db_name}
                ).fetchone()
                if not existing:
                    conn.execute(sqlalchemy.text(f"CREATE DATABASE {self.db_name}"))
                    print(f"Database '{self.db_name}' created.")
                else:
                    print(f"Database '{self.db_name}' already exists.")
        except SQLAlchemyError as e:
            print(f"Error checking/creating database '{self.db_name}': {e}")

    @abstractmethod
    def define_tables(self):
        """
        Abstract method to define tables.
        Subclasses must override this method to create their specific tables as self attributes.
        """
        pass

    def create_all_tables(self):
        """Creates all tables defined in metadata."""
        self.metadata.create_all(self.engine)

    def insert_if_not_exists(self, table, values):
        """
        Inserts rows only if they do not already exist.
        Supports composite primary keys.
        """
        if not isinstance(values, list):
            values = [values]

        primary_keys = table.primary_key.columns.keys()

        with self.engine.connect() as conn:
            for value in values:
                if any(value.get(key) is None for key in primary_keys):
                    print(f"Skipping insert: Missing primary key(s) in {value}")
                    continue

                where_condition = [table.c[key] == value[key] for key in primary_keys]
                stmt = select(table).where(*where_condition)
                result = conn.execute(stmt).fetchone()

                if not result:
                    insert_stmt = insert(table).values(**value)
                    conn.execute(insert_stmt)
                    conn.commit()
                    print(f"Inserted: {value}")
                else:
                    print(f"Row already exists: {value}")

    def get_query_results(self, stmt, return_format='df'):
        """
        Executes a given SQLAlchemy statement and retrieves results in the desired format.

        Args:
            stmt: An SQLAlchemy select statement or SQL query string
            return_format: Format of the returned data - 'dicts' for list of dicts, 'df' for Pandas DataFrame (default).

        Returns:
            List of dictionaries if return_format='dicts', or a Pandas DataFrame if return_format='df'.
        """
        if isinstance(stmt, str): 
            stmt = sqlalchemy.text(stmt)
        try:
            with self.engine.connect() as conn:
                result = conn.execute(stmt)

                if return_format == 'df':
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                elif return_format == 'dicts':
                    rows = result.mappings().all()
                    return [dict(row) for row in rows]
                else:
                    raise ValueError("Invalid return_format. Choose 'dicts' or 'df'.")
        except SQLAlchemyError as e:
            print(f"Error fetching records: {e}")
            return pd.DataFrame() if return_format == 'df' else []

class OpenAlexDatabaseHandler(DatabaseHandler):
    """
    Handles OpenAlex-specific database schema and operations.

    This subclass extends DatabaseHandler to define the tables related 
    to OpenAlex data, including works, authors, institutions, and topics.

    The correct table insertion order is set to prevent foreign key errors.
    """

    def define_tables(self):
        """
        Define tables and insert order specific to the OpenAlex database schema.
        Each table is defined as a self attribute so they can be used throughout the class.
        """
        self.tables = define_openalex_tables(self.metadata)
        for name, table in self.tables.items():
            setattr(self, name, table)

        # Define insert order
        self.insert_order = list(self.tables.keys())
        #self.delete_order = list(reversed(self.insert_order)) #Only useful if delete method is added to superclass