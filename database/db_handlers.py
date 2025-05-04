from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, Date, ForeignKey, ARRAY, select, insert
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


def create_db_url(username, host, port, database, password=None):
    if password:
        return f"postgresql://{username}:{password}@{host}:{port}/{database}"
    return f"postgresql://{username}@{host}:{port}/{database}"

class DatabaseHandler:
    """
    Base class for handling database interactions using SQLAlchemy.

    This class initializes the database connection, metadata, and provides 
    common methods for creating tables and inserting data while ensuring 
    uniqueness based on primary keys.

    Subclasses should define specific tables and the correct insertion order.
    """
    def __init__(self, db_url):
        self.engine = create_engine(db_url)
        self.metadata = MetaData()
        self.insert_order = []  # Defined in subclasses
    
    def create_all_tables(self):
        """Creates all tables in the database."""
        self.metadata.create_all(self.engine)

    def insert_if_not_exists(self, table, values):
        """
        Inserts rows only if they do not already exist.
        Supports composite primary keys.
        """
        if not isinstance(values, list):
            values = [values]

        primary_keys = table.primary_key.columns.keys()

        with self.engine.connect() as conn: #Opening connection here
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
            stmt: An SQLAlchemy select statement.
            return_format: Format of the returned data - 'dicts' for list of dicts, 'df' for Pandas DataFrame (default).

        Returns:
            List of dictionaries if return_format='dicts', or a Pandas DataFrame if return_format='df'.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(stmt)

                if return_format == 'df':
                    return pd.DataFrame(result.fetchall(), columns=result.keys())
                elif return_format == 'dicts':
                    rows = result.mappings().all()  # Returns rows as dictionaries
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

    The correct table insertion order is set to prevent foreign key issues (and other).
    """
    def __init__(self, db_url):
        super().__init__(db_url)

        self.works = Table(
            'works', self.metadata,
            Column('work_id', String, primary_key=True),
            Column('doi', String),
            Column('work_title', String),
            Column('publication_year', Integer),
            Column('publication_date', Date), # Talvez criar uma tabela separada para publication date (date_id, day, month, year) e colocar o date_id como chave estrangeira aqui
            Column('work_type', String), # Msma coisa do publication_date
            Column('cited_by_count', Integer),
            Column('primary_source_id', String, ForeignKey('primary_source.source_id')),
            Column('is_oa', Boolean),
            Column('oa_status', String), # Msma coisa do publication_date
            Column('referenced_works_count', Integer),
            Column('indexed_in', ARRAY(String))  # Daria pra pegar a combinação de cada uma delas e fazer um id para cada uma (JUNK DIMENSION)
        )

        self.primary_source = Table(
            'primary_source', self.metadata,
            Column('source_id', String, primary_key=True),
            Column('source_name', String),
            Column('source_issn_l', String),
            Column('issn', ARRAY(String)), # Remover isso depois
            Column('is_oa', Boolean), # Remover
            Column('host_organization_id', String), # Remover
            Column('host_organization_name', String), # Remover
            Column('type', String) 
        )

        self.authorships = Table(
            'authorships', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('author_id', String, ForeignKey('authors.author_id'), primary_key=True),
            Column('author_position', String),
            Column('is_corresponding', Boolean),
            Column('institution_id', ARRAY(String)) 
        )

        self.authors = Table( #Remove this
            'authors', self.metadata,
            Column('author_id', String, primary_key=True),
            Column('author_name', String),
            Column('orcid', String)
        )

        self.institutions = Table(
            'institutions', self.metadata,
            Column('institution_id', String, primary_key=True),
            Column('institution_name', String),
            Column('ror', String),
            Column('type', String), #Manter?
            Column('country_code', String)
        )

        self.cited_by_year = Table(
            'cited_by_year', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('year', Integer, primary_key=True),
            Column('cited_count', Integer)
        )

        self.topics_by_work = Table(
            'topics_by_work', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('topic_id', String, ForeignKey('topics.topic_id'), primary_key=True),
            Column('score', Float)
        )

        self.topics = Table(
            'topics', self.metadata,
            Column('topic_id', String, primary_key=True),
            Column('topic_name', String),
            Column('subfield_id', String),
            Column('subfield_name', String),
            Column('field_id', String),
            Column('field_name', String),
            Column('domain_id', String),
            Column('domain_name', String)
        )

        # 🔥 Correct insertion order to prevent foreign key errors
        self.insert_order = [
            "primary_source", "authors", "institutions", "topics",
            "works", "authorships", "cited_by_year", "topics_by_work"
        ]



class PostGradDatabaseHandler(DatabaseHandler):
    """
    Handles PostGrad-specific database schema and operations.

    This subclass extends DatabaseHandler to define the tables related 
    to post graduation data, including works, authors, institutions, and topics.

    The correct table insertion order is set to prevent foreign key issues (and other).
    """
    def __init__(self, db_url):
        super().__init__(db_url)

        self.works = Table(
            'works', self.metadata,
            Column('work_id', String, primary_key=True),
            Column('doi', String),
            Column('work_title', String),
            Column('publication_year', Integer),
            Column('publication_date', Date), # Talvez criar uma tabela separada para publication date (date_id, day, month, year) e colocar o date_id como chave estrangeira aqui
            Column('work_type', String), # Msma coisa do publication_date
            Column('cited_by_count', Integer),
            Column('primary_source_id', String, ForeignKey('primary_source.source_id')),
            Column('is_oa', Boolean),
            Column('oa_status', String), # Msma coisa do publication_date
            Column('referenced_works_count', Integer),
            Column('indexed_in', ARRAY(String))  # Daria pra pegar a combinação de cada uma delas e fazer um id para cada uma (JUNK DIMENSION)
        )

        self.primary_source = Table(
            'primary_source', self.metadata,
            Column('source_id', String, primary_key=True),
            Column('source_name', String),
            Column('source_issn_l', String),
            Column('issn', ARRAY(String)), # Remover isso depois
            Column('is_oa', Boolean), # Remover
            Column('host_organization_id', String), # Remover
            Column('host_organization_name', String), # Remover
            Column('type', String) 
        )

        self.authorships = Table(
            'authorships', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('author_id', String, ForeignKey('authors.author_id'), primary_key=True),
            Column('author_position', String),
            Column('is_corresponding', Boolean),
            Column('institution_id', ARRAY(String)) 
        )

        self.authors = Table( #Remove this
            'authors', self.metadata,
            Column('author_id', String, primary_key=True),
            Column('author_name', String),
            Column('orcid', String)
        )

        self.institutions = Table(
            'institutions', self.metadata,
            Column('institution_id', String, primary_key=True),
            Column('institution_name', String),
            Column('ror', String),
            Column('type', String), #Manter?
            Column('country_code', String)
        )

        self.cited_by_year = Table(
            'cited_by_year', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('year', Integer, primary_key=True),
            Column('cited_count', Integer)
        )

        self.topics_by_work = Table(
            'topics_by_work', self.metadata,
            Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
            Column('topic_id', String, ForeignKey('topics.topic_id'), primary_key=True),
            Column('score', Float)
        )

        self.topics = Table(
            'topics', self.metadata,
            Column('topic_id', String, primary_key=True),
            Column('topic_name', String),
            Column('subfield_id', String),
            Column('subfield_name', String),
            Column('field_id', String),
            Column('field_name', String),
            Column('domain_id', String),
            Column('domain_name', String)
        )

        # 🔥 Correct insertion order to prevent foreign key errors
        self.insert_order = [
            "primary_source", "authors", "institutions", "topics",
            "works", "authorships", "cited_by_year", "topics_by_work"
        ]