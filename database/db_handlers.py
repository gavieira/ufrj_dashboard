from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Boolean, Float, Date, ForeignKey, ARRAY, select, insert


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
            Column('publication_date', Date),
            Column('work_type', String),
            Column('cited_by_count', Integer),
            Column('primary_source_id', String, ForeignKey('primary_source.source_id')),
            Column('is_oa', Boolean),
            Column('oa_status', String),
            Column('referenced_works_count', Integer),
            Column('indexed_in', ARRAY(String))
        )

        self.primary_source = Table(
            'primary_source', self.metadata,
            Column('source_id', String, primary_key=True),
            Column('source_name', String),
            Column('source_issn_l', String),
            Column('issn', ARRAY(String)),
            Column('is_oa', Boolean),
            Column('host_organization_id', String),
            Column('host_organization_name', String),
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

        self.authors = Table(
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
            Column('type', String),
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