from sqlalchemy import MetaData, Table, Column, Integer, String, ForeignKey, DateTime, ARRAY, Boolean, Float


def define_openalex_tables(metadata: MetaData) -> dict:
    """
    Defines OpenAlex tables and returns them in insertion order,
    which is critical for ensuring correct insert dependencies.
    Python 3.7+ preserves dict insertion order.
    """
    works = Table(
       'works', metadata,
       Column('work_id', String, primary_key=True),
       Column('doi', String),
       Column('work_title', String),
       Column('publication_year', Integer),
       Column('publication_date', DateTime),
       Column('work_type', String),
       Column('cited_by_count', Integer),
       Column('primary_source_id', String, ForeignKey('primary_source.source_id')),
       Column('is_oa', Boolean),
       Column('oa_status', String),
       Column('referenced_works_count', Integer),
       Column('indexed_in', ARRAY(String))
    )

    primary_source = Table(
        'primary_source', metadata,
        Column('source_id', String, primary_key=True),
        Column('source_name', String),
        Column('source_issn_l', String),
        Column('is_oa', Boolean), # Remover
        Column('host_organization_id', String), # Remover
        Column('host_organization_name', String), # Remover
        Column('issn', ARRAY(String)),
        Column('type', String)
    )

    authorships = Table(
        'authorships', metadata,
        Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
        Column('author_id', String, primary_key=True),
        Column('author_position', String),
        Column('is_corresponding', Boolean),
        Column('institution_id', ARRAY(String))
    )

    authors = Table(
        'authors', metadata,
        Column('author_id', String, primary_key=True),
        Column('author_name', String),
        Column('orcid', String)
    )

    institutions = Table(
        'institutions', metadata,
        Column('institution_id', String, primary_key=True),
        Column('institution_name', String),
        Column('ror', String),
        Column('type', String),
        Column('country_code', String)
    )

    cited_by_year = Table(
        'cited_by_year', metadata,
        Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
        Column('year', Integer, primary_key=True),
        Column('cited_count', Integer)
    )

    topics_by_work = Table(
        'topics_by_work', metadata,
        Column('work_id', String, ForeignKey('works.work_id'), primary_key=True),
        Column('topic_id', String, ForeignKey('topics.topic_id'), primary_key=True),
        Column('score', Float)
    )

    topics = Table(
        'topics', metadata,
        Column('topic_id', String, primary_key=True),
        Column('topic_name', String),
        Column('subfield_id', String),
        Column('subfield_name', String),
        Column('field_id', String),
        Column('field_name', String),
        Column('domain_id', String),
        Column('domain_name', String)
    )

    return {
        "primary_source": primary_source,
        "authors": authors,
        "institutions": institutions,
        "topics": topics,
        "works": works,
        "authorships": authorships,
        "cited_by_year": cited_by_year,
        "topics_by_work": topics_by_work
    } #Needs to be provided in the correct insertion order