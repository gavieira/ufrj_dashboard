import json

class OpenAlexWorkParser:
    def __init__(self, json_data):
        self.json_data = json_data
        self._source = ((json_data.get("primary_location") or {}).get("source") or {}) #Deeply nested, and the 'source' object can be None, leading to errors. So we deal with it here.

    def _clean_id(self, id_str):
        """
        Clean the URL from several IDs (OpenAlex, ROR, ORCID) by splitting on '/' and taking the last part.
        Returns None if the input is None or empty.
        """
        if not id_str:
            return None #Deals with empty non-existing ids

        id_str = id_str.rstrip('/') #removing eventual trailing slashes

        if 'doi.org/' in id_str.lower():
            return id_str.split('doi.org/')[-1] #Deals with doi id

        return id_str.split('/')[-1] #Deals with other ids


    def get_table_works(self):
        """Extract and format data for the 'works' table."""
        work = self.json_data
        return {
            "work_id": self._clean_id(work.get("id")),
            "doi": self._clean_id(work.get("doi")),
            "work_title": work.get("title"),
            "publication_year": work.get("publication_year"),
            "publication_date": work.get("publication_date"),
            "work_type": work.get("type"),
            "cited_by_count": work.get("cited_by_count"),
            "primary_source_id": self._clean_id(self._source.get("id")),
            "is_oa": work.get("open_access", {}).get("is_oa"),
            "oa_status": work.get("open_access", {}).get("oa_status"),
            "referenced_works_count": work.get("referenced_works_count"),
            "indexed_in": work.get("indexed_in", [])
        }

    def get_table_primary_source(self):
        """Extract and format data for the 'primary_source' table."""
        source = self._source
        return {
            "source_id": self._clean_id(source.get("id")),
            "source_name": source.get("display_name"),
            "source_issn_l": source.get("issn_l"),
            "issn": source.get("issn", []),
            "is_oa": source.get("is_oa"),
            "host_organization_id": self._clean_id(source.get("host_organization")),
            "host_organization_name": source.get("host_organization_name"),
            "type": source.get("type")
        }

    def get_table_authorships(self):
        """Extract and format data for the 'authorships' table."""
        authorships = []
        for authorship in self.json_data.get("authorships", []):
            authorships.append({
                "work_id": self._clean_id(self.json_data.get("id")),
                "author_id": self._clean_id(authorship.get("author", {}).get("id")),
                "author_position": authorship.get("author_position"),
                "is_corresponding": authorship.get("is_corresponding"),
                "institution_id": [ self._clean_id(institution.get("id")) for institution in authorship.get("institutions", []) ],
            })
        return authorships

    def get_table_authors(self):
        """Extract and format data for the 'authors' table."""
        authors = []
        for authorship in self.json_data.get("authorships", []):
            author = authorship.get("author", {})
            authors.append({
                "author_id": self._clean_id(author.get("id")),
                "author_name": author.get("display_name"),
                "orcid": self._clean_id(author.get("orcid"))
            })
        return authors

    def get_table_institutions(self):
        """Extract and format data for the 'institutions' table."""
        institutions = []
        for authorship in self.json_data.get("authorships", []):
            for institution in authorship.get("institutions", []):
                institutions.append({
                    "institution_id": self._clean_id(institution.get("id")),
                    "institution_name": institution.get("display_name"),
                    "ror": self._clean_id(institution.get("ror")),
                    "type": institution.get("type"),
                    "country_code": institution.get("country_code")
                })
        return institutions

    def get_table_cited_by_year(self):
        """Extract and format data for the 'cited_by_year' table."""
        cited_by_year = []
        for count in self.json_data.get("counts_by_year", []):
            cited_by_year.append({
                "work_id": self._clean_id(self.json_data.get("id")),
                "year": count.get("year"),
                "cited_count": count.get("cited_by_count")
            })
        return cited_by_year

    def get_table_topics_by_work(self):
        """Extract and format data for the 'topics_by_work' table."""
        topics_by_work = []
        for topic in self.json_data.get("topics", []):
            topics_by_work.append({
                "work_id": self._clean_id(self.json_data.get("id")),
                "topic_id": self._clean_id(topic.get("id")),
                "score": topic.get("score")
            })
        return topics_by_work

    def get_table_topics(self):
        """Extract and format data for the 'topics' table."""
        topics = []
        for topic in self.json_data.get("topics", []):
            topics.append({
                "topic_id": self._clean_id(topic.get("id")),
                "topic_name": topic.get("display_name"),
                "subfield_id": self._clean_id(topic.get("subfield", {}).get("id")),
                "subfield_name": topic.get("subfield", {}).get("display_name"),
                "field_id": self._clean_id(topic.get("field", {}).get("id")),
                "field_name": topic.get("field", {}).get("display_name"),
                "domain_id": self._clean_id(topic.get("domain", {}).get("id")),
                "domain_name": topic.get("domain", {}).get("display_name")
            })
        return topics


    def get_all_tables(self):
        """
        Returns a dictionary containing all parsed tables, maintaining a specific order for insertion
        into databases without errors regarding foreign keys and such.

        :return: Dictionary where keys are table names and values are the extracted records.
        """
        return {
            "primary_source": self.get_table_primary_source(),
            "authors": self.get_table_authors(),
            "institutions": self.get_table_institutions(),
            "topics": self.get_table_topics(),
            "works": self.get_table_works(),
            "authorships": self.get_table_authorships(),
            "cited_by_year": self.get_table_cited_by_year(),
            "topics_by_work": self.get_table_topics_by_work(),
        }