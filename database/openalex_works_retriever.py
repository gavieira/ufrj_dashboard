import json
import os
import requests
from datetime import datetime
from sqlalchemy import create_engine, insert, select
from openalex_work_parser import OpenAlexWorkParser
from db_handlers import OpenAlexDatabaseHandler


class OpenAlexWorksRetriever:
    def __init__(self):
        """
        Initialize the OpenAlexWorksRetriever with the base OpenAlex API URL.
        """
        self.base_url = "https://api.openalex.org/works"


    def process_and_store_page(self, results, jsonl_filename=None, db_handler=None):
        """
        Process a page of results, saving them to a JSONL file and/or inserting them into a database.

        :param results: List of results from OpenAlex API.
        :param jsonl_filename: Filename for JSONL storage (if provided).
        :param db_handler: OpenAlexDatabaseHandler instance for direct database insertion (if provided).
        """
        if jsonl_filename:
            with open(jsonl_filename, "a", encoding="utf-8") as f:
                for record in results:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")

        if db_handler:
            for record in results:
                parser = OpenAlexWorkParser(record)
                tables = parser.get_all_tables()

                for table_name in db_handler.insert_order:
                    db_handler.insert_if_not_exists(getattr(db_handler, table_name), tables.get(table_name))

    def retrieve_works(self, ror, jsonl_filename=None, db_handler=None, email=None, start_year=None, end_year=None, per_page=200, max_pages=None):
        """
        Retrieve works from OpenAlex API and process each result page immediately, either sending the records to a .jsonl file
        (if jsonl_filename parameter is provided), to a database (if db_handler parameter is provided), or both (if both parameters 
        are passed down to this method).

        :param ror: ROR ID of the institution to filter works.
        :param jsonl_filename: Filename to save results incrementally as JSONL (required if db_handler is not provided).
        :param db_handler: OpenAlexDatabaseHandler object - used to store results directly into db (required if jsonl_filename is not provided).
        :param email: Email to be used in the API request (recommended by OpenAlex).
        :param start_year: Start year of the publication time window (optional).
        :param end_year: End year of the publication time window (optional).
        :param per_page: Number of results per page (default: 200, max: 200).
        :param max_pages: Maximum number of pages to retrieve (default: None, retrieves all pages).
        :raises ValueError: If neither jsonl_filename nor db_handler is provided.
        :raises FileExistsError: If the specified JSONL file already exists.
        """
        if not jsonl_filename and not db_handler:
            raise ValueError("At least one of 'jsonl_filename' or 'db_handler' must be provided to store the results.")

        if jsonl_filename and os.path.exists(jsonl_filename):
            raise FileExistsError(f"The file '{jsonl_filename}' already exists. Please provide a different filename.")

        current_year = datetime.now().year

        # Validate year inputs
        if start_year and start_year > current_year:
            raise ValueError(f"Start year {start_year} cannot be greater than the current year {current_year}.")
        if end_year and end_year > current_year:
            raise ValueError(f"End year {end_year} cannot be greater than the current year {current_year}.")
        if start_year and end_year and start_year > end_year:
            raise ValueError(f"Start year {start_year} cannot be greater than end year {end_year}.")

        if per_page > 200:
            raise ValueError("The per_page parameter cannot be greater than 200.")

        # Build the filter query
        filters = f"institutions.ror:{ror}"
        if start_year and end_year:
            filters += f",publication_year:{start_year}-{end_year}"
        elif start_year:
            filters += f",publication_year:>={start_year}"
        elif end_year:
            filters += f",publication_year:<={end_year}"

        cursor = "*"
        count = 0

        while cursor:
            if max_pages is not None and count >= max_pages:
                break

            url = f"{self.base_url}?filter={filters}&per-page={per_page}&cursor={cursor}"
            if email:
                url += f"&mailto={email}"
            print(f"\nFetching data from: {url}")

            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

            page_with_results = response.json()
            results = page_with_results.get("results", [])
            print(f"Retrieved {len(results)} results.")

            # Process and store results immediately
            self.process_and_store_page(results, jsonl_filename, db_handler)

            cursor = page_with_results.get("meta", {}).get("next_cursor")
            count += 1


# Example usage
if __name__ == "__main__":
    retriever = OpenAlexWorksRetriever()

    ror = "03490as77"
    jsonl_filename = "/config/workspace/openalex_data/openalex_ufrj.jsonl"
    #jsonl_filename = "~/openalex_ufrj.jsonl"
    #jsonl_filename = "openalex_ufrj.jsonl"
    db_url = "postgresql+psycopg2://gid_admin:dashboard@postgres:5432/gid_admin"
    db_handler = OpenAlexDatabaseHandler(db_url)

    retriever.retrieve_works(ror=ror, jsonl_filename=jsonl_filename, db_handler=db_handler, email="gabriel.vieira@bioqmed.ufrj.br", per_page=200)