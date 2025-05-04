from db_handlers import *
from openalex_works_retriever import OpenAlexWorksRetriever

if __name__ == "__main__":

    ##Setting variables (needed for retrieval of openalex API data via OpenAlexWorksRetriever 'retrieve_works' method)
    ror = "03490as77" #Institution ROR
    #jsonl_filename = "/config/workspace/openalex_data/openalex_ufrj.jsonl" #Jsonl location (optional if data is being saved to database)
    admin_db_url = "postgresql+psycopg2://gid_admin:dashboard@localhost:5432/gid_admin" #db_url (needed for the creation of the db_handler object - optional if data is being saved to jsonl file)
#db_url = "postgresql+psycopg2://gid_admin:dashboard@127.0.0.1:5432/gid_admin"


    #Creating db_handler object (also needed for retrieval of OpenAlex data)
    db_handler = OpenAlexDatabaseHandler(
                                         db_url= replace_db_name_in_url(admin_db_url, 'openalex_db'),
                                         create_target_db=True,
                                         admin_db_url = admin_db_url
                                        ) #Initialize db_handler object

    #OpenAlexWorksRetriever and retrieval of data via OpenAlex's API
    retriever = OpenAlexWorksRetriever() #Initializes the retriever object to get data from OpenAlex
    retriever.retrieve_works(ror=ror, 
                            db_handler=db_handler, 
                            email="gabriel.vieira@bioqmed.ufrj.br", 
                            per_page=200) #Method to retrieve OpenAlex data and save it to the database (one page at a time to minimize RAM usage)