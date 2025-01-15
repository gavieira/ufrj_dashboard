library('openalexR')
library('RSQLite')
library('tidyverse')


options(openalexR.mailto = "gabriel.vieira@bioqmed.ufrj.br")

all_works <- 


works_from_dois <- oa_fetch(
  entity = "works",
  authorships.institutions.ror = '03490as77',
  publication_year = 2022,
  verbose = TRUE
)

most_cited <- oa_fetch(
  entity = "works",
  authorships.institutions.ror = '03490as77',
  cited_by_count = '>100',
  verbose = TRUE
)

View(works_from_dois)
View(most_cited)

db_data <- most_cited %>%
  select(-c('author','counts_by_year','concepts','topics','grants',
            'ids','referenced_works','related_works'))

View(db_data)

# Connect to the SQLite database
db_path <- "~/dashboards/sqlite/most_cited.sqlite"
conn <- (dbConnect(SQLite(), dbname = db_path))

# Write the dataframe to the SQLite database
dbWriteTable(conn, "most_cited", db_data, overwrite = TRUE, row.names = FALSE)
