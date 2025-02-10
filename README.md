# IMDb-Graph
Explore and analyze IMDb data using graph technologies.

## Requirements
This repository has been tested on the following system:
- **Chip**: Apple M4 Max
- **Memory**: 36 GB
- **OS**: macOS Sequoia

### Database Information
- **Neo4j version**: 5.20.0 with the APOC library.

## Installation
### Set Up Virtual Environment
```shell
virtualenv -p python3 venv
source venv/bin/activate
```

### Install Dependencies
```shell
make init
```

## Graph Schema and System Architecture
Details about the graph schema and system architecture are available [here](graph_schema_system_design.md).

## Download
To download the required files, run:
```shell
make download
```

## Data Import
### Import All Files
To load all files into Neo4j, execute:
```shell
make import-all
```

### Import Individual Files
To load files one at a time, use the following commands:
```shell
make import-name
make import-titles
make import-title-akas
make import-title-principals
make import-title-episode
make import-title-crew
make import-title-ratings
```

## Data Quality Checking
Various strategies have been implemented to maintain and validate data integrity.

### Unique Constraints
To prevent duplicate nodes based on person and title IDs, the following constraints are defined:
```cypher
CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT personId IF NOT EXISTS FOR (node:Person) REQUIRE node.id IS UNIQUE;
```

### MERGE Clause for Deduplication
If duplicate data exists across multiple CSV files, the `MERGE` clause ensures only new data is inserted. The source file names are also tracked to improve node traceability:
```cypher
MERGE (title:Title {id: item.tconst})
SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.principals.tsv")
MERGE (person:Person {id: item.nconst})
SET person.sources = apoc.coll.toSet(coalesce(person.sources, []) + "title.principals.tsv")
```

### Property Validation for Nodes
Certain property checks are applied to ensure proper data formatting:
```cypher
MERGE (person)-[worked_in:WORKED_IN]->(title)
SET worked_in.ordering = toInteger(item.ordering),
    worked_in.category = item.category,
    worked_in.job = CASE WHEN item.job <> '\\N' THEN item.job ELSE NULL END,
    worked_in.characters = CASE WHEN item.characters <> '\\N' THEN item.characters ELSE NULL END
```

## Consistency Check Example
A consistency test verifies discrepancies between `title.basics.tsv` and the constructed graph. Run the following command to process the CSV file and execute Cypher queries:
```shell
make data-quality-analysis
```

### Example Output:
```shell
Number of unique title IDs in title.basics.tsv: 11,426,761
Number of unique titles in the graph: 11,426,762
Nodes generated from sources other than title.basics.tsv: [{'n': {'name_basics_tsv': True, 'sources': ['name.basics.tsv'], 'id': '\\N'}}]
```

This output indicates an issue in the ingestion process for `name.basics.tsv` when the `knownForTitles` field is `\N`. Example problematic entry:
```shell
nm9993719    Andre Hill    \N    \N    \N    \N
```
This issue results in an unintended Title node with ID `\\N`.

## Inference
### Running the Server
To start the inference server, run:
```shell
uvicorn service.main:app --host 0.0.0.0 --port 8000 --reload
```

### Querying the API
To execute a query, modify the Cypher query in `service/client.py` and then run:
```shell
python service/client_test.py
```

### Shortest Path Inference
The repository includes an example inference based on the [Six Degrees of Kevin Bacon](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon). This test uses Neo4j’s `allShortestPaths` function to find connections between Kevin Bacon and another actor.

To visualize the results, update the actor’s name in `service/index.html` (line 21), then open the file in a browser to see the shortest paths.

## Adding Ratings
To submit a rating, replace the title ID and rating in the following command:
```shell
python service/client_rating.py
```
This script updates the average rating for the specified title.
