# imdb-graph
Explore and analyze imdb data with graph technologies.

# Requirements
Code in this repository has been tested on the following machine:
* Chip: Apple M4 Max
* Memory: 36 GB
* OS: macOS Sequoia

Information on the database:
* Neo4j version: 5.20.0 with the APOC library.

# Installation
Create your virtual environment:
```shell
virtualenv -p python3 venv
source venv/bin/activate
```

Run the following command to import external libraries:
```shell
make init
```

# Graph Schema and System Architecture
Details on graph schema and system architecture are available in [this page](graph_schema_system_design.md). 

# Download
To download the files, you can run the following command:

```shell
make download
```

# Importing
To load all the files into Neo4j, you can run the following command:

```shell
make import-all
```

To load one file at a time you can run the following scripts:

```shell
make import-name
make import-titles
make import-title-akas
make import-title-principals
make import-title-episode
make import-title-crew
make import-title-ratings
```

# Data Quality Checking
Multiple strategies have been adopted within the import scripts to mantain and check the integrity of the data.

## Unique Constraints
To avoid duplicated nodes for based on the ids for people and titles, we defined the following constraints:

```cypher
CREATE CONSTRAINT titleId IF NOT EXISTS FOR (node:Title) REQUIRE node.id IS UNIQUE;
CREATE CONSTRAINT personId IF NOT EXISTS FOR (node:Person) REQUIRE node.id IS UNIQUE;
```

## MERGE Clause
If duplicate data exists in one or multiple CSVs, the `MERGE` clause ensures that only new data is inserted, preventing duplication. Additionally, I have adopted a strategy to store the names of the source files, allowing for better tracking of how nodes are created and enriched:

```cypher
MERGE (title:Title {id: item.tconst})
SET title.sources = apoc.coll.toSet(coalesce(title.sources, []) + "title.principals.tsv")
MERGE (person:Person {id: item.nconst})
SET person.sources = apoc.coll.toSet(coalesce(person.sources, []) + "title.principals.tsv")
```

## Checks on Node properties
Some specific checks can be applied to store properties in a proper way. 

```cypher
MERGE (person)-[worked_in:WORKED_IN]->(title)
SET worked_in.ordering = toInteger(item.ordering),
    worked_in.category = item.category,
    worked_in.job = 
        CASE WHEN item.job <> '\\N' THEN item.job
        ELSE NULL END,
    worked_in.characters =
        CASE WHEN item.characters <> '\\N' THEN item.characters
        ELSE NULL END
```

## Example of Consistency Check
I prepared a quick test for checking data consistency between the CSV files and the generated graph. More specifically, I wanted to test if there where inconsistencies between the `title.basics.tsv` and the graph built from different sources. By running the following command, you will process the CSV file and run multiple cypher queries:

```shell
make data-quality-analysis
```

Here is the output of this test:
```shell
Number of unique title ids in title.basics.tsv 11426761
Number of unique titles in the graph 11426762
Nodes generated from sources other than title.basics.tsv [{'n': {'name_basics_tsv': True, 'sources': ['name.basics.tsv'], 'id': '\\N'}}]
```

This test shows that the ingestion process of the `name.basics.tsv` needs be fixed due to the cases in which the `knownforTitle` fields is equal to `\N` like in the following example:

```shell
nm9993719	Andre Hill	\N	\N	\N	\N
```

These issue generates a new Title node from the `name.basics.tsv`, whose id is `\\N`.

# Inference
For the inference phase, you should run the server as follows (from the root folder):

## Service Test
```shell
uvicorn service.main:app --host 0.0.0.0 --port 8000 --reload
```

You can run any query, by replacing the cypher query available in `service/client.py` file. Then you can run:

```shell
python service/client_test.py
```

## Shortest Path Inference
An example of inference phase provided in this repository is based on the [Six Degrees of Kevin Bacon](https://en.wikipedia.org/wiki/Six_Degrees_of_Kevin_Bacon). To test this inference, we will use the `allShortestPath` built-in function of Neo4j. The goal of this function is to provide the shortest paths of any size between two nodes. In our scenario, we want to identify the paths between Kevin Bacon and another actor.

This result of this inference step is available as HTML visualization. To test this visualization, you have to replace the name of the actor in the `service/index.html` file (line 21). Then you can open this file in your browser and you will be able to see the shortest paths connecting Kevin Bacon and the selected person.

## Add Your Rating
To add your rating, you can run the following command by replacing the title id and the rating.

```shell
python service/client_rating.py
```

The rating will increment the current value and update the average.
