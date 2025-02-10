# imdb-graph
Explore and analyze imdb data with graph technologies.

# Requirements
Code in this repository has been tested on the following machine:
* Chip: Apple M4 Max
* Memory: 36 GB
* OS: macOS Sequoia

Information on the database:
* Neo4j version: 5.20.0

## Installation
Create your virtual environment:
```shell
virtualenv -p python3 venv
source venv/bin/activate
```

Run the following command to import external libraries:
```shell
make init
```

## Importing
To load all the files into Neo4j, you can run the following command:

```shell
make import-all
```

To load one file at a time you can run the following scripts one at a time:

```shell
make import-name
make import-titles
make import-title-akas
make import-title-principals
make import-title-episode
make import-title-crew
make import-title-ratings
```
