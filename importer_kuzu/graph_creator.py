import kuzu

def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database("./imdb")
    conn = kuzu.Connection(db)

    # Create schema
    conn.execute("CREATE NODE TABLE Person(id STRING, name STRING, birthYear INT64, deathYear INT64, PRIMARY KEY (id))")
    conn.execute("CREATE NODE TABLE Profession(name STRING, PRIMARY KEY (name))")
    conn.execute("CREATE REL TABLE HAS_PROFESSION(FROM Person TO Profession)")

    # Execute Cypher query
    response = conn.execute(
        """
        MATCH (n)
        RETURN COUNT(n)
        """
    )
    while response.has_next():
        print("Graph status:")
        print(response.get_next())

if __name__ == '__main__':
    main()