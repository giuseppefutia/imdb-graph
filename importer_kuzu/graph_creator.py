import kuzu

def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database("./imdb2")
    conn = kuzu.Connection(db)

    # Create schema
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Person(id STRING, name STRING, birthYear INT64, deathYear INT64, PRIMARY KEY (id))")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Profession(name STRING, PRIMARY KEY (name))")
    conn.execute("CREATE NODE TABLE IF NOT EXISTS Title (id STRING, PRIMARY KEY (id))")
    conn.execute("CREATE REL TABLE IF NOT EXISTS HAS_PROFESSION(FROM Person TO Profession)")
    conn.execute("CREATE REL TABLE IF NOT EXISTS KNOWN_FOR(FROM Person TO Title)")

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