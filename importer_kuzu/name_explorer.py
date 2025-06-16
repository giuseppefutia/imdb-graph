import kuzu

def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database("./imdb")
    conn = kuzu.Connection(db)

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