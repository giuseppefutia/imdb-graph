def process_neo4j_res_as_graph(result):
    nodes_dict = {}
    edges = []

    for path_entry in result:
        path = path_entry["path"]
        
        for i in range(0, len(path), 2):
            node_data = path[i]
            node_id = node_data["id"]
            
            if node_id not in nodes_dict:
                nodes_dict[node_id] = {
                    "id": node_id,
                    "label": node_data["name"],
                    "properties": node_data
                }

            if i + 2 < len(path):
                relationship = path[i + 1]
                next_node_data = path[i + 2]
                next_node_id = next_node_data["id"]

                edge = {
                    "source": next_node_id,
                    "target": node_id,
                    "type": relationship
                }
                edges.append(edge)

    return {
        "nodes": list(nodes_dict.values()),
        "edges": edges
    }