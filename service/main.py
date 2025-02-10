import configparser

from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, HTTPException, Body

from util.graphdb_base import GraphDBBase
from service.models import QueryRequest, GraphResponse
from service.utils import process_neo4j_res_as_graph

config_file = "config.ini"
config = configparser.ConfigParser()
config.read(config_file)
neo4j_config = config["neo4j"]
database = neo4j_config.get("database")

# Initialize FastAPI App
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize graphdatabase
gd = GraphDBBase()
gd._database = database

def execute_query(query, parameters, gd=gd):
    with gd._driver.session(database=gd._database) as session:
        r = session.run(query, parameters)
        return r.data()

@app.post("/query")
async def run_query(request: QueryRequest):
    try:
        result = execute_query(request.query, request.parameters)
        return {"success": True, "data": result}
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/bacon", response_model=GraphResponse)
async def show_bacon_graph(name: str):
    
    if not name or not name.strip():
        raise HTTPException(status_code=400, detail="Name cannot be empty")
    
    query = """
        MATCH (p1:Person {name: 'Kevin Bacon'})
        MATCH (p2:Person)
        WHERE p2.name = $name
        MATCH path = allShortestPaths((p1)-[:WORKED_IN|DIRECTED|KNOWN_FOR|WROTE*]-(p2))
        RETURN path
        LIMIT 2;
        """
    try:
        result = execute_query(query, {"name": name})
        return process_neo4j_res_as_graph(result)
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/add_rating")
async def add_rating(title_id: str = Body(...), rate: int = Body(...)):
    if not title_id or not title_id.strip():
        raise HTTPException(status_code=400, detail="Movie ID cannot be empty")
    
    if not (1 <= rate <= 5):
        raise HTTPException(status_code=400, detail="Rate must be between 1 and 5")

    title_id = title_id.strip()

    check_movie_query = """
        MATCH (m:Title {id: $title_id})
        RETURN m
    """
    movie_exists = execute_query(check_movie_query, {"title_id": title_id})

    if not movie_exists:
        raise HTTPException(status_code=404, detail=f"Movie with ID '{title_id}' not found")

    query = """
        MATCH (m:Title {id: $title_id})
        SET m.numVotes = COALESCE(m.numVotes, 0) + 1
        SET m.average = 
            CASE 
                WHEN m.numVotes = 1 THEN $rate
                ELSE (COALESCE(m.average, 0) * (m.numVotes - 1) + $rate) / m.numVotes
            END
        RETURN m.id AS id, m.numVotes AS numVotes, m.average AS average
    """

    try:
        result = execute_query(query, {"title_id": title_id, "rate": rate})
        return {"success": True, "data": result}
    
    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Health Check Endpoint
@app.get("/health")
def health_check():
    return {"status": "ok"}
