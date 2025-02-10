from pydantic import BaseModel
from typing import Any, Dict, List

class QueryRequest(BaseModel):
    query: str
    parameters: Dict[str, Any] = None

class Node(BaseModel):
    id: str
    label: str
    properties: Dict

class Edge(BaseModel):
    source: str
    target: str
    type: str

class GraphResponse(BaseModel):
    nodes: List[Node]
    edges: List[Edge]