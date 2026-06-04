from datetime import datetime, timezone
from fastapi import Depends, FastAPI, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.orm import Session
from src import election
from src.database import Base, engine, get_db
from src.models import Node
from src.schemas import NodeCreate, NodeResponse, NodeUpdate

Base.metadata.create_all(bind=engine)
app = FastAPI()


@app.on_event("startup")
def boot():
    election.start()


@app.get("/health")
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        db_status = "connected"
    except Exception:
        db_status = "disconnected"
    count = db.query(Node).filter(Node.status == "active").count()
    return {"status": "ok", "db": db_status, "nodes_count": count}


@app.post("/api/nodes", response_model=NodeResponse, status_code=201)
def register(node: NodeCreate, db: Session = Depends(get_db)):
    exists = db.query(Node).filter(Node.name == node.name).first()
    if exists:
        raise HTTPException(409, "Node already exists")
    item = Node(name=node.name, host=node.host, port=node.port)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item


@app.get("/api/nodes", response_model=list[NodeResponse])
def list_all(db: Session = Depends(get_db)):
    return db.query(Node).all()


@app.get("/api/nodes/{name}", response_model=NodeResponse)
def get_one(name: str, db: Session = Depends(get_db)):
    item = db.query(Node).filter(Node.name == name).first()
    if not item:
        raise HTTPException(404, "Node not found")
    return item


@app.put("/api/nodes/{name}", response_model=NodeResponse)
def update(name: str, upd: NodeUpdate, db: Session = Depends(get_db)):
    item = db.query(Node).filter(Node.name == name).first()
    if not item:
        raise HTTPException(404, "Node not found")
    if upd.host is not None:
        item.host = upd.host
    if upd.port is not None:
        item.port = upd.port
    item.updated_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(item)
    return item


@app.delete("/api/nodes/{name}", status_code=204)
def remove(name: str, db: Session = Depends(get_db)):
    item = db.query(Node).filter(Node.name == name).first()
    if not item:
        raise HTTPException(404, "Node not found")
    item.status = "inactive"
    item.updated_at = datetime.now(timezone.utc)
    db.commit()
    return Response(status_code=204)


@app.post("/election")
def election_msg(body: dict):
    sid = body.get("from")
    if sid is None:
        raise HTTPException(400, "Missing 'from'")
    return election.on_election(int(sid))


@app.post("/coordinator")
def coord_msg(body: dict):
    cid = body.get("id")
    curl = body.get("url")
    if cid is None or not curl:
        raise HTTPException(400, "Missing id or url")
    return election.on_coordinator(int(cid), str(curl))


@app.get("/ping")
def ping():
    return {"alive": True}


@app.get("/leader")
def leader_info():
    return election.state()


@app.post("/elect")
def trigger():
    return election.elect()
