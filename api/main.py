import os
import uuid

from fastapi import FastAPI
from pydantic import BaseModel
from starlette.responses import FileResponse
from starlette.middleware.cors import CORSMiddleware

from .core.generate_data import TestDataGenerator


BUNDLE_DIR = os.environ.get('BUNDLE_DIR', os.path.join(os.getcwd(), 'BUNDLE_DIR'))

class BundleConfig(BaseModel):
    unified_jobs: int
    job_events: int
    uuid: str = ''

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/bundles/")
async def create_bundle(config: BundleConfig):
    generator = TestDataGenerator(BUNDLE_DIR)
    id = str(uuid.uuid1())
    config = BundleConfig(unified_jobs=4, job_events=4, uuid=id)
    generator.handle(config)
    return config


@app.get("/bundles/{bundle_id}")
async def get_bundle(bundle_id: str):
    generator = TestDataGenerator(BUNDLE_DIR)
    return FileResponse(generator.get_bundle_file(bundle_id), media_type="application/gzip")


