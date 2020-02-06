from fastapi import FastAPI
from pydantic import BaseModel

from .core.generate_data import TestDataGenerator

class BundleConfig(BaseModel):
    unified_jobs: int
    job_events: int

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/bundles/")
async def create_bundle(config: BundleConfig):
    generator = TestDataGenerator()
    generator.handle(config)
    return config


