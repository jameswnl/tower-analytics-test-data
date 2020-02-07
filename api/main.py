from fastapi import FastAPI
from pydantic import BaseModel
from starlette.responses import FileResponse
from starlette.middleware.cors import CORSMiddleware

from .core.generate_data import TestDataGenerator

class BundleConfig(BaseModel):
    unified_jobs: int
    job_events: int

app = FastAPI()
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/bundles/")
async def create_bundle(config: BundleConfig):
    generator = TestDataGenerator()
    bundle_file = generator.handle(config)
    response = FileResponse(bundle_file, media_type="application/gzip")
    return response
    # await response(scope, receive, send)



@app.get("/bundles/{bundle}")
async def get_bundle(bundle: str):
    generator = TestDataGenerator()
    config = BundleConfig(unified_jobs=4, job_events=4)
    bundle_file = generator.handle(config)
    response = FileResponse(bundle_file, media_type="application/gzip")
    return response
    # await response(scope, receive, send)


