import logging
import os
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.logger import logger
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse

from .core.generate_data import (TestDataGenerator, get_bundle_file,
                                 notify_upload)

BUNDLE_DIR = os.environ.get('BUNDLE_DIR', '/BUNDLE_DIR')
HOST_URL = os.environ.get('HOST_URL', 'http://testbuild:8000')
LOG_LEVEL = int(os.environ.get('LOG_LEVEL', logging.INFO))

class BundleConfig(BaseModel):
    unified_jobs: int
    job_events: int
    uuid: str = ''
    tenant_id: int
    account_id: str


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)
logger.handlers = logging.getLogger('uvicorn.error').handlers
logger.setLevel(LOG_LEVEL)


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/bundles/")
async def create_bundle(config: BundleConfig):
    """Create a bundle and return an ID for later reference."""
    config.uuid = str(uuid.uuid4()).replace('-', '')
    bundle_file = TestDataGenerator().generate_bundle(config)
    notify_upload(
        HOST_URL,
        config.account_id,
        config.tenant_id,
        config.uuid)
    return config


@app.get("/bundles/{bundle_id}")
def get_bundle(bundle_id: str):
    """Return a bundle."""
    data_bundle = get_bundle_file(bundle_id)
    if not os.path.isfile(data_bundle):
        logger.error("Bundle {} not found".format(data_bundle))
        raise HTTPException(
            status_code=404,
            detail="Bundle ID={} not found".format(bundle_id))
    return FileResponse(data_bundle, media_type="application/gzip")


@app.get("/process/{bundle_id}")
def process_bundle(bundle_id: str, tenant_id: int, account_id: str = '123456'):
    """Push bundle to processor."""
    return notify_upload(HOST_URL, account_id, tenant_id, bundle_id)

