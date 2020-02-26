import logging
import os
import uuid
from os import listdir
from pathlib import Path

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.logger import logger
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse

from .core.generate_data import (TestDataGenerator, get_bundle_path,
                                 notify_upload)

BUNDLE_DIR = os.environ.get('BUNDLE_DIR', '/BUNDLE_DIR')
HOST_URL = os.environ.get('HOST_URL', 'http://testbuild:8000')
LOG_LEVEL = int(os.environ.get('LOG_LEVEL', logging.INFO))

class BundleConfig(BaseModel):
    unified_jobs: int
    job_events: int
    tasks_count: int
    orgs_count: int
    templates_count: int
    spread_days_back: int
    starting_day: int
    hosts_count: int
    failed_job_modulo: int
    uuid: str
    tenant_id: int
    account_id: str


class BundleState(BaseModel):
    uuid: str
    processed: bool


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


def remove_processed_bundles(to_del):
    logger.info('Removing processed %d bundles', len(to_del))
    for uuid in to_del:
        f = get_bundle_path(uuid)
        logger.info('Removing %s', f)
        os.remove(f)


@app.get("/bundles/")
async def list_bundles(background_tasks: BackgroundTasks):
    """Listing bundles and status."""
    all = [f for f in listdir(BUNDLE_DIR)]
    # Processed bundles has a corresponing '.done' file
    done = [f[:32] for f in all if f.endswith('.done')]
    # Bundles that is not yet processed
    tars = [f[:32] for f in all if f.endswith('.gz') and not f[:32] in done]
    # Bundles that are processed and so can be purged
    purge = [f[:32] for f in all if f.endswith('.gz') and f[:32] in done]
    out = []
    for uuid in tars:
        out.append(BundleState(uuid=uuid, processed=False))
    for uuid in done:
        out.append(BundleState(uuid=uuid, processed=True))
    if purge:
        background_tasks.add_task(remove_processed_bundles, purge)
    return out


@app.post("/bundles/")
async def create_bundle(config: BundleConfig, process: bool=True):
    """Create a bundle and return an ID for later reference."""
    config.uuid = str(uuid.uuid4()).replace('-', '')
    bundle_file = TestDataGenerator().generate_bundle(config)
    if process:
        notify_upload(
            HOST_URL,
            config.account_id,
            config.tenant_id,
            config.uuid)
    else:
        logger.info("Process=False, not sending message")
    return config


@app.get("/bundles/{bundle_id}")
def get_bundle(bundle_id: str, done: bool=False):
    """Return a bundle."""
    data_bundle = get_bundle_path(bundle_id)
    if not os.path.isfile(data_bundle):
        logger.error("Bundle {} not found".format(data_bundle))
        raise HTTPException(
            status_code=404,
            detail="Bundle ID={} not found".format(bundle_id))
    if done:
        # mark it as done, to be deleted
        bundle_done = '{}.done'.format(data_bundle)
        Path(bundle_done).touch()
    return FileResponse(data_bundle, media_type="application/gzip")


@app.get("/process/{bundle_id}")
def process_bundle(bundle_id: str, tenant_id: int, account_id: str = '123456'):
    """Push bundle to processor."""
    return notify_upload(HOST_URL, account_id, tenant_id, bundle_id)

