import logging
import os
import uuid
from os import listdir
from pathlib import Path

from datasette_auth_github import GitHubAuth
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.logger import logger
from pydantic import BaseModel
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import FileResponse

from .core.generate_data import (TestDataGenerator, get_bundle_path,
                                 notify_upload)
logger.handlers = logging.getLogger('uvicorn.error').handlers
logger.setLevel(int(os.environ.get('LOG_LEVEL', logging.INFO)))

BUNDLE_DIR = os.environ.get('BUNDLE_DIR', '/BUNDLE_DIR')
HOST_URL = os.environ.get('HOST_URL', 'http://testbuild:8000')
GH_AUTH_CLIENT_ID = os.getenv('GH_AUTH_CLIENT_ID')
GH_AUTH_CLIENT_SECRET = os.getenv('GH_AUTH_CLIENT_SECRET')
ALLOW_GH_ORGS = (os.getenv('ALLOW_GH_ORGS') or 'Ansible').split(',')


class BundleConfig(BaseModel):
    unified_jobs: int = 1
    job_events: int = 1
    tasks_count: int = 100
    orgs_count: int = 1
    templates_count: int = 1
    spread_days_back: int = 100
    starting_day: int = 1
    hosts_count: int = 1
    failed_job_modulo: int = 1
    bundle_uuid: str = ''
    tenant_id: int = 1
    account_id: str = '1'
    install_uuid: str = ''
    instance_uuid: str = ''
    tower_url_base: str = ''
    failed_job_threshold: int = 100
    pending_job_threshold: int = -1
    error_job_threshold: int = -1
    starting_event_id: int = 0


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


if GH_AUTH_CLIENT_ID and GH_AUTH_CLIENT_SECRET:
    app.add_middleware(
        GitHubAuth,
        client_id=GH_AUTH_CLIENT_ID,
        client_secret=GH_AUTH_CLIENT_SECRET,
        require_auth=True,
        ignore_paths=[('GET', '/bundles/?*')],
        allow_orgs=ALLOW_GH_ORGS,
    )
    logger.info('Github Authentication enabled')
else:
    logger.warning(
        'GH_AUTH_CLIENT_ID and GH_AUTH_CLIENT_SECRET not set, '
        'no authentication is enabled')


@app.get("/")
async def root():
    return {"message": "Hello World"}


def remove_processed_bundles(to_del):
    logger.info('Removing processed %d bundles', len(to_del))
    for id in to_del:
        f = get_bundle_path(id)
        logger.info('Removing %s', f)
        os.remove(f)


def bundles_by_state():
    all = [f for f in listdir(BUNDLE_DIR)]
    # Processed bundles has a corresponing '.done' file
    done = [f[:32] for f in all if f.endswith('.done')]
    # Bundles that is not yet processed
    tars = [f[:32] for f in all if f.endswith('.gz') and not f[:32] in done]
    # Bundles that are processed and so can be purged
    purge = [f[:32] for f in all if f.endswith('.gz') and f[:32] in done]
    return tars, done, purge


@app.get("/bundles/")
def list_bundles():
    """Listing bundles and status."""
    tars, done, _ = bundles_by_state()
    out = []
    for id in tars:
        out.append(BundleState(uuid=id, processed=False))
    for id in done:
        out.append(BundleState(uuid=id, processed=True))
    return out


@app.post("/bundles/")
def create_bundle(config: BundleConfig, process: bool = True):
    """Create a bundle and return an ID for later reference."""
    config.bundle_uuid = str(uuid.uuid4()).replace('-', '')
    TestDataGenerator().generate_bundle(config)
    if process:
        notify_upload(
            HOST_URL,
            config.account_id,
            config.tenant_id,
            config.bundle_uuid)
    else:
        logger.info("Process=False, not sending message")
    return config


@app.delete("/bundles/{bundle_id}")
def delete_bundles(
    background_tasks: BackgroundTasks,
    bundle_id: str = 'processed'
):
    """Delete bundle file(s)."""
    logger.info("Deleting bundle: %s", bundle_id)
    purge = [bundle_id]
    if bundle_id == 'processed':
        _, _, purge = bundles_by_state()
    else:
        data_bundle = get_bundle_path(bundle_id)
        if not os.path.isfile(data_bundle):
            logger.error("Bundle {} not found".format(data_bundle))
            raise HTTPException(
                status_code=404,
                detail="Bundle ID={} not found".format(bundle_id))
    background_tasks.add_task(remove_processed_bundles, purge)
    return "Deleting {} bundles: {}".format(len(purge), purge)


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
