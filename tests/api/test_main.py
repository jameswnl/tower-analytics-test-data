import os
import pytest

from pathlib import Path

from api.main import app, BUNDLE_DIR
from api.core.generate_data import get_bundle_path
from starlette.testclient import TestClient


client = TestClient(app)

@pytest.fixture()
def create_bundle():
    f = get_bundle_path('foo')
    Path(f).touch()
    yield
    os.remove(f)
    if os.path.exists(f+'.done'):
        os.remove(f+'.done')


def test_get_bundle_not_exist():
    response = client.get("/bundles/foo")
    assert response.status_code == 404


def test_get_bundle(create_bundle):
    response = client.get("/bundles/foo?done=False")
    assert response.status_code == 200
    assert not os.path.exists(get_bundle_path('foo')+'.done')


def test_get_bundle_done(create_bundle):
    response = client.get("/bundles/foo?done=True")
    assert response.status_code == 200
    assert os.path.exists(get_bundle_path('foo')+'.done')


