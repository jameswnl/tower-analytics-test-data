import os
from datetime import datetime
from pathlib import Path

import pytest
from api.core.generate_data import notify_upload

def test_notify_upload(mocker):
    url = 'a_url'
    account_id = '1'
    tenant_id = 1
    bundle_id = '1' * 32
    produce_upload_message = mocker.patch('api.core.generate_data.produce_upload_message')
    datetime_mock = mocker.patch('api.core.generate_data.datetime')
    datetime_mock.now.return_value = datetime.fromtimestamp(1582845336)
    os_stat = mocker.patch('api.core.generate_data.os.stat')
    os_stat.return_value = mocker.MagicMock()
    os_stat.return_value.st_size = 1000
    notify_upload(url, account_id, tenant_id, bundle_id)

    payload = {
        'account': account_id,
        'b64_identity': '__=',
        'category': 'analytics',
        'metadata': {},
        'principal': tenant_id,
        'request_id': bundle_id,
        'service': 'tower',
        'size': 1000,
        'timestamp': datetime_mock.now.return_value.astimezone().isoformat(),
        'url': '{}/bundles/{}?done=True'.format(url, bundle_id)
    }
    produce_upload_message.assert_called_once_with(payload)
