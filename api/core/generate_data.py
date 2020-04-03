import datetime
import io
import json
import os
import pkgutil
import shutil
import tarfile
import tempfile
import time

from fastapi.logger import logger
from kafka import KafkaProducer
from kafka.errors import KafkaError

BUNDLE_DIR = os.environ.get('BUNDLE_DIR', '/BUNDLE_DIR')
KAFKA_HOST = os.environ.get('KAFKA_HOST', 'kafka')
KAFKA_PORT = os.environ.get('KAFKA_PORT', '9092')
KAFKA_TOPIC = 'platform.upload.tower'
KAFKA_PRODUCER = None
FILES = ['config.json',
         'counts.json',
         'cred_type_counts.json',
         'events_table.csv',
         'instance_info.json',
         'inventory_counts.json',
         'job_counts.json',
         'job_instance_counts.json',
         'manifest.json',
         'org_counts.json',
         'projects_by_scm_type.json',
         'query_info.json',
         'unified_job_template_table.csv',
         'unified_jobs_table.csv']

try:
    KAFKA_PRODUCER = KafkaProducer(
        bootstrap_servers=['{0}:{1}'.format(KAFKA_HOST, KAFKA_PORT)],
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )
except:
    logger.exception('Failed to connect to: %s:%s', KAFKA_HOST, KAFKA_PORT)


class TestDataGenerator:
    def _default_date_time(self, days_ago=0, seconds=0):
        date = datetime.datetime.now() - datetime.timedelta(days=days_ago)
        date = date.replace(
            hour=1, minute=21, second=seconds, microsecond=840210)
        return date.astimezone().isoformat()

    def read_sample_data(self):
        return {filename: pkgutil.get_data('api.core.sample_data', filename)
                for filename in FILES}

    def write_data(self, temp_dir, data):
        for filename in FILES:
            with open(os.path.join(temp_dir, filename), 'wb') as f:
                f.write(data[filename])

    def build_tarfile(self, temp_dir, data_bundle):
        os.chdir(temp_dir)
        with tarfile.open(data_bundle, 'w:gz') as tar:
            for filename in FILES:
                tar.add(filename)
        return data_bundle

    def _job_status(self, i):
        if self.failed_job_threshold >= 0 and i % 200 >= self.failed_job_threshold:
            return 'failed'
        elif self.pending_job_threshold >= 0 and i % 200 >= self.pending_job_threshold:
            return 'pending'
        elif self.error_job_threshold >= 0 and i % 200 >= self.error_job_threshold:
            return 'error'
        else:
            return 'successful'

    def _failed_job(self, i):
        # half of jobs will be failed
        if i % self.failed_job_modulo >= 100:
            return 'failed'
        else:
            return 'successful'

    def generate_unified_jobs(self, data, jobs_count, orgs_count, templates_count, spread_days_back, starting_day):
        """
        Appends to the unified jobs table with a CSV data

        - respecting scheme described in `sample_data/unified_jobs_table.csv`
        - repeating an sample entry `n` times
        """
        output = io.StringIO()
        output.write(data['unified_jobs_table.csv'].decode())
        for job_id in range(jobs_count):
            output.write('{job_id},37,job,1,organization_{org_id},{created},template_name_{template_id},471,'
                         'scheduled,19,localhost,"",f,{status},f,{started},{finished},5.873,"",1\n'.format(
                             created=self._default_date_time((job_id % spread_days_back) + starting_day),
                             status=self._job_status(job_id),
                             started=self._default_date_time((job_id % spread_days_back) + starting_day, 1),
                             finished=self._default_date_time((job_id % spread_days_back) + starting_day, 5),
                             job_id=job_id,
                             org_id=job_id % orgs_count,
                             template_id=job_id % templates_count
                         ))

        data['unified_jobs_table.csv'] = output.getvalue().encode()

    def _failed_event(self, i):
        # half of events will be failed
        if i % 4 == 0 or i % 4 == 1:
            return 'f'
        else:
            return 't'

    def _changed_event(self, i):
        # every second failed and not failed event is not changed
        if i % 4 == 0 or i % 4 == 2:
            return 'f'
        else:
            return 't'

    def generate_job_events(self, data, jobs_count, events_count, tasks_count, 
                            spread_days_back, starting_day, hosts_count):
        """
        Appends to the events table with a CSV data

        - respecting scheme described in `sample_data/events_table.csv`
        - repeating an sample entry `n` times
        """
        output = io.StringIO()
        output.write(data['events_table.csv'].decode())
        for job_id in range(jobs_count):
            for event_id in range(events_count):
                id = self.starting_event_id + ((events_count+1) * job_id) + event_id
                id = ((events_count+1) * job_id) + event_id

                output.write('{id},{created},374c9e9c-561c-4222-acd4-91189dd95b1d,"",verbose_{module_id},'
                             'verbose_module_{module_id},{failed},{changed},"","","super_task_{module_id}",'
                             '"",{job_id},{host_id},"host_name_{host_id}"\n'.format(
                                 created=self._default_date_time((job_id % spread_days_back) + starting_day,
                                                                 event_id % 60),
                                 failed=self._failed_event(event_id),
                                 changed=self._changed_event(event_id),
                                 job_id=job_id,
                                 id=id,
                                 module_id=event_id % tasks_count,
                                 host_id=id % hosts_count
                             ))

        data['events_table.csv'] = output.getvalue().encode()

    def patch_config_json(self, bundle_config, data):
        config_json = json.loads(data['config.json'].decode())
        if bundle_config.install_uuid:
            config_json['install_uuid'] = bundle_config.install_uuid
        if bundle_config.instance_uuid:
            config_json['instance_uuid'] = bundle_config.instance_uuid
        if bundle_config.tower_url_base:
            config_json['tower_url_base'] = bundle_config.tower_url_base
        data['config.json'] = json.dumps(config_json).encode()

    def generate_bundle(self, bundle_config):
        start = time.time()
        tasks_count = bundle_config.tasks_count or 100
        orgs_count = bundle_config.orgs_count or 1
        templates_count = bundle_config.templates_count or 1
        spread_days_back = bundle_config.spread_days_back or 100
        starting_day = bundle_config.starting_day or 1
        hosts_count = bundle_config.hosts_count or 1
        self.failed_job_threshold = bundle_config.failed_job_threshold or 100
        self.pending_job_threshold = bundle_config.pending_job_threshold or -1
        self.error_job_threshold = bundle_config.error_job_threshold or -1
        self.starting_event_id = bundle_config.starting_event_id or 0
        self.failed_job_modulo = bundle_config.failed_job_modulo or 200

        temp_dir = tempfile.mkdtemp()
        data = self.read_sample_data()
        self.patch_config_json(bundle_config, data)
        self.generate_unified_jobs(data, bundle_config.unified_jobs,
                                   orgs_count, templates_count,
                                   spread_days_back, starting_day)
        self.generate_job_events(data, bundle_config.unified_jobs,
                                 bundle_config.job_events, tasks_count,
                                 spread_days_back, starting_day, hosts_count)

        self.write_data(temp_dir, data)
        data_bundle = os.path.join(
            BUNDLE_DIR,
            '{}_data_bundle.tar.gz'.format(bundle_config.bundle_uuid))
        self.build_tarfile(temp_dir, data_bundle)
        logger.info("bundle created: tempdir={}, bundle={}, size={}".format(
                    temp_dir, data_bundle, os.stat(data_bundle).st_size))
        end = time.time()
        logger.info('handle_analytics_bundle time:%f', end-start)
        shutil.rmtree(temp_dir)
        return data_bundle


def get_bundle_path(bundle_id):
    return os.path.join(BUNDLE_DIR, '{}_data_bundle.tar.gz'.format(bundle_id))


def produce_upload_message(json_payload):
    if not KAFKA_PRODUCER:
        raise Exception("Kafka not available")
    logger.debug("to producer.send()")
    future = KAFKA_PRODUCER.send(KAFKA_TOPIC, json_payload)
    try:
        record_metadata = future.get(timeout=10)
        logger.info("send future completed")
        return record_metadata
    except KafkaError:
        logger.exception('Failed to send to kafka')
        raise


def notify_upload(url, account_id, tenant_id, bundle_id):
    logger.debug("notify_upload")
    bundle_file = get_bundle_path(bundle_id)
    bundle_size = os.stat(bundle_file).st_size
    payload = {
        'account': account_id,
        'b64_identity': '__=',
        'category': 'analytics',
        'metadata': {},
        'principal': tenant_id,
        'request_id': bundle_id,
        'service': 'tower',
        'size': bundle_size,
        'timestamp': datetime.datetime.now().astimezone().isoformat(),
        'url': '{}/bundles/{}?done=True'.format(url, bundle_id)
    }
    return produce_upload_message(payload)
