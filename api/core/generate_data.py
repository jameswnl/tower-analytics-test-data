import datetime
import pkgutil
import json
import tempfile
import os
import io
import tarfile
import shutil
import time


FILES = [ 'config.json',
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
          'unified_jobs_table.csv' ]


class TestDataGenerator:
    def add_arguments(self, parser):
        parser.add_argument("tenant_id", type=int, help="tenant")
        parser.add_argument(
            "job_events", type=int, help="length of job events table"
        )
        parser.add_argument(
            "unified_jobs", type=int, help="length of unified jobs table"
        )

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

    def build_tarfile(self, temp_dir):
        data_bundle = os.path.join(temp_dir, 'data_bundle.tar.gz')
        os.chdir(temp_dir)
        with tarfile.open(data_bundle, 'w:gz') as tar:
            for filename in FILES:
                tar.add(filename)
        return data_bundle

    def _failed_job(self, i):
        # half of jobs will be failed
        if i % 200 >= 100:
            return 'failed'
        else:
            return 'successful'

    def generate_unified_jobs(self, data, n):

        """
        Appends to the unified jobs table with a CSV data

        - respecting scheme described in `sample_data/unified_jobs_table.csv`
        - repeating an sample entry `n` times
        """
        output = io.StringIO()
        output.write(data['unified_jobs_table.csv'].decode())
        for job_id in range(n):
            output.write('{4},37,job,1,Default,{0},verify,471,scheduled,19,localhost,"",'
                         'f,{1},f,{2},{3},5.873,"",'
                         '1\n'.format(self._default_date_time(job_id % 100 + 1), # jobs spread 100 days back
                                      self._failed_job(job_id),
                                      self._default_date_time(job_id % 100 + 1, 1),
                                      self._default_date_time(job_id % 100 + 1, 5),
                                      job_id
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

    def generate_job_events(self, data, jobs_count, events_count):
        """
        Appends to the events table with a CSV data

        - respecting scheme described in `sample_data/events_table.csv`
        - repeating an sample entry `n` times
        """
        output = io.StringIO()
        output.write(data['events_table.csv'].decode())
        for job_id in range(jobs_count):
            for event_id in range(events_count):
                output.write('{4},{0},374c9e9c-561c-4222-acd4-91189dd95b1d,"",verbose_{5},verbose_module_{5},{1},{2},'
                             '"","","super_task_{5}","",{3},,""\n'.format(
                                self._default_date_time(job_id % 100 + 1, event_id%60), # events spread 100 days back
                                self._failed_event(event_id),
                                self._changed_event(event_id),
                                job_id,
                                job_id*event_id,
                                event_id % 100,  # 100 different tasks
                                ))

        data['events_table.csv'] = output.getvalue().encode()

    def handle(self, bundle_config):
        temp_dir = tempfile.mkdtemp()
        data = self.read_sample_data()
        self.generate_unified_jobs(data, bundle_config.unified_jobs)
        self.generate_job_events(data, bundle_config.unified_jobs, bundle_config.job_events)
        self.write_data(temp_dir, data)
        data_bundle = self.build_tarfile(temp_dir)
        print("bundle created: {}, size={}".format(data_bundle, os.stat(data_bundle).st_size))
        start = time.time()
        end = time.time()
        print ('handle_analytics_bundle time:', end-start, 's')
        # shutil.rmtree(temp_dir)


