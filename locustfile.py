from datetime import datetime, timezone, timedelta
from http import HTTPStatus
from random import choice, randint, uniform

from locust import HttpUser, task, between
from locust.exception import RescheduleTask

from cloud.utils.testing import FakeCloudGen, url_for
from cloud.api.handlers import ImportsView, DeleteNodeView, NodeView, UpdatesView, NodeHistoryView


class User(HttpUser):
    _last_import_date: datetime = datetime.now(timezone.utc)
    _date_start: datetime = _last_import_date

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.cloud = FakeCloudGen(write_history=False)
        self.import_count = 0

    def request(self, method, path, expected_status=HTTPStatus.OK, **kwargs):
        with self.client.request(
                method, path, catch_response=True, **kwargs
        ) as resp:
            if resp.status_code != expected_status:
                resp.failure(f'expected status {expected_status}, '
                             f'got {resp.status_code}')
            # todo: add logging
            # logging.info(
            #     'round %r: %s %s, http status %d (expected %d), took %rs',
            #     self.round, method, path, resp.status_code, expected_status,
            #     resp.elapsed.total_seconds()
            # )
            return resp

    @classmethod
    def next_import_date(cls):
        cls._last_import_date += timedelta(hours=randint(3, 72))
        return cls._last_import_date

    def make_dataset(self):
        self.cloud.random_import(
            schemas_count=2,
            date=self.next_import_date(),
            allow_random_count=True,
            max_files_in_one_folder=8
        )
        self.cloud.random_updates(count=5, allow_random_count=True)
        return self.cloud.get_import_dict()

    @task(1)
    def post_import(self):
        self.request('POST', ImportsView.URL_PATH, json=self.make_dataset())

    @task(1)
    def delete_node(self, node_id: str = None):
        if node_id is None:
            ids = self.cloud.ids
            if ids:
                node_id = choice(self.cloud.ids)

        if node_id:
            date = self.cloud.del_item(node_id, self.next_import_date())
            path = url_for(DeleteNodeView.URL_PATH, {'node_id': node_id}, {'date': date})
            self.request('DELETE', path, name='del_node')

    @task(5)
    def get_node(self, node_id: str = None):
        if node_id is None:
            ids = self.cloud.ids
            if ids:
                node_id = choice(self.cloud.ids)

        if node_id:
            path = url_for(NodeView.URL_PATH, {'node_id': node_id})
            self.request('GET', path, name='get_node')

    @task(5)
    def get_node_history(self):
        ids = self.cloud.ids
        if ids:
            node_id = choice(self.cloud.ids)

            delta = self._last_import_date - self._date_start

            ds = self._date_start + delta * uniform(-1, 1)
            de = ds + delta * uniform(0.1, 1.5)

            path = url_for(
                NodeHistoryView.URL_PATH,
                path_params=dict(node_id=node_id),
                query_params=dict(dateStart=ds, dateEnd=de)
            )
            self.request('GET', path, name='node_history')

    @task(2)
    def get_updates(self):
        date = self._date_start + uniform(0, 1) * (self._last_import_date - self._date_start)
        date += timedelta(hours=randint(0, 24))

        path = url_for(
            UpdatesView.URL_PATH,
            query_params=dict(date=date)
        )
        self.request('GET', path, name='updates')

    # def on_start(self):
    #     self.cloud.generate_import([[[]]])
    #     self.request('POST', ImportsView.URL_PATH, json=self.cloud.get_import_dict())
