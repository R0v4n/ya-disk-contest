import logging
from datetime import datetime, timezone, timedelta
from http import HTTPStatus
from random import choice, randint, uniform

from locust import HttpUser, task

from cloud.api.handlers import ImportsView, DeleteNodeView, NodeView, UpdatesView, NodeHistoryView
from cloud.utils.testing import FakeCloudGen, url_for


# from locust.exception import RescheduleTask


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

            logging.info(
                '%s: %s, http status %d (expected %d), took %rs',
                method, path, resp.status_code, expected_status,
                resp.elapsed.total_seconds()
            )
            return resp

    @classmethod
    def next_import_date(cls):
        # lesser timedelta will cause huge updates response size.
        cls._last_import_date += timedelta(hours=randint(1, 6))
        return cls._last_import_date

    def make_dataset(self):
        self.cloud.random_import(
            schemas_count=2,
            date=self.next_import_date(),
            allow_random_count=True
        )
        self.cloud.random_updates(count=5, allow_random_count=True)
        return self.cloud.get_import_dict()

    @task(7)
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

    def get_node(self, node_id: str = None, ids: list[str] | tuple[str] = None, **req_kwargs):
        if node_id is None:
            if ids is None:
                ids = self.cloud.ids
            if ids:
                node_id = choice(ids)

        if node_id:
            path = url_for(NodeView.URL_PATH, {'node_id': node_id})
            self.request('GET', path, **req_kwargs)

    @task(18)
    def get_folder(self, folder_id: str = None):
        self.get_node(folder_id, self.cloud.folder_ids, name='get_folder')

    @task(2)
    def get_file(self, file_id: str = None):
        self.get_node(file_id, self.cloud.file_ids, name='get_file')

    @task(20)
    def get_node_history(self):
        ids = self.cloud.ids
        if ids:
            node_id = choice(ids)

            delta = self._last_import_date - self._date_start

            ds = self._date_start + delta * uniform(-1, 1)
            de = ds + delta * uniform(0.1, 1.5)

            path = url_for(
                NodeHistoryView.URL_PATH,
                path_params=dict(node_id=node_id),
                query_params=dict(dateStart=ds, dateEnd=de)
            )
            self.request('GET', path, name='node_history')

    @task(8)
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
