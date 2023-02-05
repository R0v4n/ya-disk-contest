import logging
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from random import choice, uniform, randint

from locust import task, FastHttpUser, constant, events
from locust.runners import MasterRunner

from cloud.resources import url_paths
from cloud.utils.testing import FakeCloudGen, url_for, Folder


@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    if not isinstance(environment.runner, MasterRunner):
        User.init_cloud()


class User(FastHttpUser):

    wait_time = constant(1)

    cloud: FakeCloudGen
    all_instances_last_import_ids: set[str]
    first_import_date: datetime
    root_ids: set[str]

    @classmethod
    def init_cloud(cls):
        cls.cloud = FakeCloudGen(write_history=False)
        cls.all_instances_last_import_ids = set()
        cls.first_import_date = datetime.now(timezone.utc)
        cls.root_ids = set()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._last_import_ids = set()
        self.last_import_ids = {self.root_folder_import()}

    @property
    def last_import_ids(self) -> set[str]:
        return self._last_import_ids

    @last_import_ids.setter
    def last_import_ids(self, value: set[str]):
        self.all_instances_last_import_ids -= self._last_import_ids
        self._last_import_ids = value
        self.all_instances_last_import_ids |= value

    def root_folder_import(self) -> str:
        self.cloud.generate_import()
        root = Folder(parent_id=None)
        self.cloud.insert_item(root)
        self.root_ids |= {root.id}

        self.request('POST', url_paths.IMPORTS, json=self.cloud.get_import_dict())

        return root.id

    def request(self, method, path, expected_status=HTTPStatus.OK, **kwargs):
        with self.client.request(
                method, path, catch_response=True, **kwargs
        ) as resp:
            if resp.status_code != expected_status:
                resp.failure(f'expected status {expected_status}, '
                             f'got {resp.status_code}')

            logging.info(
                '%s: %s, http status %d (expected %d)',
                method, path, resp.status_code, expected_status
            )
            return resp

    @task(9)
    def post_import(self):
        coin = randint(0, 2)
        if coin:
            self.cloud.random_import(schemas_count=1, allow_random_count=False)
        else:
            self.cloud.generate_import()
            self.cloud.random_updates(count=3, allow_random_count=False, excluded_ids=self.root_ids)

        data = self.cloud.get_import_dict()

        self.last_import_ids = {i['id'] for i in data['items']}

        self.request('POST', url_paths.IMPORTS, json=data)

    def delete_node(self, ids, **req_kwargs):
        if not isinstance(ids, tuple):
            ids = tuple(ids)

        if ids:
            node_id = choice(ids)
            date = self.cloud.del_item(node_id)
            path = url_for(url_paths.DELETE_NODE, {'node_id': node_id}, {'date': date})

            self.request('DELETE', path, **req_kwargs)

    @task(1)
    def delete_folder(self):
        self.delete_node(set(self.cloud.folder_ids) - self.root_ids, name='delete folder')

    @task(2)
    def delete_file(self):
        self.delete_node(self.cloud.file_ids, name='delete file')

    def get_node(self, ids: list[str] | tuple[str], **req_kwargs):
        ids = tuple(set(ids) - self.all_instances_last_import_ids)
        if ids:
            node_id = choice(ids)
            path = url_for(url_paths.GET_NODE, {'node_id': node_id})

            self.request('GET', path, **req_kwargs)

    @task(20)
    def get_folder(self):
        self.get_node(self.cloud.folder_ids, name='get folder')

    @task(4)
    def get_file(self):
        self.get_node(self.cloud.file_ids, name='get file')

    def get_node_history(self, ids, **req_kwargs):
        ids = tuple(set(ids) - self.all_instances_last_import_ids)

        if ids:
            node_id = choice(ids)

            delta = self.cloud.last_import_date - self.first_import_date

            ds = self.first_import_date + delta * uniform(-1, 1)
            de = ds + delta * uniform(0.1, 1.5)
            path = url_for(
                url_paths.GET_NODE_HISTORY,
                path_params=dict(node_id=node_id),
                query_params=dict(dateStart=ds, dateEnd=de)
            )
            self.request('GET', path, **req_kwargs)

    @task(20)
    def get_folder_history(self):
        self.get_node_history(self.cloud.folder_ids, name='get folder history')

    @task(4)
    def get_file_history(self):
        self.get_node_history(self.cloud.file_ids, name='get file history')

    @task(24)
    def get_updates(self):
        date = self.cloud.last_import_date + timedelta(hours=23, minutes=59, seconds=58)

        path = url_for(
            url_paths.GET_UPDATES,
            query_params=dict(date=date)
        )
        self.request('GET', path, name='updates')
