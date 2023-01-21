import logging
from datetime import datetime, timedelta
from http import HTTPStatus
from random import choice, randint, uniform

import requests
from locust import task, FastHttpUser, constant, events
from locust.runners import MasterRunner

from cloud.resources import url_paths
from cloud.settings import Settings
from cloud.utils.testing import FakeCloudGen, url_for

API_HOST = f'http://localhost:{Settings().api_port}'

cloud: FakeCloudGen | None = None
first_import_date: datetime | None = None


# todo: check asyncpgsa logs and fix queries

@events.test_start.add_listener
def on_test_start(environment, **_kwargs):
    global cloud, first_import_date
    if not isinstance(environment.runner, MasterRunner):
        cloud = FakeCloudGen(write_history=False)
        top_folders_count = 10
        cloud.generate_import(*[[] for _ in range(top_folders_count)])
        first_import_date = cloud.last_import_date

        response = requests.post(API_HOST + url_paths.IMPORTS, json=cloud.get_import_dict())
        if response.status_code == HTTPStatus.OK:
            logging.info('created %d top folders', top_folders_count)
        else:
            logging.info('failed to create top folders, status code: %d', response.status_code)


class User(FastHttpUser):
    _amount = 0
    wait_time = constant(0.2)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__class__._amount += 1
        self.id = self.__class__._amount

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

    @task(7)
    def post_import(self):
        cloud.random_import(schemas_count=2)
        # todo: think about to do top folders untouchable
        cloud.random_updates(count=5)

        data = cloud.get_import_dict()
        print(f'POST: date={data["updateDate"]}', self.id)
        self.request('POST', url_paths.IMPORTS, json=data)

    @task(1)
    def delete_node(self, node_id: str = None):
        if node_id is None:
            ids = cloud.ids
            if ids:
                node_id = choice(ids)

        if node_id:
            date = cloud.del_item(node_id)
            path = url_for(url_paths.DELETE_NODE, {'node_id': node_id}, {'date': date})
            print(f'DELETE: date={date}', self.id)

            self.request('DELETE', path, name=url_paths.DELETE_NODE)

    def get_node(self, node_id: str = None, ids: list[str] | tuple[str] = None, **req_kwargs):
        if node_id is None:
            if ids is None:
                ids = cloud.ids
            if ids:
                node_id = choice(ids)

        if node_id:
            path = url_for(url_paths.GET_NODE, {'node_id': node_id})
            self.request('GET', path, **req_kwargs)

    @task(18)
    def get_folder(self, folder_id: str = None):
        self.get_node(folder_id, cloud.folder_ids, name='get_folder')

    @task(2)
    def get_file(self, file_id: str = None):
        self.get_node(file_id, cloud.file_ids, name='get_file')

    @task(20)
    def get_node_history(self):
        ids = cloud.ids
        if ids:
            node_id = choice(ids)

            delta = cloud.last_import_date - first_import_date

            ds = first_import_date + delta * uniform(-1, 1)
            de = ds + delta * uniform(0.1, 1.5)
            print('NODE HISTORY:', ds, de)
            path = url_for(
                url_paths.GET_NODE_HISTORY,
                path_params=dict(node_id=node_id),
                query_params=dict(dateStart=ds, dateEnd=de)
            )
            self.request('GET', path, name='node_history')

    @task(8)
    def get_updates(self):
        date = first_import_date + uniform(0, 1) * (cloud.last_import_date - first_import_date)
        date += timedelta(hours=randint(0, 24))

        path = url_for(
            url_paths.GET_UPDATES,
            query_params=dict(date=date)
        )
        self.request('GET', path, name='updates')

    # todo: on_start each user creates top folder
