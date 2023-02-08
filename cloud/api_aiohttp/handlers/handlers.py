from datetime import datetime

from aiohttp.web_response import Response
from aiohttp_pydantic.oas.typing import r200, r404, r400

from cloud import services
from cloud import models
from cloud.resources import url_paths
from .base import PydanticView


class ImportsView(PydanticView):
    URL_PATH = url_paths.IMPORTS
    ServiceT = services.ImportService

    async def post(self, data: models.RequestImport) -> r200 | r400[models.Error]:
        """
        Импортирует элементы файловой системы. Элементы импортированные повторно обновляют текущие.
        Изменение типа элемента с папки на файл и с файла на папку не допускается.
        Порядок элементов в запросе является произвольным.

          - id каждого элемента является уникальным среди остальных элементов
          - поле id не может быть равно null
          - родителем элемента может быть только папка
          - принадлежность к папке определяется полем parentId
          - элементы могут не иметь родителя (при обновлении parentId на null элемент остается без родителя)
          - поле url при импорте папки всегда должно быть равно null
          - размер поля url при импорте файла всегда должен быть меньше либо равным 255
          - поле size при импорте папки всегда должно быть равно null
          - поле size для файлов всегда должно быть больше 0
          - при обновлении элемента обновленными считаются **все** их параметры
          - при обновлении параметров элемента обязательно обновляется поле **date**
            в соответствии с временем обновления
          - в одном запросе не может быть двух элементов с одинаковым id
          - дата обрабатывается согласно ISO 8601 (такой придерживается OpenAPI).
          Если дата не удовлетворяет данному формату, ответом будет код 400.

        Гарантируется, что во входных данных нет циклических зависимостей и поле updateDate монотонно возрастает.
        Гарантируется, что при проверке передаваемое время кратно секундам.

        Status codes:
            200: Вставка или обновление прошли успешно.
            400: Невалидная схема документа или входные данные не верны.
        """
        service = self.ServiceT(self.pg, data)
        await service.execute_post_import()
        return Response()


class NodeView(PydanticView):
    URL_PATH = url_paths.GET_NODE

    async def get(self, node_id: str, /) -> r200[models.ResponseNodeTree] | r404[models.Error] | r400[models.Error]:
        """
        Получить информацию об элементе по идентификатору.
        При получении информации о папке также предоставляется информация о её дочерних элементах.

        Status codes:
            200: Информация об элементе.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        service = services.NodeService(self.pg, node_id)
        node = await service.get_node()
        return Response(body=node.dict(by_alias=True))


class DeleteNodeView(PydanticView):
    URL_PATH = url_paths.DELETE_NODE
    ServiceT = services.NodeImportService

    async def delete(
            self,
            node_id: str, /,
            date: datetime
    ) -> r200 | r404[models.Error] | r400[models.Error]:
        """
        Удалить элемент по идентификатору. При удалении папки удаляются все дочерние элементы.
        Доступ к истории обновлений удаленного элемента невозможен.

        Status codes:
            200: Удаление прошло успешно.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        service = self.ServiceT(self.pg, node_id, date)
        await service.execute_delete_node()
        return Response()


class UpdatesView(PydanticView):
    URL_PATH = url_paths.GET_UPDATES

    async def get(self, date: datetime) -> r200[models.ListResponseItem] | r400[models.Error]:
        """
        Получение списка файлов, которые были обновлены за последние 24 часа включительно [date - 24h, date]
        от времени переданном в запросе.

        Status codes:
            200: Список элементов, которые были обновлены.
            400: Невалидная схема документа или входные данные не верны
        """
        service = services.HistoryService(self.pg, date)
        items = await service.get_files_updates()
        return Response(body=items.dict(by_alias=True))


class NodeHistoryView(PydanticView):
    URL_PATH = url_paths.GET_NODE_HISTORY

    # noinspection PyPep8Naming
    async def get(
            self,
            node_id: str, /,
            dateStart: datetime,
            dateEnd: datetime
    ) -> r200[models.ListResponseItem] | r400[models.Error] | r404[models.Error]:
        """
        Получение истории обновлений по элементу за заданный полуинтервал [from, to).
        История по удаленным элементам недоступна.

        Status codes:
            200: История по элементу.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        service = services.NodeService(self.pg, node_id)
        items = await service.get_node_history(dateStart, dateEnd)

        return Response(body=items.dict(by_alias=True))
