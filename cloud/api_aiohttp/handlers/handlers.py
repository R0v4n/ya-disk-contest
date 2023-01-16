from datetime import datetime

from aiohttp.web_response import Response
from aiohttp_pydantic.oas.typing import r200, r404, r400

from cloud import model
from cloud.resources import url_paths
from .base import PydanticView


class ImportsView(PydanticView):
    URL_PATH = url_paths.IMPORTS
    ModelT = model.ImportModel

    async def post(self, data: model.RequestImport) -> r200 | r400[model.Error]:
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
          - при обновлении параметров элемента обязательно обновляется поле **date** в соответствии с временем обновления
          - в одном запросе не может быть двух элементов с одинаковым id
          - дата обрабатывается согласно ISO 8601 (такой придерживается OpenAPI).
          Если дата не удовлетворяет данному формату, ответом будет код 400.

        Гарантируется, что во входных данных нет циклических зависимостей и поле updateDate монотонно возрастает.
        Гарантируется, что при проверке передаваемое время кратно секундам.

        Status codes:
            200: Вставка или обновление прошли успешно.
            400: Невалидная схема документа или входные данные не верны.
        """
        mdl = self.ModelT(data)

        # fixme: need refactor like in fastapi route
        async with self.pg.transaction() as conn:
            await mdl.init(conn)
            await mdl.execute_post_import()

        return Response()


class NodeView(PydanticView):
    URL_PATH = url_paths.GET_NODE

    async def get(self, node_id: str, /) -> r200[model.ResponseNodeTree] | r404[model.Error] | r400[model.Error]:
        """
        Получить информацию об элементе по идентификатору.
        При получении информации о папке также предоставляется информация о её дочерних элементах.

        Status codes:
            200: Информация об элементе.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        mdl = model.NodeModel(node_id)
        await mdl.init(self.pg)
        node = (await mdl.get_node()).dict(by_alias=True)
        return Response(body=node)


class DeleteNodeView(PydanticView):
    URL_PATH = url_paths.DELETE_NODE
    ModelT = model.NodeImportModel

    async def delete(
            self,
            node_id: str, /,
            date: datetime
    ) -> r200 | r404[model.Error] | r400[model.Error]:
        """
        Удалить элемент по идентификатору. При удалении папки удаляются все дочерние элементы.
        Доступ к истории обновлений удаленного элемента невозможен.

        Status codes:
            200: Удаление прошло успешно.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        mdl = self.ModelT(node_id, date)
        async with self.pg.transaction() as conn:
            await mdl.init(conn)
            await mdl.execute_delete_node()
        return Response()


class UpdatesView(PydanticView):
    URL_PATH = url_paths.GET_UPDATES

    async def get(self, date: datetime) -> r200[model.ListResponseItem] | r400[model.Error]:
        """
        Получение списка файлов, которые были обновлены за последние 24 часа включительно [date - 24h, date]
        от времени переданном в запросе.

        Status codes:
            200: Список элементов, которые были обновлены.
            400: Невалидная схема документа или входные данные не верны
        """
        mdl = model.HistoryModel(date)
        await mdl.init(self.pg)
        items = await mdl.get_files_updates()

        return Response(body=items.dict(by_alias=True))


class NodeHistoryView(PydanticView):
    URL_PATH = url_paths.GET_NODE_HISTORY

    # noinspection PyPep8Naming
    async def get(
            self,
            node_id: str, /,
            dateStart: datetime,
            dateEnd: datetime
    ) -> r200[model.ListResponseItem] | r400[model.Error] | r404[model.Error]:
        """
        Получение истории обновлений по элементу за заданный полуинтервал [from, to).
        История по удаленным элементам недоступна.

        Status codes:
            200: История по элементу.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        mdl = model.NodeModel(node_id)
        await mdl.init(self.pg)
        items = await mdl.get_node_history(dateStart, dateEnd)

        return Response(body=items.dict(by_alias=True))
