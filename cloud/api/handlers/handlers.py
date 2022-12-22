from datetime import datetime

from aiohttp.web_response import Response
from aiohttp_pydantic.oas.typing import r200, r404, r400

from .base import BasePydanticView
from ..model import ImportData, ImportModel, NodeModel, ExportNodeTree, HistoryModel, Error
from ..model.data_classes import ExportItem


class ImportsView(BasePydanticView):
    URL_PATH = '/imports'
    ModelT = ImportModel

    async def post(self, data: ImportData) -> r200 | r400[Error]:
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

        async with self.pg.transaction() as conn:
            await mdl.init(conn)
            await mdl.execute_post_import()

        return Response()


class NodeView(BasePydanticView):
    URL_PATH = r'/nodes/{node_id}'

    async def get(self, node_id: str, /) -> r200[ExportNodeTree] | r404[Error] | r400[Error]:
        """
        Получить информацию об элементе по идентификатору.
        При получении информации о папке также предоставляется информация о её дочерних элементах.

        Status codes:
            200: Информация об элементе.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        mdl = NodeModel(node_id)
        await mdl.init(self.pg)
        node = await mdl.get_node()
        return Response(body=node)


class DeleteNodeView(BasePydanticView):
    URL_PATH = r'/delete/{node_id}'
    ModelT = NodeModel

    async def delete(
            self,
            node_id: str, /,
            date: datetime
    ) -> r200 | r404[Error] | r400[Error]:
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


class UpdatesView(BasePydanticView):
    URL_PATH = r'/updates'

    async def get(self, date: datetime) -> r200[ExportItem] | r400[Error]:
        """
        Получение списка файлов, которые были обновлены за последние 24 часа включительно [date - 24h, date]
        от времени переданном в запросе.

        Status codes:
            200: Список элементов, которые были обновлены.
            400: Невалидная схема документа или входные данные не верны
        """
        mdl = HistoryModel(self.pg, date)
        nodes = await mdl.get_files_updates_24h()

        return Response(body=nodes)


class NodeHistoryView(BasePydanticView):
    URL_PATH = r'/node/{node_id}/history'

    # noinspection PyPep8Naming
    async def get(
            self,
            node_id: str, /,
            dateStart: datetime,
            dateEnd: datetime
    ) -> r200[ExportItem] | r400[Error] | r404[Error]:
        """
        Получение истории обновлений по элементу за заданный полуинтервал [from, to).
        История по удаленным элементам недоступна.

        Status codes:
            200: История по элементу.
            400: Невалидная схема документа или входные данные не верны.
            404: Элемент не найден.
        """
        mdl = NodeModel(node_id)
        await mdl.init(self.pg)
        nodes = await mdl.get_node_history(dateStart, dateEnd)

        return Response(body=nodes)
