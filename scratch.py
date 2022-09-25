from dateutil import parser
import dateutil

from cloud.api.model import NodeType
from marshmallow import Schema, fields


s = '2022-02-01T12:00:00Z'
t = parser.parse(s)


class TestSchema(Schema):
    title = fields.Str()
    date = fields.DateTime()


data = {'title':'name', 'date':s}
a = TestSchema().load(data)
print(a)
