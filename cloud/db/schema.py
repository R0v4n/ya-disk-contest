from sqlalchemy import (
    Column, DateTime, Enum as PgEnum, ForeignKey, ForeignKeyConstraint, Integer,
    String, Boolean, Table, MetaData
)

from cloud.api.model import NodeType


# todo: read about
convention = {
    'all_column_names': lambda constraint, table: '_'.join([
        column.name for column in constraint.columns.values()
    ]),

    # Именование индексов
    'ix': 'ix__%(table_name)s__%(all_column_names)s',

    # Именование уникальных индексов
    'uq': 'uq__%(table_name)s__%(all_column_names)s',

    # Именование CHECK-constraint-ов
    'ck': 'ck__%(table_name)s__%(constraint_name)s',

    # Именование внешних ключей
    'fk': 'fk__%(table_name)s__%(all_column_names)s__%(referred_table_name)s',

    # Именование первичных ключей
    'pk': 'pk__%(table_name)s'
}
metadata = MetaData(naming_convention=convention)
# Base = declarative_base(metadata=metadata)


imports = Table(
    'imports',
    metadata,
    Column('import_id', Integer, primary_key=True),
    Column('update_date', DateTime(timezone=True), nullable=False)
)

nodes = Table(
    'nodes',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.import_id')),
           # primary_key=True),
    Column('node_id', String, primary_key=True),
    Column('parent_id', String, nullable=True),
    Column('url', String, nullable=True),
    Column('size', Integer, nullable=True),
    Column('type', PgEnum(NodeType, name='type'), nullable=False),
    Column('is_actual', Boolean, default=True, nullable=False),
    ForeignKeyConstraint(('parent_id',), ('nodes.node_id',), ondelete='CASCADE')
)


# class Node(Base):
#     __tablename__ = 'nodes'
#
#     import_id = Column(Integer, ForeignKey('imports.import_id'), primary_key=True)
#     node_id = Column(String, primary_key=True)
#     parent_id = Column(String, ForeignKey('nodes.node_id'), nullable=True)
#     url = Column(String, nullable=True)
#     size = Column(Integer, nullable=True)
#     node_type = Column(PgEnum(NodeType, name='node_type'), nullable=False)
#     is_actual = Column(Boolean, default=True, nullable=False)
#
#     children = relationship('nodes')
