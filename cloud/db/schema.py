from sqlalchemy import (
    Column, DateTime, ForeignKey, ForeignKeyConstraint, Integer,
    String, Table, MetaData, create_engine, select, literal, column, func, text
)
from cloud.utils.pg import DEFAULT_PG_URL

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
    Column('id', Integer, primary_key=True),
    Column('date', DateTime(timezone=True), nullable=False)
)

folders = Table(
    'folders',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id')),
    Column('id', String, primary_key=True),
    Column('parent_id', String),
    Column('size', Integer, default=0),
    ForeignKeyConstraint(('parent_id',), ('folders.id',), ondelete='CASCADE')
)

files = Table(
    'files',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id')),
    Column('id', String, primary_key=True),
    Column('parent_id', String, ForeignKey('folders.id', ondelete='CASCADE')),
    Column('url', String(255), nullable=False),
    Column('size', Integer, nullable=False),
)

folder_history = Table(
    'folder_history',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id'), primary_key=True),
    Column('folder_id', String, ForeignKey('folders.id', ondelete='CASCADE'), primary_key=True),
    Column('parent_id', String),
    Column('size', Integer, default=0),
    ForeignKeyConstraint(('parent_id',), ('folders.id',), ondelete='CASCADE')
)

file_history = Table(
    'file_history',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id'), primary_key=True),
    Column('file_id', String, ForeignKey('files.id', ondelete='CASCADE'), primary_key=True),
    Column('parent_id', String, ForeignKey('folders.id', ondelete='CASCADE')),
    Column('url', String(255), nullable=False),
    Column('size', Integer, nullable=False),
)

if __name__ == '__main__':
    engine = create_engine(DEFAULT_PG_URL, echo=True)

    parent_id = 'd515e43f-f3f6-4471-bb77-6b455017a2d2'
    size = 50

    from unit_test import UPDATE_IMPORT

    ids = (item['id'] for item in UPDATE_IMPORT['items'])


    # print(query)
    # engine.execute(query)
