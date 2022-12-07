from sqlalchemy import (
    Column, DateTime, ForeignKey, ForeignKeyConstraint,
    Integer, BigInteger, String, Table, MetaData
)

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

# todo: DRY!
imports_table = Table(
    'imports',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('date', DateTime(timezone=True), nullable=False, index=True)
)

folders_table = Table(
    'folders',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id')),
    Column('id', String, primary_key=True),
    Column('parent_id', String),
    Column('size', BigInteger, default=0),
    ForeignKeyConstraint(('parent_id',), ('folders.id',), ondelete='CASCADE')
)

files_table = Table(
    'files',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id')),
    Column('id', String, primary_key=True),
    Column('parent_id', String, ForeignKey('folders.id', ondelete='CASCADE')),
    Column('url', String(255), nullable=False),
    Column('size', BigInteger, nullable=False),
)

folder_history = Table(
    'folder_history',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id'), primary_key=True),
    Column('folder_id', String, ForeignKey('folders.id', ondelete='CASCADE'), primary_key=True),
    Column('parent_id', String),
    Column('size', BigInteger, default=0),
)

file_history = Table(
    'file_history',
    metadata,
    Column('import_id', Integer, ForeignKey('imports.id'), primary_key=True),
    Column('file_id', String, ForeignKey('files.id', ondelete='CASCADE'), primary_key=True),
    Column('parent_id', String),
    Column('url', String(255), nullable=False),
    Column('size', BigInteger, nullable=False),
)
