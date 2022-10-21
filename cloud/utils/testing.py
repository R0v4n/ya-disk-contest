from cloud.api.model.data_classes import ImportNode

if __name__ == '__main__':
    from pydantic_factories import ModelFactory

    class NodeFactory(ModelFactory):
        __model__ = ImportNode


    res = NodeFactory.build()
