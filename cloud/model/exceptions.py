class ModelError(Exception):
    status_code = 500


class NotInitializedError(ModelError):
    pass


class ItemNotFoundError(ModelError):
    status_code = 404


class ModelValidationError(ModelError):
    status_code = 400


class ParentNotFoundError(ModelValidationError):
    """
    Derived from ModelValidationError due to the task.
    It can be raised only in POST /imports that responses only with 200 and 400 status codes.
    """
