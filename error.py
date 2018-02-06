
class Error(Exception):
    '''Base exception.'''


class ValidationError(Error):
    '''
    Base validation error.

    Args:
        * message: error message.
        * error_message: the actual error message that was raised.
        * error_type: error type.

    Attribute:
        * msg: The error message.
        * error_message: The actual error message that was raised, as a string.

    :param message:
    :param error_message:
    :param error_type:
    '''
    def __init__(self, message,
                 error_message=None,
                 error_type=None):
        Error.__init__(self, message)
        self.error_message = error_message or message
        self.error_type = error_type

class EmptyDataError(ValidationError):
    '''
    Data is empty
    '''

class DuplicateColumnNameError(ValidationError):
    '''
    Data has duplicate column names.
    '''
