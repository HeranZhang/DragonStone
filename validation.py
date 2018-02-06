import error

def validate_data_not_empty(data):
    '''
    :param data:
    '''
    if data is None:
        raise error.EmptyDataError("Data is empty.")

def validate_duplicate_column_names(data):
    '''
    :param data: (DataFrame)
    '''
    if data.columns.duplicated.any():
        raise error.DuplicateColumnNameError("Data has duplicate column names.")


def validate_initial_data(data):
    '''
    :param data: (DataFrame)
    '''
    validate_data_not_empty(data)
    validate_duplicate_column_names(data)

