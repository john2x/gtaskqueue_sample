import os

def get_env_variable(var):
    """
    Get value of environment variable. Raise exception if not present.
    """
    value = os.environ.get(var)
    assert value is not None, 'Set the environment variable $' + var
    return value
