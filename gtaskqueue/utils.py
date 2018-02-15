import os

def get_env_variable(var):
    """
    Get value of environment variable. Raise exception if not present.
    """
    value = os.environ.get(var)
    assert value is not None, 'Set the environment variable $' + var
    return value


def build_cloudtasks_project_name(project_name, project_location, queue_name, task_id=None):
    s = 'projects/%s/locations/%s/queues/%s/tasks' % (project_name, project_location, queue_name)
    if task_id:
        s += '/' + task_id
    return s
