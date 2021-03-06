#!/usr/bin/env python
#
# Copyright (C) 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Commands to interact with the Task object of the TaskQueue API."""


__version__ = '0.0.1'



from gtaskqueue.taskqueue_cmd_base import GoogleTaskCommand

from google.apputils import app
from google.apputils import appcommands
from gtaskqueue.utils import build_cloudtasks_queue_name, build_cloudtasks_task_name
import gflags as flags

FLAGS = flags.FLAGS


class GetTaskCommand(GoogleTaskCommand):
    """Get properties of an existing task."""

    def __init__(self, name, flag_values):
        super(GetTaskCommand, self).__init__(name, flag_values)

    def build_request(self, task_api, flag_values):
        """Build a request to get properties of a Task.

        Args:
            task_api: The handle to the task collection API.
            flag_values: The parsed command flags.
        Returns:
            The properties of the task.
        """
        name = build_cloudtasks_task_name(flag_values.project_name, flag_values.project_location, flag_values.taskqueue_name,
                                          task_id=flag_values.task_name)
        return task_api.get(name=name)


class LeaseTaskCommand(GoogleTaskCommand):
    """Lease a new task from the queue."""

    def __init__(self, name, flag_values):
        flags.DEFINE_integer('lease_secs',
                             None,
                             'The lease for the task in seconds',
                             flag_values=flag_values)
        flags.DEFINE_integer('num_tasks',
                             1,
                             'The number of tasks to lease',
                             flag_values=flag_values)
        flags.DEFINE_integer('payload_size_to_display',
                             2 * 1024 * 1024,
                             'Size of the payload for leased tasks to show',
                             flag_values=flag_values)
        super(LeaseTaskCommand, self).__init__(name,
                                               flag_values,
                                               need_task_flag=False)

    def build_request(self, task_api, flag_values):
        """Build a request to lease a pending task from the TaskQueue.

        Args:
            task_api: The handle to the task collection API.
            flag_values: The parsed command flags.
        Returns:
            A new leased task.
        """
        if not flag_values.lease_secs:
            raise app.UsageError('lease_secs must be specified')

        parent = build_cloudtasks_task_name(flag_values.project_name, flag_values.project_location, flag_values.taskqueue_name)
        body = {
            'maxTasks': flag_values.num_tasks,
            'leaseDuration': '%ss' % flag_values.lease_secs,
            'responseView': 'FULL',
        }
        return task_api.lease(parent=parent,
                              body=body)

    def print_result(self, result):
        """Override to optionally strip the payload since it can be long."""
        if result.get('tasks'):
            items = []
            for task in result.get('tasks'):
                payloadlen = len(task.get('pullMessage', {}).get('payload', ''))
                if payloadlen > FLAGS.payload_size_to_display:
                    extra = payloadlen - FLAGS.payload_size_to_display
                    task['payload'] = ('%s(%d more bytes)' %
                        (task['payload'][:FLAGS.payload_size_to_display],
                         extra))
                items.append(task)
            result['tasks'] = items
        GoogleTaskCommand.print_result(self, result)


class DeleteTaskCommand(GoogleTaskCommand):
    """Delete an existing task."""

    def __init__(self, name, flag_values):
        super(DeleteTaskCommand, self).__init__(name, flag_values)

    def build_request(self, task_api, flag_values):
        """Build a request to delete a Task.

        Args:
            task_api: The handle to the taskqueue collection API.
            flag_values: The parsed command flags.
        Returns:
            Whether the delete was successful.
        """
        name = build_cloudtasks_task_name(flag_values.project_name, flag_values.project_location, flag_values.taskqueue_name,
                                          task_id=flag_values.task_name)
        return task_api.delete(name=name)


class ListTasksCommand(GoogleTaskCommand):
    """Lists all tasks in a queue (currently upto a max of 100)."""

    def __init__(self, name, flag_values):
        super(ListTasksCommand, self).__init__(name,
                                               flag_values,
                                               need_task_flag=False)

    def build_request(self, task_api, flag_values):
        """Build a request to lists tasks in a queue.

        Args:
            task_api: The handle to the taskqueue collection API.
            flag_values: The parsed command flags.
        Returns:
          A list of pending tasks in the queue.
        """
        parent = build_cloudtasks_queue_name(flag_values.project_name, flag_values.project_location, flag_values.taskqueue_name)
        return task_api.list(parent=parent,
                             responseView='FULL')


class ClearTaskQueueCommand(GoogleTaskCommand):
    """Deletes all tasks in a queue (default to a max of 100)."""


    def __init__(self, name, flag_values):
        flags.DEFINE_integer('max_delete', 100, 'How many to clear at most',
                             flag_values=flag_values)
        super(ClearTaskQueueCommand, self).__init__(name,
                                                    flag_values,
                                                    need_task_flag=False)

    def run_with_api_and_flags(self, api, flag_values):
        """Run the command, returning the result.

        Args:
            api: The handle to the Google TaskQueue API.
            flag_values: The parsed command flags.
        Returns:
            The result of running the command.
        """
        tasks_api = api.tasks()
        self._flag_values = flag_values
        self._to_delete = flag_values.max_delete
        total_deleted = 0
        while self._to_delete > 0:
          n_deleted = self._delete_a_batch(tasks_api)
          if n_deleted <= 0:
            break
          total_deleted += n_deleted
        return {'deleted': total_deleted}

    def _delete_a_batch(self, tasks):
        """Delete a batch of tasks.

        Since the list method only gives us back 100 at a time, we may have
        to call it several times to clear the entire queue.

        Args:
            tasks: The handle to the Google TaskQueue API Tasks resource.
        Returns:
            The number of tasks deleted.
        """
        parent = build_cloudtasks_task_name(self._flag_values.project_name, self._flag_values.project_location, self._flag_values.taskqueue_name)
        list_request = tasks.list(parent=parent,
                                  responseView='BASIC',
                                  pageSize=100)
        result = list_request.execute()
        n_deleted = 0
        if result:
          for task in result.get('tasks', []):
            if self._to_delete > 0:
              self._to_delete -= 1
              n_deleted += 1
              print 'Deleting: %s' % task['name']
              tasks.delete(name=task['name']).execute()
        return n_deleted


def add_commands():
    appcommands.AddCmd('listtasks', ListTasksCommand)
    appcommands.AddCmd('gettask', GetTaskCommand)
    appcommands.AddCmd('deletetask', DeleteTaskCommand)
    appcommands.AddCmd('leasetask', LeaseTaskCommand)
    appcommands.AddCmd('clear', ClearTaskQueueCommand)
