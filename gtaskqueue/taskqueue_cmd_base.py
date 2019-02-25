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


"""Commands for interacting with Google TaskQueue."""

__version__ = '0.0.1'


import os
import sys
import urlparse
import json


from apiclient.discovery import build
from apiclient.errors import HttpError
import httplib2
from oauth2client.service_account import ServiceAccountCredentials
from gtaskqueue.old_run import run

from google.apputils import app
from google.apputils import appcommands
import gflags as flags


SCOPES = ['https://www.googleapis.com/auth/cloud-tasks']
FLAGS = flags.FLAGS

flags.DEFINE_string(
        'service_version',
        'v2beta2',
        'Google taskqueue api version.')
flags.DEFINE_string(
        'api_host',
        'https://cloudtasks.googleapis.com/',
        'API host name')
flags.DEFINE_string(
        'project_name',
        'default',
        'The name of the Taskqueue API project.')
flags.DEFINE_string(
        'project_location',
        'us-central1',
        'The location of the Cloud Tasks project')
flags.DEFINE_bool(
        'use_developer_key',
        False,
        'User wants to use the developer key while accessing taskqueue apis')
flags.DEFINE_string(
        'developer_key_file',
        '~/.taskqueue.apikey',
        'Developer key provisioned from api console')
flags.DEFINE_bool(
        'dump_request',
        False,
        'Prints the outgoing HTTP request along with headers and body.')
flags.DEFINE_string(
        'service_account_file',
        'service_account.json',
        'Service account key file in json format')
flags.DEFINE_string(
    'taskqueue_name',
    'myqueue',
    'TaskQueue name')
flags.DEFINE_string(
    'task_name',
    None,
    'Task name')

class GoogleTaskQueueCommandBase(appcommands.Cmd):
    """Base class for all the Google TaskQueue client commands."""

    DEFAULT_PROJECT_PATH = 'projects/default'

    def __init__(self, name, flag_values):
        super(GoogleTaskQueueCommandBase, self).__init__(name, flag_values)

    def _dump_request_wrapper(self, http):
        """Dumps the outgoing HTTP request if requested.

        Args:
            http: An instance of httplib2.Http or something that acts like it.

        Returns:
            httplib2.Http like object.
        """
        request_orig = http.request

        def new_request(uri, method='GET', body=None, headers=None,
                        redirections=httplib2.DEFAULT_MAX_REDIRECTS,
                        connection_type=None):
            """Overrides the http.request method to add some utilities."""
            if (FLAGS.api_host + "discovery/" not in uri and
                FLAGS.use_developer_key):
                developer_key_path = os.path.expanduser(
                    FLAGS.developer_key_file)
                if not os.path.isfile(developer_key_path):
                    print 'Please generate developer key from the Google APIs' \
                    'Console and store it in %s' % (FLAGS.developer_key_file)
                    sys.exit()
                developer_key_file = open(developer_key_path, 'r')
                try:
                    developer_key = developer_key_file.read().strip()
                except IOError, io_error:
                    print 'Error loading developer key from file %s' % (
                            FLAGS.developer_key_file)
                    print 'Error details: %s' % str(io_error)
                    sys.exit()
                finally:
                    developer_key_file.close()
                s = urlparse.urlparse(uri)
                query = 'key=' + developer_key
                if s.query:
                    query = s.query + '&key=' + developer_key
                d = urlparse.ParseResult(s.scheme,
                                         s.netloc,
                                         s.path,
                                         s.params,
                                         query,
                                         s.fragment)
                uri = urlparse.urlunparse(d)

            if FLAGS.dump_request:
                print '--request-start--'
                print '%s %s' % (method, uri)
                if headers:
                    for (h, v) in headers.iteritems():
                        print '%s: %s' % (h, v)
                print ''
                if body:
                    print json.dumps(json.loads(body), sort_keys=True, indent=2)
                print '--request-end--'
            return request_orig(uri,
                                method,
                                body,
                                headers,
                                redirections,
                                connection_type)
        http.request = new_request
        return http

    def Run(self, argv):
        """Run the command, printing the result.

        Args:
            argv: The non-flag arguments to the command.
        """
        if not FLAGS.project_name:
            raise app.UsageError('You must specify a project name'
                                 ' using the "--project_name" flag.')
        discovery_uri = (
                FLAGS.api_host + 'discovery/v1/apis/{api}/{apiVersion}/rest')
        try:
            # Load the credentials from the service account credentials json file
            credentials = ServiceAccountCredentials.from_json_keyfile_name(FLAGS.service_account_file, scopes=SCOPES)
            http = credentials.authorize(self._dump_request_wrapper(
                    httplib2.Http()))
            api = build('cloudtasks',
                       FLAGS.service_version,
                       http=http,
                       discoveryServiceUrl=discovery_uri)
            result = self.run_with_api_and_flags_and_args(api, FLAGS, argv)
            self.print_result(result)
        except HttpError, http_error:
            print 'Error Processing request: %s' % str(http_error)

    def run_with_api_and_flags_and_args(self, api, flag_values, unused_argv):
        """Run the command given the API, flags, and args.

        The default implementation of this method discards the args and
        calls into run_with_api_and_flags.

        Args:
            api: The handle to the Google TaskQueue API.
            flag_values: The parsed command flags.
            unused_argv: The non-flag arguments to the command.
        Returns:
            The result of running the command
        """
        return self.run_with_api_and_flags(api, flag_values)

    def print_result(self, result):
        """Pretty-print the result of the command.

        The default behavior is to dump a formatted JSON encoding
        of the result.

        Args:
            result: The JSON-serializable result to print.
        """
        # We could have used the pprint module, but it produces
        # noisy output due to all of our keys and values being
        # unicode strings rather than simply ascii.
        print json.dumps(result, sort_keys=True, indent=2)



class GoogleTaskQueueCommand(GoogleTaskQueueCommandBase):
    """Base command for working with the taskqueues collection."""

    def __init__(self, name, flag_values):
        super(GoogleTaskQueueCommand, self).__init__(name, flag_values)

    def run_with_api_and_flags(self, api, flag_values):
        """Run the command, returning the result.

        Args:
            api: The handle to the Google TaskQueue API.
            flag_values: The parsed command flags.
        Returns:
            The result of running the command.
        """
        taskqueue_request = self.build_request(api.projects().locations().queues(), flag_values)
        return taskqueue_request.execute()


class GoogleTaskCommand(GoogleTaskQueueCommandBase):
    """Base command for working with the tasks collection."""

    def __init__(self, name, flag_values, need_task_flag=True):
        super(GoogleTaskCommand, self).__init__(name, flag_values)

    def run_with_api_and_flags(self, api, flag_values):
        """Run the command, returning the result.

        Args:
            api: The handle to the Google TaskQueue API.
            flag_values: The parsed command flags.
            flags.DEFINE_string('payload',
            None,
            'Payload of the task')
        Returns:
            The result of running the command.
        """
        task_request = self.build_request(api.projects().locations().queues().tasks(), flag_values)
        return task_request.execute()
