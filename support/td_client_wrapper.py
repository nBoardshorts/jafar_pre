# support/td_client_wrapper.py
import tda
from tda import auth
import json
import os
import threading
import time
from gitignore.config import CLIENT_ID, REDIRECT_URI, TOKEN_PATH
from helpers.logging_helper import configure_logging
from contextlib import contextmanager
from httpx import ConnectError
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue

# Purpose:
# 1. Authenticate the user and obtain an API client object to interact with the TD Ameritrade API.
# 2. Manage token updates and maintain the state of the client object.
# 3. Execute tasks with different priorities using ThreadPoolExecutor and PriorityQueue.
#
# Workflow:
# 1. Call the static `get_instance` method of the TD_Client_Wrapper class to retrieve an instance of the class.
# 2. During initialization, the `_authenticate` method is called.
# 3. The `_authenticate` method attempts to read the token data from the token file. If no token data is found, the `_perform_authentication` method is called to perform the authentication process and obtain the token data.
# 4. Whenever the token data is updated, the `_update_token_data` method is called to write the updated token data to the token file.
# 5. The `get_client` method returns the authenticated client object for interacting with the TD Ameritrade API.
# 6. The `submit_task` method is used to add tasks with different priorities to the priority queue.
# 7. The `_execute_tasks` method executes tasks from the priority queue using the ThreadPoolExecutor.
#
# Criteria:
# 1. Authenticate the user and create an API client object for interacting with the TD Ameritrade API.
# 2. Read token data from a token file and use it for authentication, if available.
# 3. Update token data in a token file when needed.
# 4. Provide a method to get the authenticated API client object for use in other parts of the application.
# 5. Execute tasks with different priorities using ThreadPoolExecutor and PriorityQueue.
#
# Usage and Interaction Instructions:
# 1. Call `TD_Client_Wrapper.get_instance()` to retrieve an instance of the TD_Client_Wrapper class.
# 2. Use the `get_client()` method to get the authenticated API client object for interacting with the TD Ameritrade API.
# 3. Define your tasks as separate functions.
# 4. Submit tasks to the TD_Client_Wrapper instance using the `submit_task()` method, specifying the task and its priority.
# 5. The tasks with higher priority will be executed before tasks with lower priority when resources are available.
# 6. Make sure to call the `close()` method on the TD_Client_Wrapper instance when you are done executing tasks to properly shut down the ThreadPoolExecutor.

# Initialize logging for the td_client_wrapper
configure_logging()

class TD_Client_Wrapper:
    """TD Ameritrade API client wrapper class.

        The TD_Client_Wrapper class provides a simple task queue with a priority system and handles authentication with the TD Ameritrade API. The class is implemented as a singleton to ensure that only one instance of the class exists at any given time.

        The class also contains a nested `Instrument` class, which provides access to the `Projection` attribute of the `Client.Instrument` class.

        Attributes:
            _instance (TD_Client_Wrapper): Singleton instance of the TD_Client_Wrapper class.
            _lock (threading.Lock): Lock object used to ensure thread-safety of singleton instance creation.
            Instrument (class): Nested class providing access to the `Projection` attribute of the `Client.Instrument` class.

        Methods:
            __init__: Private method that initializes the attributes of the TD_Client_Wrapper class.
            _authenticate: Authenticate the TD Ameritrade API client.
            _perform_authentication: Perform the TD Ameritrade API authentication process.
            _update_token_data: Update the saved token data with new token data.
            _read_token: Read token data from the saved token file.
            get_client: Return the authenticated TD Ameritrade API client.
            submit_task: Submit a task to the task queue with an optional priority.
            _execute_tasks: Execute tasks in the task queue.
            close: Shutdown the thread pool executor.
            get_instance: Get an instance of the TD_Client_Wrapper class.

        Usage:
            client_wrapper = TD_Client_Wrapper.get_instance()
            client_wrapper.submit_task(task, priority)
            ...
            client_wrapper.close()

            instrument_projection = TD_Client_Wrapper.Instrument.Projection
        """
    _instance = None # Singleton class The __init__ method is private, meaning it can only be accessed from within the class. This ensures that no new instances of the class can be created from outside the class.
    _lock = threading.Lock()

    @contextmanager
    def retry_request(max_retries=3, backoff_factor=1):
        """ Adds a retry mechanism with exponential backoff to any actions with usage:
            with retry_request():
                response = self.td_client.search_instruments
        """
        retry = 0
        while retry <= max_retries:
            try:
                yield
                break
            except ConnectError as e:
                wait_time = backoff_factor * (2 ** retry)
                print(f"Connection error: {e}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                retry += 1
        else:
            raise Exception("Failed to complete request after multiple retries")
        
        
    @classmethod
    def get_instance(cls):
        """ Get an instance of the TD_Client_Wrapper class.

        Gets an instance of the TD_Client_Wrapper class. If an instance already exists, returns that instance. Otherwise, creates a new instance of the class.

        Returns:
            TD_Client_Wrapper: An instance of the TD_Client_Wrapper class.
        """
        with TD_Client_Wrapper._lock:
            if TD_Client_Wrapper._instance is None:
                TD_Client_Wrapper._instance = TD_Client_Wrapper()
        return TD_Client_Wrapper._instance

    def __init__(self):
        """Create an instance of the TD_Client_Wrapper class.

        Creates an instance of the TD_Client_Wrapper class. Raises an exception if an instance of the class already exists. Initializes the `client` attribute to `None`, creates a `Lock` object to ensure thread-safety of token updates, creates a `ThreadPoolExecutor` object with a maximum of 4 workers to execute tasks in the task queue, creates a `PriorityQueue` object to store tasks with priorities, and calls the `_authenticate` method to authenticate the client with the TD Ameritrade API.

        Returns:
            None

        Raises:
            Exception: If an instance of the TD_Client_Wrapper class already exists.
        """
        if TD_Client_Wrapper._instance is not None:
            raise Exception("TD_Client_Wrapper is a Singleton. Please use TD_Client_Wrapper.get_instance() instead.")
        else:
            TD_Client_Wrapper._instance = self
            self.client = None
            self._token_lock = threading.Lock()
            self.executor = ThreadPoolExecutor(max_workers=4)
            self.task_queue = PriorityQueue()
            self._authenticate()

    def _authenticate(self):
        """Authenticate the client by either reading an existing token or performing authentication.

        If an existing token is found, use it to authenticate the client. Otherwise, perform authentication and save the token for future use.

        Returns:
            None

        Raises:
            TDAAuthException: If authentication fails."""
        token_data = self._read_token()
        if token_data is None:
            self._perform_authentication()
        else:
            self.client = tda.auth.client_from_access_functions(
                api_key=CLIENT_ID,
                token_read_func=lambda: token_data,
                token_write_func=self._update_token_data
            )

    def _perform_authentication(self):
        """Perform authentication with the TD Ameritrade API and save the resulting token data.

        Uses the `tda.auth.easy_client` function to perform authentication with the TD Ameritrade API, and then saves the resulting token data using the `_update_token_data` method.

        Returns:
            None

        Raises:
            TDAAuthException: If authentication fails.
        """
        token_data = tda.auth.easy_client(CLIENT_ID, REDIRECT_URI, TOKEN_PATH)
        self.client = tda.auth.client_from_access_functions(
            api_key=CLIENT_ID,
            token_read_func=lambda: token_data,
            token_write_func=self._update_token_data
        )

    def _update_token_data(self, token_data, **kwargs):
        """Update the saved token data with new token data.

        Updates the saved token data with the new token data provided as an argument. The token data is saved in a JSON file located at `TOKEN_PATH`.

        Args:
            token_data (dict): The new token data to be saved.

        Returns:
            None
        """
        with self._token_lock:
            with open(TOKEN_PATH, "w") as token_file:
                json.dump(token_data, token_file)

    def _write_token(self, token_data):
        """Writes the token data to the token file.

        Args:
            token_data (dict): The token data to be written to the file.
        """
        with open('gitignore/td_credentials.json', 'w') as f:
            json.dump(token_data, f)


    def _read_token(self):
        """Read token data from the saved token file.

        Reads the token data from the saved token file located at `TOKEN_PATH` if it exists. If the file does not exist, returns `None`.

        Returns:
            dict: The token data read from the saved token file, or `None` if the file does not exist.
        """
        if os.path.exists(TOKEN_PATH):
            with open(TOKEN_PATH, "r") as token_file:
                token_data = json.load(token_file)
                return token_data
        return None
    
    def get_client(self):
        """Return the authenticated TD Ameritrade API client.

        Returns:
            tda.client.synchronous.TDASyncClient: The authenticated TD Ameritrade API client.
        """
        return self.client
    
    def submit_task(self, task, priority=1):
        """Submit a task to the task queue with an optional priority.

        Puts a tuple of `(priority, task)` onto the task queue, where `priority` is an integer and `task` is a callable object representing the task to be executed. The `_execute_tasks` method is then called to execute the tasks in the queue.

        Args:
            task (callable): The task to be executed.
            priority (int): The priority of the task in the queue. Defaults to 1.

        Returns:
            None
        """
        self.task_queue.put((priority, task))
        self._execute_tasks()

    def _execute_tasks(self):
        """Execute tasks in the task queue.

        Executes the tasks in the task queue in priority order using the `submit` method of the `concurrent.futures.ThreadPoolExecutor` object.

        Returns:
            None
        """
        while not self.task_queue.empty():
            priority, task = self.task_queue.get()
            if task is not None:
                self.executor.submit(task)

    def close(self):
        """Shutdown the thread pool executor.

        Shuts down the thread pool executor and frees any resources used by the executor.

        Returns:
            None
        """
        self.executor.shutdown()             

    def refresh_access_token(self):
        """Refresh the access token and update the token file.

        Returns:
            bool: True if the access token was refreshed successfully, False otherwise.
        """
        token_data = self._read_token()
        if token_data:
            print("Token data:", token_data)  # Add this line to print the token_data
            client_id = CLIENT_ID
            print("Token data before accessing refresh_token:", token_data)  # Add this line
            refresh_token = token_data['token']['refresh_token']
            new_token_data = auth.refresh_token(client_id, refresh_token)
            if new_token_data:
                self._write_token(new_token_data)
                return True
        return False

    
    def get_access_token(self):
        """Return the current access token for the authenticated TD Ameritrade API client.

        Returns:
            str: The access token.
        """
        token_data = self._read_token()
        access_token = token_data.get('access_token', None) if token_data else None
        print(access_token)
        return access_token
    
    # Add any other methods for interacting with the TD Ameritrade API, like placing orders or fetching data, and ensure they're thread-safe by using the ThreadPoolExecutor.
