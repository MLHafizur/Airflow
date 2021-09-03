import requests
from airflow.hooks.base_hook import BaseHook

class MovielenHook(BaseHook):

    DEFAULT_SCHEMA = 'http'
    DEFAULT_PORT = 80

    def __init__(self, conn_id, retry=3):
        super().__init__()
        self._conn_id = conn_id
        self._retry = retry

        self._session = None
        self._base_url = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_conn(self):
        if self._session is None:
            config = self.get_connection(self._conn_id)
        
            if not config.host:
                raise ValueError('Missing host')
        
            schema = config.schema if config.schema else self.DEFAULT_SCHEMA
            port = config.port if config.port else self.DEFAULT_PORT

            self._base_url = '{}://{}:{}'.format(schema, config.host, port)

            self._session = requests.Session()

            if config.login:
                self._session.auth = (config.login, config.password)
        return self._session, self._base_url
    
    def close(self):
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None
    
    
    # API methods

    def get_movies(self):
        raise NotImplementedError()
    
    def get_users(self):
        raise NotImplementedError()
    

    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        yield from self._get_with_pagination(
            endpoint='/ratings',
            params={'start_date': start_date, 'end_date': end_date, 'batch_size': batch_size}
        )
    
    def _get_with_pagination(self, endpoint, params, batch_size=100):
        """
        Fetches records using a get request with given url/params,
        taking pagination into account.
        """

        session, base_url = self.get_conn()
        url = base_url + endpoint

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(
                url, params={**params, **{"offset": offset, "limit": batch_size}}
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"]

            offset += batch_size
            total = response_json["total"]