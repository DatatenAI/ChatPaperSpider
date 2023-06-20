import os

import httpx

from dev_run import RequestParams

data_params = RequestParams(task_id='052b2a36-c37d-4ffe-b272-755666cae760',
                            user_type="spider"
                            )

response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
                     params=data_params.dict())