import os

import httpx

from dev_run import RequestParams

data_params = RequestParams(task_id='00922cdc-3653-43c8-9277-c95a4724d36b',
                            user_type="spider"
                            )



response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
                     params=data_params.dict())

print(f"status: {response.text}")