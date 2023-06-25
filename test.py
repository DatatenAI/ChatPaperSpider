import os

import httpx

from dev_run import RequestParams

# test spider task
# data_params = RequestParams(task_id='113',
#                             user_type="spider"
#                             )
#
# response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
#                      params=data_params.dict())

# test user task

data_params = RequestParams(task_id='2',
                            user_type="user"
                            )

response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
                     params=data_params.dict())

