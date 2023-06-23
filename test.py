import os

import httpx

from dev_run import RequestParams

# data_params = RequestParams(task_id='00922cdc-3653-43c8-9277-c95a4724d36b',
#                             user_type="spider"
#                             )
#
# data_params = RequestParams(task_id='3',
#                             user_type="spider"
#                             )
#
#
# response = httpx.get(os.getenv("FUNCTION_ENDPOINT"),
#                      params=data_params.dict())

# print(f"status: {response.text}")


print(len(f"""Based on the current text content {1}, you should determine which section the text belongs to "Methods, Experimental settings, and Experimental details" or others.     If there is a corresponding section, you should output a specific description; if not, a section is ignored.     If the text contains the Method section, you need to summarize the method in detail, step by step.      If the text contains experimental setting, you need to summarize the experimental setting in detail.     If the text contains experimental results, you need to summarize the experimental performance according to current text.     Remember:                                                                         - You must keep concise and clear, and output as English.     - You should maintain the key data, nouns, settings, and other specific valuable information in the original text, and retain the original logic and correspondence.     - Do not output any specific data when the current text does not exist!     - In short, make sure your output is comprehensive and accurate!     - Output as following format:     Section Name:         Content.              """))
