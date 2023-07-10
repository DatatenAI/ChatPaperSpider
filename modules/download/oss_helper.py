import asyncio
import os
from dotenv import load_dotenv
import oss2
from loguru import logger

if os.getenv('ENV') == 'DEV':
    load_dotenv()




async def upload_to_oss(local_file_path: str,
                        oss_folder: str,
                        oss_file_path: str):
    try:
        endpoint = os.environ.get('OSS_ENDPOINT')
        bucket_name = os.environ.get('OSS_BUCKET')
        access_key_id = os.environ.get('OSS_ACCESS_KEY')
        access_key_secret = os.environ.get('OSS_ACCESS_SECRET')

        # 创建阿里云OSS客户端
        logger.info(f"upload oss, folder:{oss_folder}")
        logger.info(f"oss_file_path:{oss_file_path}")
        logger.info(f"oss bucket name:{bucket_name}")
        auth = oss2.Auth(access_key_id, access_key_secret)
        bucket = oss2.Bucket(auth, endpoint, bucket_name)
        logger.info(f"bucket:{bucket}")
        oss_remote_path = oss_folder + "/" + oss_file_path
        logger.info(f"oss remote path:{oss_remote_path}")
        # 上传文件
        bucket.put_object_from_file(oss_remote_path, local_file_path)

        print('File uploaded successfully.')
    except Exception as e:
        logger.error(f"error:{repr(e)}")


async def test():
    # 调用示例
    from dotenv import load_dotenv
    load_dotenv()

    local_file_path = '/home/rongkang/WorkSpace/WebSite/ChatPaperDevelop/ChatPaperSpider/modules/download/images/d97d2668bf6b22409e2f8a0c4b9f3290/1_74.png'
    file_name = os.path.basename(local_file_path)
    parent_folder = os.path.dirname(local_file_path)
    hash_folder = os.path.basename(parent_folder)
    oss_file_path = os.path.join(hash_folder, file_name)  # has

    await upload_to_oss(local_file_path, oss_folder='images', oss_file_path=oss_file_path)

    pdf_file = "/home/rongkang/WorkSpace/WebSite/ChatPaperDevelop/uploads/d97d2668bf6b22409e2f8a0c4b9f3290.pdf"
    output_folder = './images'


if __name__ == '__main__':
    # 调用示例
    # 调用示例

    asyncio.run(test())
