import aiohttp
import asyncio
import hashlib
import os
from loguru import logger

CHUNK_SIZE = 64 * 1024  # 64 KB

async def download_pdf_from_url(url, save_path):
    """
    异步从url下载pdf
    :param url:
    :param save_path:
    :return:
    """
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()

                # 计算PDF文件的哈希值
                file_content = bytearray()
                async for chunk in response.content.iter_any():
                    file_content.extend(chunk)

                if not file_content:
                    logger.error("空文件异常")
                    return

                # 保存文件
                with open(save_path, 'wb') as file:
                    file.write(file_content)

                # 提取哈希值的十六进制表示
                file_hash = hashlib.md5(file_content).hexdigest()
                logger.info(f"下载PDF文件成功，哈希值为: {file_hash}")

                # 构建新的文件名
                new_file_name = file_hash + '.pdf'
                new_file_path = os.path.join(os.path.dirname(save_path), new_file_name)

                # 重命名文件
                os.rename(save_path, new_file_path)
                logger.info(f"重命名文件为: {new_file_path}")

                logger.info("文件下载成功！")
    except aiohttp.ClientError as e:
        logger.error(f"文件下载失败: {e}")


if __name__ == "__main__":
    # 使用示例
    pdf_url = "http://www.biorxiv.org/content/10.1101/2022.07.22.501196v2.full.pdf"
    save_path = "./example.pdf"
    asyncio.run(download_pdf_from_url(pdf_url, save_path))
