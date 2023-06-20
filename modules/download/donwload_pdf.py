from pathlib import Path
import aiofiles
import aiohttp
import asyncio
import hashlib
import os
from loguru import logger
import fitz

CHUNK_SIZE = 64 * 1024  # 64 KB

async def save_pdf(file_content, file_hash, save_path):
    """
    保存pdf的内容
    :param file_content:
    :param file_hash:
    :param save_path:
    :return:
    """
    save_path = os.path.join(save_path, f"{file_hash}.pdf")
    if not Path(save_path).is_file():
        async with aiofiles.open(save_path, "wb") as f:
            await f.write(file_content)
            logger.info(f"save pdf: {save_path}")

async def calculate_pdf_hash(pdf_bytes):
    """
    读取pdf 为bytes 然后用fitz只读取内容
    :param pdf_bytes:
    :param pages: PDF 的页数
    :return:
    """
    logger.info("begin cal pdf hash")
    file_hash = hashlib.md5()

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages = len(doc)
    for page_num in range(pages):
        page = doc[page_num]
        text = page.get_text()
        file_hash.update(text.encode())

    # 提取哈希值的十六进制表示
    file_hash_hex = file_hash.hexdigest()
    logger.info(f"get pdf hash: {file_hash_hex}")
    return file_hash_hex, pages

async def download_pdf_from_url(url, save_path):
    """
    异步从url下载pdf
    :param url:
    :param save_path:
    :return:
    """
    logger.info(f"begin download pdf:{url}")
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                response.raise_for_status()

                # 读取PDF文件的二进制内容
                pdf_bytes = await response.content.read()

                # 计算PDF文件的哈希值
                file_hash_hex, pages = await calculate_pdf_hash(pdf_bytes)
                logger.info(f"下载PDF文件成功，哈希值为: {file_hash_hex}")

                # 保存文件
                try:
                    await save_pdf(pdf_bytes, file_hash_hex, save_path)
                    logger.info(f"end save pdf url :{url}, filename:{file_hash_hex}.pdf 文件下载成功！")
                    return file_hash_hex, pages
                except Exception as e:
                    logger.error(f"file save 失败: {e}")
                    return False
    except aiohttp.ClientError as e:
        logger.error(f"文件下载失败: {e}")
        return False

def compare_pdf_files(file1_path, file2_path):
    """
    比较两个PDF文件内容是否不同
    :param file1_path:
    :param file2_path:
    :return:
    """
    pdf1 = fitz.open(file1_path)
    pdf2 = fitz.open(file2_path)

    if len(pdf1) != len(pdf2):
        return False

    for page_num in range(len(pdf1)):
        page1 = pdf1[page_num]
        page2 = pdf2[page_num]

        if page1.get_text() != page2.get_text():
            return False

    return True
if __name__ == "__main__":
    # 使用示例
    pdf_url = "http://www.biorxiv.org/content/10.1101/2022.07.22.501196v2.full.pdf"
    save_path = "../download"
    asyncio.run(download_pdf_from_url(pdf_url, save_path))

