import asyncio
import os
from pathlib import Path
from typing import List

import fitz  # pip install PyMuPDF
from dotenv import load_dotenv
from loguru import logger


# 使用fitz 库直接提取pdf的图像
async def Extract_Images_From_PDF(file_name: str,
                                  output_file: str,
                                  image_size: int = 30*1024,
                                  min_width: int = 128,
                                  min_height: int = 128) -> List[str]:
    """

    :param file_name:
    :param output_file:
    :param min_width:
    :param min_height:
    :return:
    """
    # 打开pdf，打印PDF的相关信息
    pdf_hash = Path(file_name).stem
    doc = fitz.open(file_name)
    # 图片计数
    imgcount = 0
    lenXREF = doc.xref_length()  # 获取pdf文件对象总数

    # 打印PDF的信息
    logger.info(f"文件名:{file_name}, 页数: {len(doc)}, 对象: {lenXREF - 1}")

    pic_path = Path(output_file) / Path(file_name).stem
    if not pic_path.exists():
        pic_path.mkdir(parents=True)
    # os.system(f"rm \'{pic_path}/*\' ")

    # 遍历doc，获取每一页
    image_file_list = []
    for page in doc:
        try:
            imgcount += 1
            tupleImage = page.get_images()
            lstImage = list(tupleImage)
            for xref in list(tupleImage):
                xref = list(xref)[0]
                # print("imgID:    %s" % imgcount)
                # print("xref:  %s" % xref)
                img = doc.extract_image(xref)  # 获取文件扩展名，图片内容 等信息
                if len(img['image']) > image_size and img['width'] > min_width and img['height'] > min_height:  # 只有大于30kb的才保存
                    image_filename = f"{imgcount}_{xref}.png"
                    logger.info(f"save image:{image_filename}")
                    image_file_list.append(image_filename)
                    image_file_path = os.path.join(pic_path, image_filename)  # 合成最终图像完整路径名
                    with open(image_file_path, 'wb') as imgout:
                        imgout.write(img["image"])
        except Exception as e:
            logger.error(f"Extract_Images_From_PDF,pdf_hash:{pdf_hash}, page:{page},error:{repr(e)}")
            continue
    return image_file_list


if __name__ == '__main__':
    # 调用示例
    # 调用示例
    pdf_file = "/home/rongkang/WorkSpace/WebSite/ChatPaperDevelop/uploads/d97d2668bf6b22409e2f8a0c4b9f3290.pdf"
    output_folder = './images'
    asyncio.run(Extract_Images_From_PDF(pdf_file, output_folder,
                                        min_width=128,
                                        min_height=128))
