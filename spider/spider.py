# -*- coding:utf-8 -*-
import os
from PIL import Image
import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from Myrequest import request
from my_logger import Loggings
from concurrent.futures import ThreadPoolExecutor

logger = Loggings()


def download_image(item):
    info = {}
    url = item['URL']
    try:
        print(f">>>下载：{url}")
        headers = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36"
        }
        response = request("get", url, headers=headers, verify=False)
        if response.status_code == 200:
            info['img_data'] = response.content
            if item.get("WIDTH"):
                info['original width'] = item.get("WIDTH")
                info['original height'] = item.get("HEIGHT")
            else:
                image = Image.open(io.BytesIO(response.content))
                # 获取图片的宽度和高度
                width, height = image.size
                info['original width'] = width
                info['original height'] = height
            info['url'] = url
            info['caption'] = item.get("TEXT")
            info['status'] = "1"
            return info
    except Exception as e:
        logger.error(f"{url}, 失败原因：{e}")
        faild_info = {}
        faild_info['url'] = url
        faild_info['caption'] = item.get("TEXT")
        faild_info['status'] = "0"
        return faild_info


# 定义一个函数来将图片数据追加到Parquet文件中
def append_images_to_parquet(image_urls, batch_size=1000, num_threads=10):
    # 分批处理图片
    num_batches = len(image_urls) // batch_size + 1
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        for i in range(num_batches):
            start_idx = i * batch_size
            end_idx = (i + 1) * batch_size
            batch_urls = image_urls[start_idx:end_idx]

            # 使用多线程并行下载图片
            image_data_list = list(executor.map(download_image, batch_urls))

            dfs = []
            faild_dfs = []
            # 追加每个图片的数据到数据框
            for image_data in image_data_list:

                if image_data.get("status") in "0":
                    df = pd.DataFrame([image_data])
                    faild_dfs.append(df)
                else:
                    df = pd.DataFrame([image_data])
                    dfs.append(df)
            if dfs:
                logger.debug(f"当前下载{batch_size}/成功条数：{len(dfs)}")
                merged_df = pd.concat(dfs, ignore_index=True)
                table = pa.Table.from_pandas(merged_df)
                pq.write_table(table, f'{outpath}000{i}.parquet')
            if faild_dfs:
                merged_df = pd.concat(faild_dfs, ignore_index=True)
                table = pa.Table.from_pandas(merged_df)
                pq.write_table(table, f'{outpath}000{i}_faild.parquet')


def split_parquet_to_feather(feather_outpath, parquet_file, chunk_size):
    table = pq.read_table(parquet_file)

    # 获取数据总行数
    total_rows = table.num_rows

    # 计算需要拆分成多少个Feather文件
    num_files = (total_rows // chunk_size) + 1

    # 拆分并保存为多个Feather文件
    for i in range(num_files):
        print(f"开始分割-----{i}")
        start = i * chunk_size
        # end = min((i + 1) * chunk_size, total_rows)
        chunk_table = table.slice(start, chunk_size)
        chunk_table.to_pandas().to_feather(f'{feather_outpath}output_{i}.feather')


def get_file_paths(directory):
    file_paths = []
    for root, directories, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            file_paths.append(file_path)
    return file_paths


def main(feather_outpath, parquet_path, chunk_size=10000, batch_size=10000, is_split=True):
    """
        @param feather_outpath: 优先级 越小越优先 默认300
        @param parquet_path: 回调函数所在的类名 默认为当前类
        @param chunk_size: 分割条数，默认一万
        @param batch_size: 生成单个parquet文件里面数据量，默认一万
    """
    # 进行分割parquet
    if not os.path.exists(feather_outpath):
        os.makedirs(feather_outpath)

    # chunk_size 分割条数
    if is_split:
        split_parquet_to_feather(feather_outpath, parquet_path, chunk_size=chunk_size)
    # 获取分割后feather文件路径
    file_paths = get_file_paths(feather_outpath)
    for fp in file_paths:
        items = pd.read_feather(fp).to_dict(orient='records')
        # 调用append_images_to_parquet函数将图片数据追加到Parquet文件中
        append_images_to_parquet(items, batch_size=batch_size, num_threads=100)
        break


if __name__ == '__main__':
    # 采集结果文件路径
    outpath = r"C:/Users/chenw/Desktop/pic/data/"
    if not os.path.exists(outpath):
        os.makedirs(outpath)
    main(feather_outpath=r"C:/Users/chenw/Desktop/pic/feather/",
         parquet_path=r"C:\Users\chenw\Desktop\pic\url\part-00000-5114fd87-297e-42b0-9d11-50f1df323dfa-c000.snappy.parquet", is_split=False)
