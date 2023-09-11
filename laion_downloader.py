from img2dataset import download
import shutil
import os

if __name__ == '__main__':

    url_list = r"F:\Laion\0000.parquet"
    output_dir = r"F:\Laion\0000\output_00000"
    isExists = os.path.exists(output_dir)
    if not isExists:
        os.makedirs(output_dir)

    download(
        processes_count=2,
        thread_count=64,
        url_list=url_list,
        image_size=1024,
        resize_only_if_bigger=False,
        timeout=15,
        encode_quality=100,
        resize_mode="no",
        skip_reencode=True,
        output_folder=output_dir,
        output_format="parquet",
        input_format="parquet",
        url_col="URL",
        caption_col="TEXT",
        disable_all_reencoding=True,
        #enable_wandb=True,
        number_sample_per_shard=10000,
        save_additional_columns=["NSFW", "similarity", "LICENSE"],
        #oom_shard_count=6,
        retries=2
    )