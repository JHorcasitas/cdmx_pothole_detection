"""Reads all the videos from an AWS bucket prefix, extracts the frames and
saves the results in another prefix. If a video name is `example_name.mp4`, the
extracted frames names would be named example_name_frame_i.jpg where i is the
index of the extracted frame.

The following things should be specified as arguments to the script;
- sampling_rate: The frequency of frame extraction.
- input_prefix: The input prefix where videos are contained. 
- output_prefix: The prefix where frame images are going to be saved. 

The following example will read images from <input_prefix>, it will extract
images every 5 frames, and it will save them to <output_prefix>.

    >> python extract_frames_from_videos.py --bucket=<bucket_name> --input_prefix=<input_prefix> --output_prefix=<output_prefix> --sampling_rate=5

The script is idempotent in the sense that executing it again will not download
and process the videos that were already processed. A video is considered to be
fully processed when all of its frames have been sampled and uploaded in the
target prefix.

# TODO: Specify documentation about the cache.
"""
import os
import argparse
from typing import List

import cv2
import boto3
from loguru import logger


def get_video_keys(s3_client, bucket, prefix) -> List[str]:
    objects = s3_client.list_objects(Bucket=bucket, Prefix=prefix)
    return [content["Key"] for content in objects.get("Contents", [])]


def main(bucket, input_prefix, output_prefix, sampling_rate):
    logger.info(f"Starting script execution")
    logger.info(f"bucket: {bucket}")
    logger.info(f"input_prefix: {input_prefix}")
    logger.info(f"output_prefix: {output_prefix}")
    logger.info(f"sampling_rate: {sampling_rate}")
    s3_client = boto3.client("s3")
    video_keys = get_video_keys(s3_client, bucket, input_prefix)
    for vk in video_keys:
        logger.info(f"Processing {vk}")
        # Download video from S3 to local
        video_name = os.path.basename(vk)
        download_path = f"/tmp/{video_name}"

        if os.path.exists("/tmp/extract_frames_from_videos_cache.txt"):
            with open("/tmp/extract_frames_from_videos_cache.txt", "rt") as f:
                completed_download_paths = f.readlines()
            if download_path in completed_download_paths:
                continue

        s3_client.download_file(bucket, vk, download_path)

        # Open video and start processing
        cap = cv2.VideoCapture(download_path)
        i = 0
        frame_index = 0
        while cap.isOpened():
            # If a frame is successfully grabbed, ret will be True, otherwise
            # it will be False.
            ret, frame = cap.read()
            if not ret:
                break

            if i % sampling_rate == 0:
                frame_name = f"{video_name}_frame_{frame_index}.jpg"
                frame_path = f"/tmp/{frame_name}"
                cv2.imwrite(frame_path, frame)

                # Upload the frame to S3
                upload_key = f"{output_prefix}/{frame_name}"
                s3_client.upload_file(frame_path, bucket, upload_key)
                os.remove(frame_path)  # Remove frame file
                
                frame_index += 1
            i += 1

        cap.release()
        os.remove(download_path)  # Remove temporary video file
        with open("/tmp/extract_frames_from_videos_cache.txt", "a") as f:
            f.write(f"{download_path}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract frames from videos in an S3 bucket")
    parser.add_argument("--bucket", required=True, help="The name of the AWS S3 bucket.")
    parser.add_argument("--input_prefix", required=True, help="The input prefix where videos are contained.")
    parser.add_argument("--output_prefix", required=True, help="The prefix where extracted frame images are going to be saved.")
    parser.add_argument("--sampling_rate", type=int, default=10, help="The frequency of frame extraction. Defaults to 10.")
    args = parser.parse_args()

    main(args.bucket, args.input_prefix, args.output_prefix, args.sampling_rate)
