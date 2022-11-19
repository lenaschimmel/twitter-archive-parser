""" get_likes.py
Parse like.js and retrieve full tweet information, along with any media
Operates in several stages, each of which may be skipped.
Stage 1 - get tweet data
Stage 2 - get images
Stage 3 - get videos
Tweet data will be saved to "likes.json" in the root of the archive.
Media will be saved to a new "other_media" subdirectory of the archive.
This is interruptible - if an exception occurs or the user breaks out with Ctrl-C,
it will save the progress so far and reload next time.
"""
import argparse
import json
import logging
import os
import re
import requests

import twitter_guest_api

LIKE_REGEX = re.compile(r'"tweetId" : "(\d+)"')

def download_file(output_directory, url):
    """Download file to output directory. Skip if it already exists and is the correct size"""
    file_name = url.rpartition('/')[2]
    file_name = file_name.split("?")[0]
    output_path = os.path.join(output_directory, file_name)

    try:
        # Check if we have a local file and how big it is.
        size = os.path.getsize(output_path)
        # Check file size on server
        response = requests.head(url)
        if response.status_code == 200:
            if int(response.headers['content-length']) == size:
                logging.info("%s already exists", output_path)
                return
            logging.info("%s is incomplete, retrying download", output_path)
    except OSError:
        pass

    with open(output_path, "wb") as output_file:
        response = requests.get(url)
        if response.status_code == 200:
            image_data = response.content
            output_file.write(image_data)
            logging.info("Wrote %s", output_path)
        else:
            logging.error("Error getting %s: %s (%i)", url, response.reason, response.status_code)

def get_liked_tweets(contents, output_dict):
    """For every media link found in contents, download the file"""
    with requests.Session() as session:
        download = twitter_guest_api.TwitterGuestAPI(session)
        for found in LIKE_REGEX.finditer(contents):
            tweet_id = found.group(1)
            if tweet_id in output_dict:
                logging.info("%s already processed", tweet_id)
                continue
            logging.info("Getting tweet %s", tweet_id)
            output_dict[tweet_id] = download.get_tweet(session, tweet_id)

def download_images(media_path, like_dict):
    """Download every image in the liked tweets"""
    for tweet in like_dict.values():
        if not tweet:
            # Deleted tweets will be empty
            continue
        try:
            media = tweet["entities"]["media"]
            for entry in media:
                media_url = entry["media_url_https"]
                download_file(media_path, media_url)
        except KeyError:
            # No media in this tweet
            pass

def download_videos(media_path, like_dict):
    """Download every video in the liked tweets"""
    for tweet in like_dict.values():
        if not tweet:
            # Deleted tweets will be empty
            continue
        try:
            media = tweet["extended_entities"]["media"]
        except KeyError:
            # No media in this tweet
            continue
        for entry in media:
            if "video_info" in entry:
                video_info = entry["video_info"]
                best_variant = {}
                for variant in video_info["variants"]:
                    if not "bitrate" in variant:
                        continue
                    variant_bitrate = variant["bitrate"]
                    if not best_variant or best_variant["bitrate"] < variant_bitrate:
                        best_variant = variant
                if best_variant:
                    download_file(media_path, best_variant["url"])
                else:
                    logging.error("Failed to find best variant")

def main():
    """Main function"""
    parser = argparse.ArgumentParser()
    parser.add_argument('archive', help='Directory of unzipped Twitter archive')
    parser.add_argument('--skip_metadata', action="store_true", help="Don't get tweet metadata")
    parser.add_argument('--skip_images', action="store_true", help="Don't download image files")
    parser.add_argument('--skip_videos', action="store_true", help="Don't download video files")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    like_dictionary_path = os.path.join(args.archive, "likes.json")
    like_js_path = os.path.join(args.archive, "data", "like.js")
    media_path = os.path.join(args.archive, "other_media")
    os.makedirs(media_path, exist_ok=True)

    like_dict = {}
    try:
        with open(like_dictionary_path, 'r', encoding='utf-8') as output_file:
            like_dict = json.load(output_file)
    except FileNotFoundError:
        # Dictionary doesn't exist yet.
        pass

    with open(like_js_path, "r", encoding="utf-8") as input_file:
        contents = input_file.read()

    if not args.skip_metadata:
        cancelled = False
        try:
            get_liked_tweets(contents, like_dict)
        except Exception as exc: #pylint: disable=broad-except
            # This is really slow and hammers your internet, so make sure we don't lose work
            # by catching absolutely everything
            logging.error("Exception: %s", exc)
        except KeyboardInterrupt:
            logging.warning("Cancelled by keyboard interrupt")
            cancelled = True

        # Write finished directionary
        with open(like_dictionary_path, 'w', encoding='utf-8') as output_file:
            json.dump(like_dict, output_file)

        if cancelled:
            return

    if not args.skip_images:
        download_images(media_path, like_dict)

    if not args.skip_videos:
        download_videos(media_path, like_dict)

if __name__ == "__main__":
    main()
