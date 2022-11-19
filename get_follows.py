""" get_follows.py
Parse the following and follower files and save account details to accounts.json
"""
import argparse
import json
import logging
import os
import re
import requests

import twitter_guest_api

ACCOUNT_REGEX = re.compile(r'"accountId" : "(\d+)"')

def get_accounts(filename, output_dict):
    """For every account id found in file, get metadata"""
    with open(filename, "r", encoding="utf-8") as input_file:
        contents = input_file.read()
    with requests.Session() as session:
        api = twitter_guest_api.TwitterGuestAPI(session)
        for found in ACCOUNT_REGEX.finditer(contents):
            account_id = found.group(1)
            if account_id in output_dict:
                logging.info("%s already processed", account_id)
                continue
            logging.info("Getting account %s", account_id)
            output_dict[account_id] = api.get_account(session, account_id)

def main():
    """Main function"""
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", default=".", help="Directory of unzipped Twitter archive")
    parser.add_argument("--following", action="store_true", help="Get accounts you follow")
    parser.add_argument("--follower", action="store_true", help="Get accounts who follow you")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if not args.following and not args.follower:
        logging.error("Please specify --following or --follower")
        return

    account_dictionary_path = os.path.join(args.archive, "accounts.json")

    account_dict = {}
    try:
        with open(account_dictionary_path, 'r', encoding='utf-8') as output_file:
            account_dict = json.load(output_file)
    except FileNotFoundError:
        # Dictionary doesn't exist yet.
        pass

    try:
        if args.following:
            following_path = os.path.join(args.archive, "data", "following.js")
            get_accounts(following_path, account_dict)
        if args.follower:
            follower_path = os.path.join(args.archive, "data", "follower.js")
            get_accounts(follower_path, account_dict)
    except Exception as exc: #pylint: disable=broad-except
        # This is really slow and hammers your internet, so make sure we don't lose work
        # by catching absolutely everything
        logging.error("Exception: %s", exc)
    except KeyboardInterrupt:
        logging.warning("Cancelled by keyboard interrupt")

    # Write finished directionary
    with open(account_dictionary_path, 'w', encoding='utf-8') as output_file:
        json.dump(account_dict, output_file)

if __name__ == "__main__":
    main()
