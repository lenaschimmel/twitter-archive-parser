#!/usr/bin/env python3
"""
    twitter-archive-parser - Python code to parse a Twitter archive and output in various ways
    Copyright (C) 2022 Tim Hutton - https://github.com/timhutton/twitter-archive-parser

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

from argparse import ArgumentParser
from collections import defaultdict
import math
from typing import Optional, Tuple, Union
from urllib.parse import urlparse
import datetime
import glob
import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import traceback
from typing import List
from zoneinfo import ZoneInfo
# hot-loaded if needed, see import_module():
#  imagesize
#  requests
#  tzlocal


# Print a compile-time error in Python < 3.6. This line does nothing in Python 3.6+ but is reported to the user
# as an error (because it is the first line that fails to compile) in older versions.
f' Error: This script requires Python 3.6 or later. Use `python --version` to check your version.'


class UserData:
    def __init__(self, user_id: str, handle: str):
        if user_id is None:
            raise ValueError('ID "None" is not allowed in UserData.')
        elif type(user_id) is not str:
            self.user_id = str(user_id)
        else:
            self.user_id = user_id
        if handle is None:
            raise ValueError('handle "None" is not allowed in UserData.')
        self.handle = handle

    def to_dict(self) -> dict:
        return {
            'user_id': self.user_id,
            'handle': self.handle,
        }


class PathConfig:
    """
    Helper class containing constants for various directories and files.
    
    The script will only add / change / delete content in its own directories, which start with `parser-`.
    Files within `parser-output` are the end result that the user is probably interested in.
    Files within `parser-cache` are temporary working files, which improve the efficiency if you run
    this script multiple times. They can safely be removed without harming the consistency of  the
    files within `parser-output`.
    """
    def __init__(self, dir_archive):
        self.dir_archive                    = dir_archive
        self.dir_input_data                 = os.path.join(dir_archive,             'data')
        self.file_account_js                = os.path.join(self.dir_input_data,     'account.js')

        # check if user is in correct folder
        if not os.path.isfile(self.file_account_js):
            print(f'Error: Failed to load {self.file_account_js}. ')
            exit()

        self.dir_input_media                = find_dir_input_media(self.dir_input_data)
        self.dir_output                     = os.path.join(self.dir_archive,        'parser-output')
        self.dir_output_media               = os.path.join(self.dir_output,         'media')
        self.dir_output_cache               = os.path.join(self.dir_archive,        'parser-cache')
        self.file_output_following          = os.path.join(self.dir_output,         'following.txt')
        self.file_output_followers          = os.path.join(self.dir_output,         'followers.txt')
        self.file_download_log              = os.path.join(self.dir_output_cache,   'download_log.txt')
        self.file_tweet_icon                = os.path.join(self.dir_output_media,   'tweet.ico')
        self.file_media_download_state      = os.path.join(self.dir_output_cache,   'media_download_state.json')
        self.files_input_tweets             = find_files_input_tweets(self.dir_input_data)

        # structured like an actual tweet output file, can be used to compute relative urls to a media file
        self.example_file_output_tweets = self.create_path_for_file_output_tweets(year=2020, month=12)

    def create_path_for_file_output_tweets(self, year, month, output_format: str = "html", kind: str = "tweets") -> str:
        """Builds the path for a tweet-archive file based on some properties."""
        # Previously the filename was f'{dt.year}-{dt.month:02}-01-Tweet-Archive-{dt.year}-{dt.month:02}'
        return os.path.join(
            self.dir_output, f"{kind}-{output_format}", f"{year:04}", f"{year:04}-{month:02}-01-{kind}.{output_format}"
        )

    def create_path_for_file_output_dms(
            self, name: str, index: Optional[int] = None, output_format: str = "html", kind: str = "DMs"
    ) -> str:
        """Builds the path for a dm-archive file based on some properties."""
        index_suffix = ""
        if index:
            index_suffix = f"-part{index:03}"
        return os.path.join(self.dir_output, kind, f"{kind}-{name}{index_suffix}.{output_format}")

    def create_path_for_file_output_single(self, output_format: str, kind: str) -> str:
        """Builds the path for a single output file which, i.e. one that is not part of a larger group or sequence."""
        return os.path.join(self.dir_output, f"{kind}.{output_format}")


def format_duration(seconds: float) -> str:
    duration_datetime: datetime.datetime = \
        datetime.datetime.fromtimestamp(
            seconds,
            tz=datetime.timezone.utc
        )
    if duration_datetime.hour >= 1:
        return f"{duration_datetime.hour  } hour{  '' if duration_datetime.hour   == 1 else 's'} " \
               f"{duration_datetime.minute} minute{'' if duration_datetime.minute == 1 else 's'}"
    elif duration_datetime.minute >= 1:
        return f"{duration_datetime.minute} minute{'' if duration_datetime.minute == 1 else 's'} " \
               f"{duration_datetime.second} second{'' if duration_datetime.second == 1 else 's'}"
    else:
        return f"{duration_datetime.second} second{'' if duration_datetime.second == 1 else 's'}"


def get_config(key: str) -> Optional[str]:
    """
    Here you can configure values to skip get_consent prompts. Return 'None' to ask the user,
    '' to choose accepts the prompt's default, 'y' to accept or 'n' to decline. Future
    versions of this script may use a proper config file and/or command line args instead of
    there hardcoded values.
    """
    if key == "download_tweets":
        return "y"
    if key == "download_users":
        return "y"
    if key == "download_media":
        return "y"
    if key == "delete_old_files":
        return "y"
    if key == "install_via_pip":
        return "y"
    if key == "lookup_followers":
        return "y"
    if key == "lookup_tweet_users":
        return "y"
    if key == "download_profile_images":
        return "y"

    print(f"Warning: config for key '{key}' not present, asking user instead.")
    # Hint: if you want the config to be "ask the user" without this warning,
    # catch the key above and return None before this warning is printed.

    return None


def get_consent(prompt: str, key: str, default_to_yes: bool = False):
    """Asks the user for consent, using the given prompt. Accepts various versions of yes/no, or 
    an empty answer to accept the default. The default is 'no' unless default_to_yes is passed as 
    True. The default will be indicated automatically. For unacceptable answers, the user will 
    be asked again."""
    if default_to_yes:
        suffix = " [Y/n]"
        default_answer = "yes"
    else:
        suffix = " [y/N]"
        default_answer = "no"
    while True:
        config_val = get_config(key)
        if config_val is None:
            user_input = input(prompt + suffix)
        else:
            user_input = config_val
            print(f"Skipped question: '{prompt}', used config '{config_val}'.")
        if user_input == "":
            print(f"Your empty response was assumed to mean '{default_answer}' (the default for this question).")
            return default_to_yes
        if user_input.lower() in ('y', 'yes'):
            return True
        if user_input.lower() in ('n', 'no'):
            return False
        print(f"Sorry, did not understand. Please answer with y, n, yes, no, or press enter to accept "
              f"the default (which is '{default_answer}' in this case, as indicated by the uppercase "
              f"'{default_answer.upper()[0]}'.)")


def import_module(module):
    """Imports a module specified by a string. Example: requests = import_module('requests')"""
    try:
        return importlib.import_module(module)
    except ImportError:
        print(f'\nError: This script uses the "{module}" module which is not installed.\n')
        if not get_consent('OK to install using pip?', key='install_via_pip'):
            exit()
        subprocess.run([sys.executable, '-m', 'pip', 'install', module], check=True)
        return importlib.import_module(module)


def open_and_mkdirs(path_file):
    """Opens a file for writing. If the parent directory does not exist yet, it is created first."""
    mkdirs_for_file(path_file)
    return open(path_file, 'w', encoding='utf-8')


def mkdirs_for_file(path_file):
    """Creates the parent directory of the given file, if it does not exist yet."""
    path_dir = os.path.split(path_file)[0]
    os.makedirs(path_dir, exist_ok=True)


def rel_url(media_path, document_path):
    """Computes the relative URL needed to link from `document_path` to `media_path`.
       Assumes that `document_path` points to a file (e.g. `.md` or `.html`), not a directory."""
    return os.path.relpath(media_path, os.path.split(document_path)[0]).replace("\\", "/")


def get_twitter_api_guest_token(session, bearer_token):
    """Returns a Twitter API guest token for the current session."""
    guest_token_response = session.post("https://api.twitter.com/1.1/guest/activate.json",
                                        headers={'authorization': f'Bearer {bearer_token}'},
                                        timeout=2,
                                        )
    guest_token = json.loads(guest_token_response.content)['guest_token']
    if not guest_token:
        raise Exception(f"Failed to retrieve guest token")
    return guest_token


# TODO if downloading fails within the for loop, we should be able to return the already 
#  fetched users, but also make it clear that it is incomplete. Maybe do it like in get_tweets.
def get_twitter_users(session, bearer_token, guest_token, user_ids):
    """Asks Twitter for all metadata associated with user_ids."""
    users = {}
    failed_count = 0
    while user_ids:
        max_batch = 100
        user_id_batch = user_ids[:max_batch]
        user_ids = user_ids[max_batch:]
        user_id_list = ",".join(user_id_batch)
        query_url = f"https://api.twitter.com/1.1/users/lookup.json?user_id={user_id_list}"
        response = session.get(query_url,
                               headers={'authorization': f'Bearer {bearer_token}', 'x-guest-token': guest_token},
                               timeout=2,
                               )
        if response.status_code == 404:
            failed_count += len(user_id_batch)
            print(f'requested download of {len(user_id_batch)} users returned with status "404 - Not found."')
            continue
        elif not response.status_code == 200:
            raise Exception(f'Failed to get user handle: {response}')
        response_json = json.loads(response.content)
        for user in response_json:
            users[user["id_str"]] = user
        print(f'fetched {len(response_json)} users, {len(user_ids)} remaining...')
        failed_count += (len(user_id_batch) - len(response_json))
    print(f'{failed_count} users could not be fetched.')
    return users


def get_tweets(
        session,
        bearer_token,
        guest_token,
        tweet_ids: list[str],
        include_user=True,
        include_alt_text=True
) -> Tuple[dict[str, Optional[dict]], list[str]]:
    """Get the json metadata for multiple tweets.
    If include_user is False, you will only get a numerical id for the user.
    Requested tweets may be unavailable for two reasons:
     - the API request fails, e.g. due to network errors or rate limiting (probably temporary problem)
     - the API explicitly returns `null` for the tweet (probably permanent problem, e.g. tweet was deleted)
    This function will include null values in the first part of the returned tuple. Only tweets which are
    absent due to (probably) temporary problems are listed with their id in the second part of the return value.
    """
    tweets = {}
    remaining_tweet_ids = tweet_ids.copy()
    try:
        while remaining_tweet_ids:
            max_batch = 100
            tweet_id_batch = remaining_tweet_ids[:max_batch]
            tweet_id_list = ",".join(map(str,tweet_id_batch))
            print(f"Download {len(tweet_id_batch)} tweets of {len(remaining_tweet_ids)} remaining...")
            query_url = f"https://api.twitter.com/1.1/statuses/lookup.json?id={tweet_id_list}&" \
                        f"tweet_mode=extended&map=true"
            if not include_user:
                query_url += "&trim_user=1"
            if include_alt_text:
                query_url += "&include_ext_alt_text=1"
            response = session.get(
                query_url,
                headers={
                    'authorization': f'Bearer {bearer_token}',
                    'x-guest-token': guest_token
                },
                timeout=5
            )
            if response.status_code == 429:
                # Rate limit exceeded - get a new token
                guest_token = get_twitter_api_guest_token(session, bearer_token)
                continue
            if not response.status_code == 200:
                raise Exception(f'Failed to get tweets: {response}')
            response_json = json.loads(response.content)['id'] # when map=true, everything is under a key 'id'
            for tweet_id in response_json:
                tweet = response_json[tweet_id]
                tweets[tweet_id] = tweet
            remaining_tweet_ids = remaining_tweet_ids[max_batch:]
    except Exception as err:
        traceback.print_exc()
        print(f"Exception during batch download of tweets: {err}")
        print(f"Try to work with the tweets we got so far.")
    return tweets, remaining_tweet_ids


def lookup_users(user_ids, users, extended_user_data) -> dict:
    """Fill the users and extended_user_data dictionaries with data from Twitter"""
    if not user_ids:
        # Don't bother opening a session if there's nothing to get
        return {}
    unknown_user_ids = []
    for user_id in user_ids:
        if user_id not in users.keys() or user_id not in extended_user_data.keys():
            unknown_user_ids.append(user_id)
    # Account metadata observed at ~2.1KB on average.
    estimated_size = int(2.1 * len(unknown_user_ids))
    estimated_download_time_seconds = math.ceil(len(unknown_user_ids) / 100) * 2
    estimated_download_time_str = format_duration(estimated_download_time_seconds)
    print(f'{len(unknown_user_ids)} users to look up, this will take up to {estimated_download_time_str} ...')
    if not get_consent(f'Download user data from Twitter (approx {estimated_size:,} KB)?', key='download_users'):
        return {}

    requests = import_module('requests')
    try:
        with requests.Session() as session:
            bearer_token = 'AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xn' \
                           'Zz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
            guest_token = get_twitter_api_guest_token(session, bearer_token)
            retrieved_users = get_twitter_users(session, bearer_token, guest_token, unknown_user_ids)
            for user_id, user_info in retrieved_users.items():
                if user_id not in extended_user_data.keys():
                    extended_user_data[user_id] = user_info
                elif extended_user_data[user_id] == user_info:
                    pass
                else:
                    old_user_info = extended_user_data[user_id]
                    try:
                        extended_user_data[user_id] = merge_dicts(old_user_info, user_info)
                    except Exception as err:
                        print(f'Could not update extended user data for user id {user_id}: {err}')
                        print('Keeping previously stored data for this user.')
            for user_id, user in retrieved_users.items():
                if user["screen_name"] is not None:
                    users[str(user_id)] = UserData(user_id=user_id, handle=user["screen_name"])
        print()  # empty line for better readability of output
    except Exception as err:
        print(f'Failed to download user data: {err}')


def read_json_from_js_file(filename):
    """Reads the contents of a Twitter-produced .js file into a dictionary."""
    print(f'Parsing {filename}...')
    with open(filename, 'r', encoding='utf8') as f:
        data = f.readlines()
        # if the JSON has no real content, it can happen that the file is only one line long.
        # in this case, return an empty dict to avoid errors while trying to read non-existing lines.
        if len(data) <= 1:
            return {}
        # convert js file to JSON: replace first line with just '[', squash lines into a single string
        prefix = '['
        if '{' in data[0]:
            prefix += ' {'
        data = prefix + ''.join(data[1:])
        # parse the resulting JSON and return as a dict
        return json.loads(data)


def extract_user_data(paths: PathConfig) -> UserData:
    """Returns the user's Twitter username from account.js."""
    account = read_json_from_js_file(paths.file_account_js)[0]['account']
    return UserData(account['accountId'], account['username'])


def escape_markdown(input_text: str) -> str:
    """
    Escape markdown control characters from input text so that the text will not break in rendered markdown.
    (Only use on unformatted text parts that do not yet have any markdown control characters added on purpose!)
    """
    characters_to_escape: str = r"\_*[]()~`>#+-=|{}.!"
    output_text: str = ''
    for char in input_text:
        if char in characters_to_escape:
            # add backslash before control char
            output_text = output_text + "\\" + char
        elif char == '\n':
            # add double space before line break
            output_text = output_text + "  " + char
        else:
            output_text = output_text + char
    return output_text


def parse_as_number(str_or_number):
    """Returns an int if you give it either an int or a str that can be parsed as an int. Otherwise, returns None."""
    if isinstance(str_or_number, str):
        if str_or_number.isnumeric():
            return int(str_or_number)
        else:
            return None
    elif isinstance(str_or_number, int):
        return str_or_number
    else:
        return None


def equal_ignore_types(a, b):
    """
    Recognizes two things as equal even if one is a str and the other is a number (but with identical content),
    or if both are lists or both are dicts, and all of their nested values are equal_ignore_types
    """
    if a == b:
        return True
    if parse_as_number(a) is not None and parse_as_number(b) is not None: 
        return parse_as_number(a) == parse_as_number(b)
    if isinstance(a, dict) and isinstance (b, dict):
        if len(a) != len(b):
            return False
        for key in a.keys():
            if not equal_ignore_types(a[key], b[key]):
                return False
        return True
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        for i in range(len(a)):
            if not equal_ignore_types(a[i], b[i]):
                return False
        return True
    return False


def merge_lists(a: list, b: list, ignore_types: bool = False):
    """
    Adds all items from b to a which are not already in a.
    If you pass ignore_types=True, it uses equal_ignore_types internally,
    and also recognizes two list items as equal if they both are dicts with equal id_str values in it,
    which results in merging the dicts instead of adding both separately to the result.
    Modifies a and returns a.
    """
    for item_b in b:
        found_in_a = False
        if ignore_types:
            for item_a in a:
                if equal_ignore_types(item_a, item_b):
                    found_in_a = True
                    break
                if isinstance(item_a, dict) and isinstance(item_b, dict) and has_path(item_a, ['id_str']) \
                        and has_path(item_b, ['id_str']) and item_a['id_str'] == item_b['id_str']:
                    merge_dicts(item_a, item_b)
                    found_in_a = True
                    # TODO add code that merges items with same id_str in old 
                    #  lists, which were written to the cache before this was fixed
        else:
            found_in_a = item_b in a

        if not found_in_a:
            a.append(item_b)
    return a


# Taken from https://stackoverflow.com/a/7205107/39946, then adapted to
# some commonly observed twitter specifics.
def merge_dicts(a, b, path=None):
    """merges b into a"""
    if path is None:
        path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif isinstance(a[key], list) and isinstance(b[key], list):
                merge_lists(a[key], b[key], ignore_types=True)
            elif a[key] == b[key]:
                pass  # same leaf value
            elif key.endswith('_count'):
                a[key] = max(parse_as_number(a[key]), parse_as_number(b[key]))
            elif key in ['possibly_sensitive', 'protected', 'monetizable']:
                # ignore conflicts in unimportant fields that tend to differ
                pass
            elif parse_as_number(a[key]) == parse_as_number(b[key]):
                # Twitter sometimes puts numbers into strings, so that the same number might be 3 or '3'
                a[key] = parse_as_number(a[key])
            elif a[key] is None and b[key] is not None:
                # just as if `not key in a`
                a[key] = b[key]
            elif a[key] is not None and b[key] is None:
                # Nothing to update
                pass
            else:
                raise Exception(f"Conflict at {'.'.join(path + [str(key)])}, value '{a[key]}' vs. '{b[key]}'")
        else:
            a[key] = b[key]
    return a


def unwrap_tweet(tweet):
    if tweet is None:
        return None
    if 'tweet' in tweet.keys():
        return tweet['tweet']
    else:
        return tweet


def add_known_tweet(known_tweets, tweet_id, new_tweet):
    """Adds the new_tweet to known_tweets, possibly merging old and new tweets. If new_tweet is 
    None, it will be marked with 'api_returned_null' in known_tweets, no matter if it was contained 
    there before or not."""
    if new_tweet is not None:
        if tweet_id in known_tweets:
            if known_tweets[tweet_id] == new_tweet:
                pass
            else:
                try:
                    merge_dicts(known_tweets[tweet_id], new_tweet)
                except Exception as err:
                    print(traceback.format_exc())
                    print(f"Tweet {tweet_id} could not be merged: {err}")
        else:
            known_tweets[tweet_id] = new_tweet
    else:  # new_tweet is None
        # Mark tweet as unavailable via the API
        if tweet_id in known_tweets:
            known_tweets[tweet_id]['api_returned_null'] = True
        else:
            known_tweets[tweet_id] = { 'id': tweet_id, 'id_str': tweet_id, 'api_returned_null': True }


def collect_tweet_references(tweet, known_tweets):
    tweet = unwrap_tweet(tweet)
    tweet_ids = set()

    # Don't search for tweet references if this tweet was not part of the original archive
    if 'from_archive' not in tweet:
        return tweet_ids

    # Collect quoted tweets
    if has_path(tweet, ['entities', 'urls']):
        for url in tweet['entities']['urls']:
            if 'url' in url and 'expanded_url' in url:
                expanded_url = url['expanded_url']
                matches = re.match(r'^https://twitter.com/([0-9A-Za-z_]*)/status/(\d+)$', expanded_url)
                if matches is not None:
                    # user_handle = matches[1]
                    quoted_id = matches[2]
                    if quoted_id is not None and quoted_id not in known_tweets:
                        tweet_ids.add(quoted_id)

    # Collect previous tweet in conversation
    if has_path(tweet, ['in_reply_to_status_id_str']):
        prev_tweet_id = tweet['in_reply_to_status_id_str']
        if prev_tweet_id not in known_tweets:
            tweet_ids.add(prev_tweet_id)

    # Collect retweets (adds the tweet itself)
    # Don't do this if we already re-downloaded this tweet
    if 'from_api' not in tweet and 'full_text' in tweet and tweet['full_text'].startswith('RT @'):
        if tweet['id_str'] is None:
            print('Tweet has no id_str, this should not happen')
            if 'id' in tweet and tweet['id'] is not None:
                tweet_ids.add(str(tweet['id']))
            else:
                print('Tweet also has no id, this should really not happen')
        else:
            tweet_ids.add(tweet['id_str'])

    # Collect tweets with media, which might lack alt text
    # TODO we might filter for media which has "type" : "photo" because there is no alt text for videos
    # Don't do this if we already re-downloaded this tweet with alt texts enabled
    if 'download_with_alt_text' not in tweet and has_path(tweet, ['entities', 'media']):
        if tweet['id_str'] is None:
            print('Tweet has no id_str, this should not happen')
            if 'id' in tweet and tweet['id'] is not None:
                tweet_ids.add(str(tweet['id']))
            else:
                print('Tweet also has no id, this should really not happen')
        else:
            tweet_ids.add(tweet['id_str'])

    if None in tweet_ids:
        raise Exception(f"Tweet has reference to other tweet with id None: {tweet}")

    return tweet_ids


def has_path(root_dict: dict, index_path: List[str]):
    """Walks a path through nested dicts or lists,
    returns True if all the keys are present, and all of the values are not None."""
    if not isinstance(index_path, List):
        raise Exception("Path must be provided as list of strings.")

    for index in index_path:
        if index not in root_dict:
            return False
        root_dict = root_dict[index]
        if root_dict is None:
            return False
    return True


class EmptyTweetFullTextError(ValueError):
    """
    custom error class for tweets with empty full_text
    """
    pass


class QuoteRecursionDepthError(ValueError):
    """
    custom error class for nested quote-tweets that are above the recursion limit
    """
    pass


def convert_tweet(
        tweet: dict,
        known_tweets: Optional[dict],
        own_user_data: UserData,
        media_sources: Optional[dict],
        users: dict[str, UserData],
        extended_user_data: dict,
        local_timezone: ZoneInfo,
        paths: PathConfig,
        depth: int,
) -> Tuple[int, str, str]:
    """Converts a JSON-format tweet. Returns tuple of timestamp, markdown and HTML."""
    tweet = unwrap_tweet(tweet)

    if depth >= 5:
        raise QuoteRecursionDepthError('reached depth limit for nested quotes.')

    if 'full_text' not in tweet or tweet['full_text'] is None:
        raise EmptyTweetFullTextError(
            'empty full_text - tweet or user has probably been withheld, or protected their account.'
        )

    # Unwrap retweets
    if has_path(tweet, ['retweeted_status']):
        # TODO retweets are not unpacked as separate tweets in known_tweets, so this will usually not work:
        # tweet = known_tweets[tweet['retweeted_status']['id_str']]
        outer_tweet = tweet
        tweet = tweet['retweeted_status']
    else:
        outer_tweet = None

    original_date_format = '%a %b %d %X %z %Y'  # Example: Tue Mar 19 14:05:17 +0000 2019
    nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 2019, 14:05

    tweet_datetime = datetime.datetime.strptime(tweet['created_at'], original_date_format)
    tweet_timestamp = int(round(tweet_datetime.timestamp()))

    tweet_datetime_local = datetime.datetime.fromtimestamp(tweet_datetime.timestamp(), tz=local_timezone)
    tweet_timestamp_str = tweet_datetime_local.strftime(nicer_date_format)

    # egg is a custom intermediate form, which contains pre-processed parts of the tweet
    # for conversion into md and html.
    # (egg may be a strange name, but it's definitely more practical than intermediate_form.)
    egg = {
        'id': tweet['id_str'],
        'timestamp_str': tweet_timestamp_str,
        'timestamp': tweet_timestamp,
        'urls': [],
        'media': {},
        'original_tweet_url': f"https://twitter.com/{own_user_data.handle}/status/{tweet['id_str']}",
        'icon_url': rel_url(paths.file_tweet_icon, paths.example_file_output_tweets)
    }

    if outer_tweet is not None:
        egg['is_retweeted'] = True
        retweet_datetime = datetime.datetime.strptime(outer_tweet['created_at'], original_date_format)
        retweet_timestamp = int(round(retweet_datetime.timestamp()))
        retweet_datetime_local = datetime.datetime.fromtimestamp(retweet_datetime.timestamp(), tz=local_timezone)
        retweet_timestamp_str = retweet_datetime_local.strftime(nicer_date_format)

        egg['retweeted_timestamp_str'] = retweet_timestamp_str
        egg['retweeted_timestamp'] = retweet_timestamp

    if has_path(tweet, ['user', 'id_str']):
        egg['user_id'] = tweet['user']['id_str']
        egg['outer_user_id'] = own_user_data.user_id
    else:
        egg['user_id'] = own_user_data.user_id

    # Process full_text
    # Extract users which are replied to
    full_text = tweet['full_text']
    if has_path(tweet, ['in_reply_to_status_id']):
        # match and remove all occurrences of '@username ' at the start of the body
        replying_to = re.match(r'^(@[0-9A-Za-z_]* )*', full_text)[0]
        if replying_to:
            full_text = full_text[len(replying_to):]
        else:
            # no '@username ' in the body: we're replying to self
            replying_to = f'@{own_user_data.handle}'
        names = replying_to.split()
        # some old tweets lack 'in_reply_to_screen_name': use it if present, otherwise fall back to names[0]
        in_reply_to_screen_name = tweet['in_reply_to_screen_name'] if 'in_reply_to_screen_name' in tweet else names[0]
        # create a list of names of the form '@name1, @name2 and @name3' - or just '@name1' if there is only one name
        egg['name_list'] = ', '.join(names[:-1]) + (f' and {names[-1]}' if len(names) > 1 else names[0])
        in_reply_to_status_id = tweet['in_reply_to_status_id']
        egg['replying_to_url'] = f'https://twitter.com/{in_reply_to_screen_name}/status/{in_reply_to_status_id}'

    egg['full_text'] = full_text
    
    # for old tweets before embedded t.co redirects were added, ensure the links are
    # added to the urls entities list so that we can build correct links later on.
    if 'entities' in tweet and 'media' not in tweet['entities'] and len(tweet['entities'].get("urls", [])) == 0:
        for word in tweet['full_text'].split():
            # TODO: what about URLs without '://' ?
            if "://" in word:  # checking this might be quicker than trying and handling the exception
                try:
                    url = urlparse(word)
                except ValueError:
                    pass  # don't crash when trying to parse something that looks like a URL but actually isn't
                else:
                    if url.scheme != '' and url.netloc != '' and not word.endswith('\u2026'):
                        # Shorten links similar to twitter
                        netloc_short = url.netloc[4:] if url.netloc.startswith("www.") else url.netloc
                        path_short = url.path if len(url.path + '?' + url.query) < 15 \
                            else (url.path + '?' + url.query)[:15] + '\u2026'
                        egg['urls'].append({
                            'short_url': word,
                            'expanded_url': word,
                            'display_url': netloc_short + path_short,
                        })

    if has_path(tweet, ['entities', 'urls']):
        for url in tweet['entities']['urls']:
            if 'url' in url and 'expanded_url' in url:
                # TODO if expanded_url has the form of a Tweet URL, that is an embedded / quoted tweet
                #  we would have to retrieve the inner tweet here, and put it into egg, since the
                #  format-specific convert functions don't have access to all tweets.
                #  Also we should probably extract a function convert_tweet_to_egg so that
                #  we can easily pass two nested eggs later.
                egg['urls'].append({
                    'short_url': url['url'],
                    'expanded_url': url['expanded_url'],
                    'display_url': url['display_url'],
                })

                matches = re.match(r'^https://twitter.com/([0-9A-Za-z_]*)/status/(\d+)$', url['expanded_url'])
                if matches:
                    quoted_id = matches[2]
                    if known_tweets is not None and quoted_id in known_tweets:
                        egg['inner_tweet'] = known_tweets[quoted_id]
                    elif "from_archive" in tweet and tweet["from_archive"] is True:
                        print(f'Tweet {tweet["id_str"]} is part of the original archive and quotes tweet {quoted_id} '
                              f'but quoted tweet is not known.')
                        # if a tweet that is not from the archive contains a tweet, it's expected to be unknown,
                        # since we don't recurse deeper into quoted tweets.

    if media_sources is not None:
        egg['media'] = collect_media_ids_from_tweet(tweet, media_sources, paths)

    profile_image_size_suffix = "_x96"
    add_user_metadata_to_egg(egg, users, extended_user_data, profile_image_size_suffix)

    md = convert_tweet_to_md(egg, paths)
    html = convert_tweet_to_html(
        egg, known_tweets, own_user_data, users, extended_user_data, media_sources, local_timezone, paths, depth
    )

    # Do some other stuff that is traditionally done while converting a tweet
    # This used to get the "simple" users dict, but we use extended_user_data now.
    # Not sure if we still need this step at all?
    # collect_user_connections_from_tweet(tweet, users)

    timestamp: int = egg['timestamp']
    if 'is_retweeted' in egg and egg['is_retweeted'] is True:
        timestamp = egg['retweeted_timestamp']

    return timestamp, md, html


def add_user_metadata_to_egg(egg: dict, users: dict, extended_user_data: dict, profile_image_size_suffix: str) -> dict:
    """
    adds user metadata like name, screen name, description and profile image to an egg
    """
    if egg['user_id'] in extended_user_data:
        user: dict = extended_user_data[egg['user_id']]
        user_screen_name = user["screen_name"]
        user_description = user["description"]
        user_name = user["name"]
        user_id_str = user["id_str"]
        if 'profile_image_url_https' in user and user['profile_image_url_https'] is not None:
            profile_image_url_https = user['profile_image_url_https'].replace("_normal", profile_image_size_suffix)
        else:
            profile_image_url_https = None
        # TODO maybe link to nitter or local profile page, add config for this
        user_profile_url = f'https://twitter.com/{user_screen_name}'
    elif egg['user_id'] in users:
        user: UserData = users[egg['user_id']]
        user_screen_name = user.handle
        user_description = "(user profile not available)"
        user_name = "(unknown / @{user.handle})"
        user_id_str = str(egg['user_id'])
        profile_image_url_https = None
        user_profile_url = f'https://twitter.com/{user_screen_name}'
    else:
        user_screen_name = "unknown_user"
        user_description = "(user profile not available)"
        user_name = "(unknown)"
        user_id_str = str(egg['user_id'])
        profile_image_url_https = None
        user_profile_url = f'https://twitter.com/i/user/{user_id_str}'

    egg['user_screen_name'] = user_screen_name
    egg['user_description'] = user_description
    egg['user_name'] = user_name
    egg['user_id_str'] = user_id_str
    egg['profile_image_url_https'] = profile_image_url_https
    egg['user_profile_url'] = user_profile_url

    return egg


def convert_tweet_to_html_head( 
    egg: dict,
    paths: PathConfig,
) -> str:
    """Returns the <div class="tweet-header"> with all needed contents for a tweet."""

    # TODO maybe link to nitter, add config for this
    tweet_url = f'https://twitter.com/{egg["user_screen_name"]}/status/{egg["id"]}'

    if 'retweeted_timestamp_str' in egg:
        timestamp = f'<span class="tweet-timestamp">originally posted at <a title="Tweet (twitter.com)" ' \
                    f'href="{tweet_url}">{egg["timestamp_str"]}</a></span>'
        retweet_timestamp = f'<span class="retweet-timestamp">retweeted at {egg["retweeted_timestamp_str"]}</spn>'
    else:
        timestamp = f'<a class="tweet-timestamp" title="Tweet (twitter.com)" ' \
                    f'href="{tweet_url}">{egg["timestamp_str"]}</a>'
        retweet_timestamp = ''

    # build profile image output:
    if egg["profile_image_url_https"] is None:
        print(f'user {egg["user_id_str"]} has no profile_image_url_https')
        profile_image_html = ""
    else:
        file_extension = os.path.splitext(egg["profile_image_url_https"])[1]
        profile_image_file_name = egg["user_id_str"] + file_extension
        profile_image_file_path = os.path.join(paths.dir_output_media, "profile-images", profile_image_file_name)
        # In the future, the five lines above can be replaced by this:
        # profile_image_file_path = user['profile_image_file_path']

        profile_image_rel_url = rel_url(profile_image_file_path, paths.example_file_output_tweets)
        # TODO the entire group profile_image + user_name + user_handle should link to a local profile
        profile_image_html = f'<a class="profile-picture" href="{profile_image_rel_url}" ' \
                             f'title="Enlarge profile picture (local)">' \
                             f'<img width="48" src="{profile_image_rel_url}" /></a>'

    user_name_html = f'<span class="user-name" title="{egg["user_description"]}">{egg["user_name"]}</span>'
    user_handle_html = f'<a class="user-handle" title="User profile and tweets (twitter.com)" ' \
                       f'href="{egg["user_profile_url"]}">@{egg["user_screen_name"]}</a>'
    
    return f'<div class="tweet-header">{profile_image_html}' \
           f'<div class="upper-line">{user_handle_html}{timestamp}</div>' \
           f'<div class="lower-line">{user_name_html}{retweet_timestamp}</div></div>'


def convert_tweet_to_html(
    egg: dict,
    known_tweets: dict,
    own_user_data: UserData,
    users: dict[str, UserData],
    extended_user_data: dict,
    media_sources: dict,
    local_timezone: ZoneInfo,
    paths: PathConfig,
    depth: int,
) -> str:
   
    body_html = egg['full_text'].replace('\n', '<br/>\n')

    # if the tweet is a reply, construct a header that links the names
    # of the accounts being replied to the tweet being replied to
    pre_header_html = ''

    if 'is_retweeted' in egg:
        outer_user = extended_user_data[egg['outer_user_id']]
        outer_user_name = f'<span class="user-name" title="{outer_user["description"]}">{outer_user["name"]}</span>'
        pre_header_html += f'<div class="tweet-pre-header">Retweeted by {outer_user_name}</div>'
    elif 'replying_to_url' in egg:
        pre_header_html += f'<div class="tweet-pre-header">Replying to <a href="{egg["replying_to_url"]}">' \
                           f'{egg["name_list"]}</a></div>'

    # replace t.co URLs with their original versions
    for url in egg['urls']:
        display_url = url['display_url']
        expanded_url = url['expanded_url']
        expanded_url_html = f'<a href="{expanded_url}">{display_url}</a>'
        body_html = body_html.replace(url['short_url'], expanded_url_html)

    # TODO add links for mentions

    header_html = convert_tweet_to_html_head(egg, paths)

    all_media_html = ""
    # handle media: append img tags and remove image URL from text
    for media in egg['media'].values():
        original_url = media['original_url']
        # best_quality_url = media['best_quality_url'] # online URL of the media,
        # could be useful if the file does not exist locally
        local_filename = media['local_filename']
        media_url = rel_url(local_filename, paths.example_file_output_tweets)
        if media['type'] == 'photo':
            single_image_html = f'<br/>\n<a href="{media_url}"><img width="400px" src="{media_url}" ' \
                                f'title="{media["alt_text"]}" /></a>\n'
        else:
            single_image_html = f'<br/>\n<video width="400px" controls><source src="{media_url}">' \
                                f'Your browser does not support the video tag.</video>\n'

        body_html = body_html.replace(original_url, '')
        all_media_html = all_media_html + single_image_html

    # Convert inner tweet. We need the tweet author's user data, which may be (partially) unknown.
    if has_path(egg, ['inner_tweet']):
        inner_tweet = egg['inner_tweet']
        if has_path(inner_tweet, ['user', 'id_str']):
            inner_user_id = inner_tweet['user']['id_str']
            if has_path(extended_user_data, [inner_user_id]):
                inner_user = extended_user_data[inner_user_id]
                inner_user_data = UserData(inner_user_id, inner_user['screen_name'])
            else:
                inner_user_data = UserData(inner_user_id, 'unknown user')
        else:
            inner_user_data = own_user_data
        try:
            _, _, inner_tweet_html = convert_tweet(
                inner_tweet,
                known_tweets,
                inner_user_data,
                media_sources,
                users,
                extended_user_data,
                local_timezone,
                paths,
                depth=depth+1,
            )
        except Exception as error:
            inner_tweet_html = f"<i>Could not convert quoted tweet because of error: {error}</i>"
        all_media_html += f'<div class="quote-tweet">{inner_tweet_html}</div>'

    full_html = pre_header_html + header_html + '<div class="tweet-body">\n' + body_html + '\n' + \
        all_media_html + f'\n</div>\n'
    # Twitter icon, probably obsolete now that the tweet link is in the timestamp:
    # f'<a href="{egg["original_tweet_url"]}">'
    # f'<img src="{egg["icon_url"]}" width="12" />&nbsp;{egg["timestamp_str"]}</a>\n'

    return full_html


def convert_tweet_to_md(
    egg: dict,
    paths: PathConfig,
) -> str:

    body_markdown = egg['full_text']

    # if the tweet is a retweet, insert the handle of the user being retweeted before the body:
    if 'is_retweeted' in egg and 'user_screen_name' in egg:
        body_markdown = f'RT @{egg["user_screen_name"]}: {body_markdown}'

    # replace t.co URLs with their original versions
    for url in egg['urls']:
        body_markdown = body_markdown.replace(url['short_url'], url['expanded_url'])

    # if the tweet is a reply, construct a header that links the names
    # of the accounts being replied to the tweet being replied to
    header_markdown = ''
    
    if 'name_list' in egg:
        header_markdown += f'Replying to [{escape_markdown(egg["name_list"])}]({egg["replying_to_url"]})\n\n'

    # escape tweet body for markdown rendering:
    body_markdown = escape_markdown(body_markdown)
    all_media_md = ""

    # handle media: append img tags and remove image URL from text
    for media in egg['media'].values():
        original_url = media['original_url']
        local_filename = media['local_filename']
        media_url = rel_url(local_filename, paths.example_file_output_tweets)

        if media['type'] == 'photo':
            alt_text = escape_markdown(media["alt_text"].replace("\n", " "))
            single_image_md = f'\n![{alt_text}]({media_url})\n'
        else:
            single_image_md = f'\n<video width="400px" controls><source src="{media_url}">' \
                              f'Your browser does not support the video tag.</video>\n'

        body_markdown = body_markdown.replace(escape_markdown(original_url), '')
        all_media_md = all_media_md + single_image_md

    # make the body a quote
    body_markdown = '> ' + '\n> '.join(body_markdown.splitlines())
    # append the original Twitter URL as a link
   
    body_markdown = header_markdown + body_markdown + '\n' + all_media_md + \
        f'\n\n<img src="{egg["icon_url"]}" width="12" /> [{egg["timestamp_str"]}]({egg["original_tweet_url"]})'
    return body_markdown


def collect_media_ids_from_tweet(tweet, media_sources: Optional[dict], paths: PathConfig) -> dict[str, dict]:
    """
    This function is dual-use:
    If you pass media_sources, information about the media will be put there.
    You can use it to download high-res media from Twitter.
    The return value will be a list of this tweet's media, which you can use to create html or md output.
    """
    tweet_id_str = tweet['id_str']
    tweet_media = {}

    if has_path(tweet, ['entities', 'media']) and has_path(tweet, ['extended_entities', 'media']) and \
            len(tweet['entities']['media']) > 0 and 'url' in tweet['entities']['media'][0]:
        
        # Not sure I understand this code - if a tweet has more than 1 attached image, is the first media's 
        # URL the only one in the full text, and represents all (up to four) images?
        original_url = tweet['entities']['media'][0]['url']
        for media in tweet['extended_entities']['media']:
            if 'url' in media and 'media_url' in media and media['media_url'] is not None:
                # Note: for videos the media_url points to a jpeg file, which is the thumbnail of the video
                original_expanded_url = media['media_url']
                original_filename = os.path.split(original_expanded_url)[1]
                archive_media_filename = tweet_id_str + '-' + original_filename
                media_id = media['id_str']

                media_type = media['type']
                if media_type == "photo":
                    # Save the online location of the best-quality version of this file, for later upgrading if wanted
                    best_quality_url = f'https://pbs.twimg.com/media/{original_filename}:orig'
                    file_output_media = os.path.join(paths.dir_output_media, archive_media_filename)
                    alt_text = ''
                    if has_path(media, ['ext_alt_text']):
                        alt_text = media['ext_alt_text']

                    tweet_media[media_id] = {
                        'type': media_type,
                        'original_url': original_url,
                        'best_quality_url': best_quality_url,
                        'local_filename': file_output_media,
                        'alt_text': alt_text,
                        'id': media_id,
                    }
                    if media_sources is not None:
                        media_sources[file_output_media] = best_quality_url
                elif media_type in ["video", "animated_gif"]:
                    # For videos, the filename might be found like this:
                    # Is there any other file that includes the tweet_id in its filename?
                    archive_media_paths = glob.glob(os.path.join(paths.dir_input_media, tweet_id_str + '*'))
                    if len(archive_media_paths) > 0:
                        file_output_media = None
                        for archive_media_path in archive_media_paths:
                            archive_media_filename = os.path.split(archive_media_path)[-1]
                            file_output_media = os.path.join(paths.dir_output_media, archive_media_filename)
                            if not os.path.isfile(file_output_media):
                                shutil.copy(archive_media_path, file_output_media)
                    else:
                        # when we reach this line, archive_media_filename and file_output_media still point to the 
                        # name of the thumbnail jpeg, but we need a filename for the actual video. We can't
                        # find out the name yet, so we just forget the jpeg name.
                        archive_media_filename = None
                        file_output_media = None
                        
                    # Save the online location of the best-quality version of this file,
                    # for later upgrading if wanted
                    if 'video_info' in media and 'variants' in media['video_info']:
                        best_quality_url = ''
                        best_bitrate = -1  # some valid videos are marked with bitrate=0 in the JSON
                        for variant in media['video_info']['variants']:
                            if 'bitrate' in variant:
                                bitrate = int(variant['bitrate'])
                                if bitrate > best_bitrate:
                                    best_quality_url = variant['url']
                                    best_bitrate = bitrate
                        if best_bitrate == -1:
                            print(f"Warning No URL found for the actual video file in tweet '{tweet_id_str}'. '\
                                'Media URL: '{original_url}', expands to: '{original_expanded_url}'.")
                        else:
                            # if we don't have archive_media_filename and file_output_media, we try to build it
                            # from the URL
                            if archive_media_filename is None or file_output_media is None:
                                archive_media_filename = os.path.split(best_quality_url)[-1]
                                if '?' in archive_media_filename:
                                    archive_media_filename = archive_media_filename[0:archive_media_filename.find('?')]
                                file_output_media = os.path.join(paths.dir_output_media, archive_media_filename)

                            if media_sources is not None:
                                media_sources[os.path.join(paths.dir_output_media, archive_media_filename)] = \
                                    best_quality_url
                            tweet_media[media_id] = {
                                'type': media_type,
                                'original_url': original_url,
                                'best_quality_url': best_quality_url,
                                'local_filename': file_output_media,
                                'alt_text': '',
                                'id': media_id,
                            }
                else:
                    print(f"Unknown media type: {media_type}")
    return tweet_media


def collect_user_connections_from_tweet(tweet: dict, users: dict) -> None:
    # extract user_id:handle connections
    if 'in_reply_to_user_id' in tweet and 'in_reply_to_screen_name' in tweet and \
            tweet['in_reply_to_screen_name'] is not None:
        reply_to_id = tweet['in_reply_to_user_id']
        if int(reply_to_id) >= 0:  # some ids are -1, not sure why
            handle = tweet['in_reply_to_screen_name']
            if str(reply_to_id) not in users.keys():
                users[str(reply_to_id)] = UserData(user_id=reply_to_id, handle=handle)
    if 'entities' in tweet and 'user_mentions' in tweet['entities'] and tweet['entities']['user_mentions'] is not None:
        for mention in tweet['entities']['user_mentions']:
            if mention is not None and 'id' in mention and 'screen_name' in mention:
                mentioned_id = mention['id']
                if int(mentioned_id) >= 0:  # some ids are -1, not sure why
                    handle = mention['screen_name']
                    if handle is not None and str(mentioned_id) not in users.keys():
                        users[str(mentioned_id)] = UserData(user_id=mentioned_id, handle=handle)


def find_files_input_tweets(dir_path_input_data):
    """Identify the tweet archive's file and folder names -
    they change slightly depending on the archive size it seems."""
    input_tweets_file_templates = ['tweet.js', 'tweets.js', 'tweets-part*.js']
    files_paths_input_tweets = []
    for input_tweets_file_template in input_tweets_file_templates:
        files_paths_input_tweets += glob.glob(os.path.join(dir_path_input_data, input_tweets_file_template))
    if len(files_paths_input_tweets) == 0:
        print(f'Error: no files matching {input_tweets_file_templates} in {dir_path_input_data}')
        exit()
    return files_paths_input_tweets


def find_dir_input_media(dir_path_input_data):
    input_media_dir_templates = ['tweet_media', 'tweets_media']
    input_media_dirs = []
    for input_media_dir_template in input_media_dir_templates:
        input_media_dirs += glob.glob(os.path.join(dir_path_input_data, input_media_dir_template))
    if len(input_media_dirs) == 0:
        print(f'Error: no folders matching {input_media_dir_templates} in {dir_path_input_data}')
        exit()
    if len(input_media_dirs) > 1:
        print(f'Error: multiple folders matching {input_media_dir_templates} in {dir_path_input_data}')
        exit()
    return input_media_dirs[0]


def download_file_if_larger(url, filename, progress: str) -> Tuple[bool, Optional[str], int]:
    """Attempts to download from the specified URL. Overwrites file if larger.
       Returns whether the file is now known to be the largest available, and the number of bytes downloaded.
    """
    requests = import_module('requests')
    imagesize = import_module('imagesize')

    # Request the URL (in stream mode so that we can conditionally abort depending on the headers)
    pref = "    "
    logging.info(f'URL: {url}')
    print(f'{pref}Requesting headers...')
    print(progress, end='\r')
    if os.path.exists(filename):
        byte_size_before = os.path.getsize(filename)
    else:
        byte_size_before = 0
    try:
        with requests.get(url, stream=True, timeout=2) as res:
            if not res.status_code == 200:
                # Try to get content of response as `res.text`.
                # For twitter.com, this will be empty in most (all?) cases.
                # It is successfully tested with error responses from other domains.
                logging.error(f'{pref}Download failed with status "{res.status_code} {res.reason}". '
                                f'Response content: "{res.text}"')
                return False, f"status {res.status_code}", 0
            byte_size_after = int(res.headers['content-length'])
            if byte_size_after != byte_size_before:
                # Proceed with the full download
                tmp_filename = filename+'.tmp'
                print(f'{pref}Downloading...                                                          ')
                print(progress, end='\r')
                with open(tmp_filename, 'wb') as f:
                    shutil.copyfileobj(res.raw, f)
                post = '' # f'{byte_size_after/2**20:.1f}MB downloaded'

                if byte_size_before > 0:
                    width_before, height_before = imagesize.get(filename)
                    width_after, height_after = imagesize.get(tmp_filename)
                    pixels_before, pixels_after = width_before * height_before, width_after * height_after
                    pixels_percentage_increase = 100.0 * (pixels_after - pixels_before) / pixels_before

                    if width_before == -1 and height_before == -1 and width_after == -1 and height_after == -1:
                        # could not check size of both versions, probably a video or unsupported image format
                        os.replace(tmp_filename, filename)
                        bytes_percentage_increase = 100.0 * (byte_size_after - byte_size_before) / byte_size_before
                        logging.info(f'{pref}SUCCESS. New version is {bytes_percentage_increase:3.0f}% '
                                     f'larger in bytes (pixel comparison not possible). {post}')
                        return True, None, byte_size_after
                    elif width_before == -1 or height_before == -1 or width_after == -1 or height_after == -1:
                        # could not check size of one version, this should not happen (corrupted download?)
                        logging.info(f'{pref}SKIPPED. Pixel size comparison inconclusive: '
                                     f'{width_before}*{height_before}px vs. {width_after}*{height_after}px. {post}')
                        return False, "pixelsize", byte_size_after
                    elif pixels_after >= pixels_before:
                        os.replace(tmp_filename, filename)
                        bytes_percentage_increase = 100.0 * (byte_size_after - byte_size_before) / byte_size_before
                        if bytes_percentage_increase >= 0:
                            logging.info(f'{pref}SUCCESS. New version is {bytes_percentage_increase:3.0f}% '
                                         f'larger in bytes and {pixels_percentage_increase:3.0f}% '
                                         f'larger in pixels. {post}')
                        else:
                            logging.info(f'{pref}SUCCESS. New version is actually {-bytes_percentage_increase:3.0f}% '
                                         f'smaller in bytes but {pixels_percentage_increase:3.0f}% '
                                         f'larger in pixels. {post}')
                        return True, None, byte_size_after
                    else:
                        logging.info(f'{pref}SKIPPED. Online version has {-pixels_percentage_increase:3.0f}% '
                                     f'smaller pixel size. {post}')
                        return True, None, byte_size_after
                else:  # File did not exist before
                    os.replace(tmp_filename, filename)
                    logging.info(f'{pref}SUCCESS. Previously missing file is present now. {post}')
                    return True, None, byte_size_after
            else:
                logging.info(f'{pref}SKIPPED. Online version is same byte size, assuming same content. Not downloaded.')
                return True, None, 0
    except Exception as err:
        logging.error(f"{pref}FAIL. Media couldn't be retrieved from {url} because of exception: {err}")
        return False, f"{err}", 0


def download_larger_media(media_sources: dict, error_codes_to_exclude: list[str], paths: PathConfig, state: dict):
    """Uses (filename, URL) items in media_sources to download files from remote storage.
       Aborts download if the remote file is the same size or smaller than the existing local version.
       Retries the failed downloads several times, with increasing pauses between each to avoid being blocked.
    """
    # Log to file as well as the console
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(message)s')
    mkdirs_for_file(paths.file_download_log)
    logfile_handler = logging.FileHandler(filename=paths.file_download_log, mode='w')
    logfile_handler.setLevel(logging.INFO)
    logging.getLogger().addHandler(logfile_handler)
    # Download new versions
    start_time = time.time()
    total_bytes_downloaded = 0
    sleep_time = 0.25
    remaining_tries = 5

    remaining_media_sources = list(media_sources.items())

    try:
        while remaining_tries > 0:
            new_remaining_media_sources = []
            for item in remaining_media_sources:
                local_media_path, media_url = item[0], item[1]
                if state.get(media_url, {}).get('success'):
                    continue
                if state.get(media_url, {}).get('error') in error_codes_to_exclude:
                    continue
                new_remaining_media_sources.append((local_media_path, media_url))

            remaining_media_sources = new_remaining_media_sources

            number_of_files = len(remaining_media_sources)
            success_count = 0
            retries = []

            if number_of_files > 0:
                print(f"\nTry to download remaining {number_of_files} files...\n")

            for index, (local_media_path, media_url) in enumerate(remaining_media_sources):
                # show % done and estimated remaining time:
                time_elapsed: float = time.time() - start_time
                estimated_time_per_file: float = time_elapsed / (index + 1)

                time_remaining_string = format_duration(
                    seconds=(number_of_files - (index + 1)) * estimated_time_per_file
                )

                if index + 1 == number_of_files:
                    percent = '100'
                else:
                    percent = f'{(100*(index+1)/number_of_files):.1f}'

                progress = f'{index+1:6d}/{number_of_files:5d} = {percent}% done. About {time_remaining_string} remaining.'
                # Sleep briefly, in an attempt to minimize the possibility of triggering some auto-cutoff mechanism
                print(progress, end='\r')
                time.sleep(sleep_time)

                success, download_error, bytes_downloaded = download_file_if_larger(
                    media_url, local_media_path, progress
                )
                state.update(
                    {media_url: {"local": local_media_path, "success": success, "error": download_error, "downloaded": bytes_downloaded}}
                )

                if success:
                    success_count += 1
                else:
                    retries.append((local_media_path, media_url))
                total_bytes_downloaded += bytes_downloaded

            remaining_media_sources = retries
            remaining_tries -= 1
            sleep_time += 2
            logging.info(f'\n{success_count} of {number_of_files} tested media files '
                         f'are known to be the best-quality available.\n')
            if len(retries) == 0:
                break
            if remaining_tries > 0:
                print(f'----------------------\n\nRetrying the ones that failed, with a longer sleep. '
                      f'{remaining_tries} tries remaining.\n')
        end_time = time.time()

    except KeyboardInterrupt as e:
        # save current state of download to avoid losing all of it when running the script is interrupted
        export_media_download_state(state, paths)
        print(f'\nsaved current media download state to {paths.file_media_download_state}')
        sys.exit(1)

    logging.info(f'Total downloaded: {total_bytes_downloaded/2**20:.1f}MB = {total_bytes_downloaded/2**30:.2f}GB')
    logging.info(f'Time taken: {end_time-start_time:.0f}s')
    print(f'Wrote log to {paths.file_download_log}')


def collect_user_ids_from_tweets(known_tweets) -> list:
    """
    find user ids in tweets
    """
    user_ids_set = set()
    for tweet in known_tweets.values():
        if has_path(tweet, ['user', 'id_str']):
            user_ids_set.add(tweet['user']['id_str'])
        if 'in_reply_to_user_id' in tweet and tweet['in_reply_to_user_id'] is not None:
            if int(tweet['in_reply_to_user_id']) >= 0:  # some ids are -1, not sure why
                user_ids_set.add(str(tweet['in_reply_to_user_id']))
        if 'entities' in tweet:
            entities = tweet['entities']
            if 'user_mentions' in entities and entities['user_mentions'] is not None:
                for mention in entities['user_mentions']:
                    if mention is not None and 'id' in mention:
                        mentioned_id = mention['id']
                        if int(mentioned_id) >= 0:  # some ids are -1, not sure why
                            user_ids_set.add(str(mentioned_id))
        if has_path(tweet, ['retweeted_status', 'user', 'id_str']):
            user_ids_set.add(tweet['retweeted_status']['user']['id_str'])
    return list(user_ids_set)


def load_tweets(paths: PathConfig) -> dict[str, dict]:
    known_tweets: dict[str, dict] = dict()

    # Load tweets that we saved in an earlier run between pass 2 and 3
    tweet_dict_filename = os.path.join(paths.dir_output_cache, 'known_tweets.json')
    if os.path.exists(tweet_dict_filename):
        with open(tweet_dict_filename, 'r', encoding='utf8') as f:
            known_tweets = json.load(f)
    
    # Fist pass: Load tweets from all archive files and add them to known_tweets
    for tweets_js_filename in paths.files_input_tweets:
        json_result = read_json_from_js_file(tweets_js_filename)
        for tweet in json_result:
            tweet = unwrap_tweet(tweet)
            tweet['from_archive'] = True
            add_known_tweet(known_tweets, tweet['id_str'], tweet)

    return known_tweets


def collect_tweet_ids_from_tweets(known_tweets: dict[str, dict]) -> set[str]:
    """Second pass: Iterate through all those tweets"""
    tweet_ids_to_download = set()
    for tweet in known_tweets.values():
        tweet_ids_to_download.update(collect_tweet_references(tweet, known_tweets))
    return tweet_ids_to_download


def download_tweets(
        known_tweets: dict[str, dict],
        tweet_ids_to_download: Union[list[str], set[str]],
        paths: PathConfig
) -> None:
    """(Maybe) download referenced tweets"""
    if len(tweet_ids_to_download) > 0:
        print(f"Found references to {len(tweet_ids_to_download)} tweets which should be downloaded.")
        
        tweet_ids_to_download_completely_new = []
        tweet_ids_to_download_api_returned_null = []
        tweet_ids_to_download_can_be_extended = []
        
        for tweet_id in tweet_ids_to_download:
            if tweet_id in known_tweets.keys():
                known_tweet = known_tweets[tweet_id]
                if known_tweet is None:
                    tweet_ids_to_download_completely_new.append(tweet_id)
                elif 'api_returned_null' in known_tweet and known_tweet['api_returned_null'] is True:
                    tweet_ids_to_download_api_returned_null.append(tweet_id)
                else:
                    tweet_ids_to_download_can_be_extended.append(tweet_id)
            else:
                tweet_ids_to_download_completely_new.append(tweet_id)
        print()
        print("Breakdown of availability:")
        print(f" * {len(tweet_ids_to_download_completely_new)} completely unknown to the local cache.")
        print(f" * {len(tweet_ids_to_download_api_returned_null)} known to be unavailable from previous runs "
              f"of this script. They will not be tried again.")
        print(f" * {len(tweet_ids_to_download_can_be_extended)} contained in the local cache, but might lack some "
              f"details which could be supplemented.")

        # ignore tweet_ids_to_download_api_returned_null from now on by using just the other lists
        # TODO there should be an override (via question, config or command line arg) to re-try those anyway
        tweet_ids_to_download = tweet_ids_to_download_completely_new + tweet_ids_to_download_can_be_extended

    retried_times = 0
    max_retries = 5

    if len(tweet_ids_to_download) == 0:
        print("All referenced tweets are present, nothing to download.")
    else:
        print()
        print("Please note that the downloaded tweets will not be included in the generated output yet.")
        print("Anyway, we recommend to download the tweets now, just in case Twitter (or its API which")
        print("we use), won't be available forever. A future version of this script will be able to")
        print("include the downloaded tweets into the output, even if Twitter should not be available then.")
        print()

    while len(tweet_ids_to_download) > 0 and retried_times < max_retries:
        estimated_download_time_seconds = math.ceil(len(tweet_ids_to_download) / 100) * 2
        estimated_download_time_str = format_duration(estimated_download_time_seconds)
        if get_consent(f"OK to download {len(tweet_ids_to_download)} tweets from twitter? "
                       f"This would take about {estimated_download_time_str}.", key='download_tweets'):
            # TODO maybe let the user choose which of the tweets to download, by selecting a subset of those reasons
            requests = import_module('requests')
            try:
                with requests.Session() as session:
                    bearer_token = 'AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xn' \
                                   'Zz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA'
                    guest_token = get_twitter_api_guest_token(session, bearer_token)
                    # TODO We could download user data together with the tweets, because we will need it anyway.
                    #  But we might download the data for each user multiple times then.
                    downloaded_tweets, tweet_ids_to_download = get_tweets(
                        session, bearer_token, guest_token, list(tweet_ids_to_download), False
                    )

                    for downloaded_tweet_id in downloaded_tweets:
                        downloaded_tweet = downloaded_tweets[downloaded_tweet_id]
                        # downloaded_tweet may be None,
                        # which means that the API explicitly returned null (e.g. deleted user)
                        if downloaded_tweet is not None:
                            downloaded_tweet = unwrap_tweet(downloaded_tweet)
                            downloaded_tweet['from_api'] = True
                            downloaded_tweet['download_with_user'] = False
                            downloaded_tweet['download_with_alt_text'] = True
                        add_known_tweet(known_tweets, downloaded_tweet_id, downloaded_tweet)
                    print("Download finished. Saving tweets to disk - do not kill this script until this is done!")
                    tweet_dict_filename = os.path.join(paths.dir_output_cache, 'known_tweets.json')
                    with open(tweet_dict_filename, "w") as outfile:
                        json.dump(known_tweets, outfile, indent=2)
                    print(f"Saved {len(known_tweets)} tweets to '{tweet_dict_filename}'.")

            except Exception as err:
                # this code is rather unlikely to be reached, since get_tweets has internal error handling.
                print(f'Failed to download tweets: {err}')
                time.sleep(2)  # sleep 2 seconds before trying again to avoid rate-limit

            if len(tweet_ids_to_download) > 0:
                retried_times += 1
                print("Not all tweets could be downloaded, but you can retry if you want.")
        else:
            # Don't ask again and again if the user said 'no'
            break


def convert_tweets(
        own_user_data: UserData,
        users: dict[str, UserData],
        extended_user_data: dict,
        html_template: dict,
        known_tweets: dict[str, dict],
        local_timezone: ZoneInfo,
        paths: PathConfig
) -> dict:
    """Third pass: convert tweets, using the downloaded references from pass 2"""
    converted_tweets = []
    media_sources = {}
    for tweet in known_tweets.values():
        try:
            # known_tweets will contain tweets that do not belong directly into our output
            if 'from_archive' in tweet and tweet['from_archive'] is True:
                # Generate output for this tweet, and at the same time collect its media ids
                converted_tweets.append(convert_tweet(
                    tweet,
                    known_tweets,
                    own_user_data,
                    media_sources,
                    users,
                    extended_user_data,
                    local_timezone,
                    paths,
                    depth=0,
                ))
            else:
                # Only collect media ids
                collect_media_ids_from_tweet(tweet, media_sources, paths)
        except EmptyTweetFullTextError as err:
            print(f"Could not convert tweet {tweet['id_str']} because: {err}")
        except QuoteRecursionDepthError as err:
            print(f"Could not convert tweet {tweet['id_str']} because: {err}")
        except Exception as err:
            traceback.print_exc()
            print(f"Could not convert tweet {tweet['id_str']} because: {err}")
    converted_tweets.sort(key=lambda tup: tup[0])  # oldest first

    # Group tweets by month
    grouped_tweets = defaultdict(list)
    for timestamp, md, html in converted_tweets:
        # Use a (markdown) filename that can be imported into Jekyll: YYYY-MM-DD-your-title-here.md
        dt = datetime.datetime.fromtimestamp(timestamp)
        grouped_tweets[(dt.year, dt.month)].append((md, html))

    for (year, month), content in grouped_tweets.items():
        # Write into *.md files
        month_str = datetime.datetime.strftime(datetime.datetime.strptime(str(month), '%m'), '%b')
        md_string = f'## Tweets by @{own_user_data.handle} from {month_str} {year} \n\n'
        md_string += f'Time zone: {local_timezone}\n\n----\n\n'
        md_string += '\n\n----\n\n'.join(md for md, _ in content)
        md_path = paths.create_path_for_file_output_tweets(year, month, output_format="md")
        with open_and_mkdirs(md_path) as f:
            f.write(md_string)

        # Write into *.html files
        html_string = f'<h2>Tweets by @{own_user_data.handle} from {month_str} {year}</h2>'
        html_string += f'<p><small>Time zone: {local_timezone}</small></p><hr>'
        html_string += '<hr>\n'.join(html for _, html in content)
        html_path = paths.create_path_for_file_output_tweets(year, month, output_format="html")
        with open_and_mkdirs(html_path) as f:
            f.write(html_template['begin'])
            f.write(html_string)
            f.write(html_template['end'])
            
    print(f'Wrote {len(converted_tweets)} tweets to *.md and *.html, '
          f'with images and video embedded from {paths.dir_output_media}')

    return media_sources


def collect_user_ids_from_followings(paths) -> list:
    """
     Collect all user ids that appear in the followings archive data.
     (For use in bulk online lookup from Twitter.)
    """
    # read JSON file from archive
    following_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'following.js'))
    # collect all user ids in a list
    following_ids = []
    for follow in following_json:
        if 'following' in follow and 'accountId' in follow['following']:
            following_ids.append(follow['following']['accountId'])
    return following_ids


def parse_followings(users, user_id_url_template, paths: PathConfig):
    """Parse paths.dir_input_data/following.js, write to paths.file_output_following.
    """
    following = []
    following_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'following.js'))
    following_ids = []
    for follow in following_json:
        if 'following' in follow and 'accountId' in follow['following']:
            following_ids.append(follow['following']['accountId'])
    for following_id in following_ids:
        handle = users[following_id].handle if following_id in users else '~unknown~handle~'
        following.append(handle + ' ' + user_id_url_template.format(following_id))
    following.sort()
    following_output_path = paths.create_path_for_file_output_single(output_format="txt", kind="following")
    with open_and_mkdirs(following_output_path) as f:
        f.write('\n'.join(following))
    print(f"Wrote {len(following)} accounts to {following_output_path}")


def collect_user_ids_from_followers(paths) -> list:
    """
     Collect all user ids that appear in the followers archive data.
     (For use in bulk online lookup from Twitter.)
    """
    # read JSON file from archive
    follower_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'follower.js'))
    # collect all user ids in a list
    follower_ids = []
    for follower in follower_json:
        if 'follower' in follower and 'accountId' in follower['follower']:
            follower_ids.append(follower['follower']['accountId'])
    return follower_ids


def parse_followers(users, user_id_url_template, paths: PathConfig):
    """Parse paths.dir_input_data/followers.js, write to paths.file_output_followers.
    """
    followers = []
    follower_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'follower.js'))
    follower_ids = []
    for follower in follower_json:
        if 'follower' in follower and 'accountId' in follower['follower']:
            follower_ids.append(follower['follower']['accountId'])
    for follower_id in follower_ids:
        handle = users[follower_id].handle if follower_id in users else '~unknown~handle~'
        followers.append(handle + ' ' + user_id_url_template.format(follower_id))
    followers.sort()
    followers_output_path = paths.create_path_for_file_output_single(output_format="txt", kind="followers")
    with open_and_mkdirs(followers_output_path) as f:
        f.write('\n'.join(followers))
    print(f"Wrote {len(followers)} accounts to {followers_output_path}")


def chunks(lst: list, n: int):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def collect_user_ids_from_direct_messages(paths) -> list:
    """
     Collect all user ids that appear in the direct messages archive data.
     (For use in bulk online lookup from Twitter.)
    """
    # read JSON file from archive
    dms_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'direct-messages.js'))
    # collect all user ids in a set
    dms_user_ids = set()
    for conversation in dms_json:
        if 'dmConversation' in conversation and 'conversationId' in conversation['dmConversation']:
            dm_conversation = conversation['dmConversation']
            conversation_id = dm_conversation['conversationId']
            user1_id, user2_id = conversation_id.split('-')
            dms_user_ids.add(user1_id)
            dms_user_ids.add(user2_id)
    return list(dms_user_ids)


def parse_direct_messages(username, users, user_id_url_template, local_timezone: ZoneInfo, paths: PathConfig):
    """Parse paths.dir_input_data/direct-messages.js, write to one markdown file per conversation.
    """
    # read JSON file
    dms_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'direct-messages.js'))

    print('')  # blank line for readability

    # Parse the DMs and store the messages in a dict
    conversations_messages = defaultdict(list)
    for conversation in dms_json:
        if 'dmConversation' in conversation and 'conversationId' in conversation['dmConversation']:
            dm_conversation = conversation['dmConversation']
            conversation_id = dm_conversation['conversationId']
            user1_id, user2_id = conversation_id.split('-')
            messages = []
            if 'messages' in dm_conversation:
                for message in dm_conversation['messages']:
                    if 'messageCreate' in message:
                        message_create = message['messageCreate']
                        if all(tag in message_create for tag in ['senderId', 'recipientId', 'text', 'createdAt']):
                            from_id = message_create['senderId']
                            to_id = message_create['recipientId']
                            body = message_create['text']
                            # replace t.co URLs with their original versions
                            if 'urls' in message_create and len(message_create['urls']) > 0:
                                for url in message_create['urls']:
                                    if 'url' in url and 'expanded' in url:
                                        expanded_url = url['expanded']
                                        body = body.replace(url['url'], expanded_url)
                            # escape message body for markdown rendering:
                            body_markdown = escape_markdown(body)
                            # replace image URLs with image links to local files
                            if 'mediaUrls' in message_create \
                                    and len(message_create['mediaUrls']) == 1 \
                                    and 'urls' in message_create:
                                original_expanded_url = message_create['urls'][0]['expanded']
                                message_id = message_create['id']
                                media_hash_and_type = message_create['mediaUrls'][0].split('/')[-1]
                                media_id = message_create['mediaUrls'][0].split('/')[-2]
                                archive_media_filename = f'{message_id}-{media_hash_and_type}'
                                new_url = os.path.join(paths.dir_output_media, archive_media_filename)
                                archive_media_path = \
                                    os.path.join(paths.dir_input_data, 'direct_messages_media', archive_media_filename)
                                if os.path.isfile(archive_media_path):
                                    # found a matching image, use this one
                                    if not os.path.isfile(new_url):
                                        shutil.copy(archive_media_path, new_url)
                                    image_markdown = f'\n![]({new_url})\n'
                                    body_markdown = body_markdown.replace(
                                        escape_markdown(original_expanded_url), image_markdown
                                    )

                                    # Save the online location of the best-quality version of this file,
                                    # for later upgrading if wanted
                                    best_quality_url = \
                                        f'https://ton.twitter.com/i//ton/data/dm/' \
                                        f'{message_id}/{media_id}/{media_hash_and_type}'
                                    # there is no ':orig' here, the url without any suffix has the original size

                                    # TODO: a cookie (and a 'Referer: https://twitter.com' header)
                                    #  is needed to retrieve it, so the url might be useless anyway...

                                    # WARNING: Do not uncomment the statement below until the cookie problem is solved!
                                    # media_sources.append(
                                    #     (
                                    #         os.path.join(output_media_folder_name, archive_media_filename),
                                    #         best_quality_url
                                    #     )
                                    # )

                                else:
                                    archive_media_paths = glob.glob(
                                        os.path.join(paths.dir_input_data, 'direct_messages_media', message_id + '*'))
                                    if len(archive_media_paths) > 0:
                                        for archive_media_path in archive_media_paths:
                                            archive_media_filename = os.path.split(archive_media_path)[-1]
                                            media_url = os.path.join(paths.dir_output_media, archive_media_filename)
                                            if not os.path.isfile(media_url):
                                                shutil.copy(archive_media_path, media_url)
                                            video_markdown = f'\n<video controls><source src="{media_url}">' \
                                                             f'Your browser does not support the video tag.</video>\n'
                                            body_markdown = body_markdown.replace(
                                                escape_markdown(original_expanded_url), video_markdown
                                            )

                                    # TODO: maybe  also save the online location of the best-quality version for videos?
                                    #  (see above)

                                    else:
                                        print(f'Warning: missing local file: {archive_media_path}. '
                                              f'Using original link instead: {original_expanded_url})')

                            message_created_at: str = message_create['createdAt']  # example: 2022-01-27T15:58:52.744Z
                            created_at: datetime = datetime.datetime.strptime(
                                message_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 2019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)

                            from_handle = escape_markdown(users[from_id].handle) if from_id in users \
                                else user_id_url_template.format(from_id)
                            to_handle = escape_markdown(users[to_id].handle) if to_id in users \
                                else user_id_url_template.format(to_id)

                            # make the body a quote
                            body_markdown = '> ' + '\n> '.join(body_markdown.splitlines())
                            message_markdown = f'{from_handle} -> {to_handle}: ({created_at_local_time_str}) \n\n' \
                                               f'{body_markdown}'
                            messages.append((timestamp, message_markdown))

            # find identifier for the conversation
            other_user_id = user2_id if (user1_id in users and users[user1_id].handle == username) else user1_id

            # collect messages per identifying user in conversations_messages dict
            conversations_messages[other_user_id].extend(messages)

    # output as one file per conversation (or part of long conversation)
    num_written_messages = 0
    num_written_files = 0
    for other_user_id, messages in conversations_messages.items():
        # sort messages by timestamp
        messages.sort(key=lambda tup: tup[0])

        other_user_name = escape_markdown(users[other_user_id].handle) if other_user_id in users \
            else user_id_url_template.format(other_user_id)

        other_user_short_name: str = users[other_user_id].handle if other_user_id in users else other_user_id

        escaped_username = escape_markdown(username)

        # if there are more than 1000 messages, the conversation was split up in the twitter archive.
        # following this standard, also split up longer conversations in the output files:

        if len(messages) > 1000:
            for chunk_index, chunk in enumerate(chunks(messages, 1000)):
                markdown = ''
                markdown += f'### Conversation between {escaped_username} and {other_user_name}, ' \
                            f'part {chunk_index+1}: ###\n\n'
                markdown += f'Time zone: {local_timezone}\n\n----\n\n'
                markdown += '\n\n----\n\n'.join(md for _, md in chunk)
                conversation_output_path = paths.create_path_for_file_output_dms(
                    name=other_user_short_name, index=(chunk_index + 1), output_format="md"
                )

                # write part to a markdown file
                with open_and_mkdirs(conversation_output_path) as f:
                    f.write(markdown)
                print(f'Wrote {len(chunk)} messages to {conversation_output_path}')
                num_written_files += 1

        else:
            markdown = ''
            markdown += f'### Conversation between {escaped_username} and {other_user_name}: ###\n\n'
            markdown += f'Time zone: {local_timezone}\n\n----\n\n'
            markdown += '\n\n----\n\n'.join(md for _, md in messages)
            conversation_output_path = paths.create_path_for_file_output_dms(
                name=other_user_short_name, output_format="md"
            )

            with open_and_mkdirs(conversation_output_path) as f:
                f.write(markdown)
            print(f'Wrote {len(messages)} messages to {conversation_output_path}')
            num_written_files += 1

        num_written_messages += len(messages)

    print(f"\nWrote {len(conversations_messages)} direct message conversations "
          f"({num_written_messages} total messages) to {num_written_files} markdown files\n")


def make_conversation_name_safe_for_filename(conversation_name: str) -> str:
    """
    Remove/replace characters that could be unsafe in filenames
    """
    forbidden_chars = \
        ['"', "'", '*', '/', '\\', ':', '<', '>', '?', '|', '!', '@', ';', ',', '=', '.', '\n', '\r', '\t']
    new_conversation_name = ''
    for char in conversation_name:
        if char in forbidden_chars:
            new_conversation_name = new_conversation_name + '_'
        elif char.isspace():
            # replace spaces with underscores
            new_conversation_name = new_conversation_name + '_'
        elif char == 0x7F or (0x1F >= ord(char) >= 0x00):
            # 0x00 - 0x1F and 0x7F are also forbidden, just discard them
            continue
        else:
            new_conversation_name = new_conversation_name + char

    return new_conversation_name


def find_group_dm_conversation_participant_ids(conversation: dict) -> set:
    """
    Find IDs of all participating Users in a group direct message conversation
    """
    group_user_ids = set()
    if 'dmConversation' in conversation and 'conversationId' in conversation['dmConversation']:
        dm_conversation = conversation['dmConversation']
        if 'messages' in dm_conversation:
            for message in dm_conversation['messages']:
                if 'messageCreate' in message:
                    group_user_ids.add(message['messageCreate']['senderId'])
                elif 'joinConversation' in message:
                    group_user_ids.add(message['joinConversation']['initiatingUserId'])
                    for participant_id in message['joinConversation']['participantsSnapshot']:
                        group_user_ids.add(participant_id)
                elif "participantsJoin" in message:
                    group_user_ids.add(message['participantsJoin']['initiatingUserId'])
                    for participant_id in message['participantsJoin']['userIds']:
                        group_user_ids.add(participant_id)
    return group_user_ids


def collect_user_ids_from_group_direct_messages(paths) -> list:
    """
     Collect all user ids that appear in the group direct messages archive data.
     (For use in bulk online lookup from Twitter.)
    """
    # read JSON file from archive
    group_dms_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'direct-messages-group.js'))
    # collect all user ids in a set
    group_dms_user_ids = set()
    for conversation in group_dms_json:
        participants = find_group_dm_conversation_participant_ids(conversation)
        for participant_id in participants:
            group_dms_user_ids.add(participant_id)
    return list(group_dms_user_ids)


def parse_group_direct_messages(
        username: str,
        users: dict,
        user_id_url_template: str,
        local_timezone: ZoneInfo,
        paths: PathConfig
):
    """Parse data_folder/direct-messages-group.js, write to one markdown file per conversation.
    """
    # read JSON file from archive
    group_dms_json = read_json_from_js_file(os.path.join(paths.dir_input_data, 'direct-messages-group.js'))

    print('')  # blank line for readability

    # Parse the group DMs, store messages and metadata in a dict
    group_conversations_messages = defaultdict(list)
    group_conversations_metadata = defaultdict(dict)
    for conversation in group_dms_json:
        if 'dmConversation' in conversation and 'conversationId' in conversation['dmConversation']:
            dm_conversation = conversation['dmConversation']
            conversation_id = dm_conversation['conversationId']
            participants = find_group_dm_conversation_participant_ids(conversation)
            participant_names = []
            for participant_id in participants:
                if participant_id in users:
                    participant_names.append(users[participant_id].handle)
                else:
                    participant_names.append(user_id_url_template.format(participant_id))

            # save names in metadata
            group_conversations_metadata[conversation_id]['participants'] = participants
            group_conversations_metadata[conversation_id]['participant_names'] = participant_names
            group_conversations_metadata[conversation_id]['conversation_names'] = [(0, conversation_id)]
            group_conversations_metadata[conversation_id]['participant_message_count'] = defaultdict(int)
            for participant_id in participants:
                # init every participant's message count with 0, so that users with no activity are not ignored
                group_conversations_metadata[conversation_id]['participant_message_count'][participant_id] = 0
            messages = []
            if 'messages' in dm_conversation:
                for message in dm_conversation['messages']:
                    if 'messageCreate' in message:
                        message_create = message['messageCreate']
                        if all(tag in message_create for tag in ['senderId', 'text', 'createdAt']):
                            from_id = message_create['senderId']
                            # count how many messages this user has sent to the group
                            group_conversations_metadata[conversation_id]['participant_message_count'][from_id] += 1
                            body = message_create['text']
                            # replace t.co URLs with their original versions
                            if 'urls' in message_create:
                                for url in message_create['urls']:
                                    if 'url' in url and 'expanded' in url:
                                        expanded_url = url['expanded']
                                        body = body.replace(url['url'], expanded_url)
                            # escape message body for markdown rendering:
                            body_markdown = escape_markdown(body)
                            # replace image URLs with image links to local files
                            if 'mediaUrls' in message_create \
                                    and len(message_create['mediaUrls']) == 1 \
                                    and 'urls' in message_create:
                                original_expanded_url = message_create['urls'][0]['expanded']
                                message_id = message_create['id']
                                media_hash_and_type = message_create['mediaUrls'][0].split('/')[-1]
                                media_id = message_create['mediaUrls'][0].split('/')[-2]
                                archive_media_filename = f'{message_id}-{media_hash_and_type}'
                                new_url = os.path.join(paths.dir_output_media, archive_media_filename)
                                archive_media_path = \
                                    os.path.join(paths.dir_input_data, 'direct_messages_group_media',
                                                 archive_media_filename)
                                if os.path.isfile(archive_media_path):
                                    # found a matching image, use this one
                                    if not os.path.isfile(new_url):
                                        shutil.copy(archive_media_path, new_url)
                                    image_markdown = f'\n![]({new_url})\n'
                                    body_markdown = body_markdown.replace(
                                        escape_markdown(original_expanded_url), image_markdown
                                    )

                                    # Save the online location of the best-quality version of this file,
                                    # for later upgrading if wanted
                                    best_quality_url = \
                                        f'https://ton.twitter.com/i//ton/data/dm/' \
                                        f'{message_id}/{media_id}/{media_hash_and_type}'
                                    # there is no ':orig' here, the url without any suffix has the original size

                                    # TODO: a cookie (and a 'Referer: https://twitter.com' header)
                                    #  is needed to retrieve it, so the url might be useless anyway...

                                    # WARNING: Do not uncomment the statement below until the cookie problem is solved!
                                    # media_sources.append(
                                    #     (
                                    #         os.path.join(output_media_folder_name, archive_media_filename),
                                    #         best_quality_url
                                    #     )
                                    # )

                                else:
                                    archive_media_paths = glob.glob(
                                        os.path.join(paths.dir_input_data, 'direct_messages_group_media',
                                                     message_id + '*'))
                                    if len(archive_media_paths) > 0:
                                        for archive_media_path in archive_media_paths:
                                            archive_media_filename = os.path.split(archive_media_path)[-1]
                                            media_url = os.path.join(paths.dir_output_media,
                                                                     archive_media_filename)
                                            if not os.path.isfile(media_url):
                                                shutil.copy(archive_media_path, media_url)
                                            video_markdown = f'\n<video controls><source src="{media_url}">' \
                                                             f'Your browser does not support the video tag.</video>\n'
                                            body_markdown = body_markdown.replace(
                                                escape_markdown(original_expanded_url), video_markdown
                                            )

                                    # TODO: maybe  also save the online location of the best-quality version for videos?
                                    #  (see above)

                                    else:
                                        print(f'Warning: missing local file: {archive_media_path}. '
                                              f'Using original link instead: {original_expanded_url})')

                            message_created_at = message_create['createdAt']  # example: 2022-01-27T15:58:52.744Z
                            created_at = datetime.datetime.strptime(
                                message_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 20019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)

                            from_handle = escape_markdown(users[from_id].handle) if from_id in users \
                                else user_id_url_template.format(from_id)
                            # make the body a quote
                            body_markdown = '> ' + '\n> '.join(body_markdown.splitlines())
                            message_markdown = f'{from_handle}: ({created_at_local_time_str})\n\n' \
                                               f'{body_markdown}'
                            messages.append((timestamp, message_markdown))
                    elif "conversationNameUpdate" in message:
                        conversation_name_update = message['conversationNameUpdate']
                        if all(tag in conversation_name_update for tag in ['initiatingUserId', 'name', 'createdAt']):
                            from_id = conversation_name_update['initiatingUserId']
                            body_markdown = f"_changed group name to: " \
                                            f"{escape_markdown(conversation_name_update['name'])}_"
                            update_created_at = conversation_name_update['createdAt']
                            # example: 2022-01-27T15:58:52.744Z
                            created_at = datetime.datetime.strptime(
                                update_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 20019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)

                            from_handle = escape_markdown(users[from_id].handle) if from_id in users \
                                else user_id_url_template.format(from_id)
                            message_markdown = f'{from_handle}: ({created_at_local_time_str})\n\n{body_markdown}'
                            messages.append((timestamp, message_markdown))
                            # save metadata about name change:
                            group_conversations_metadata[conversation_id]['conversation_names'].append(
                                (timestamp, conversation_name_update['name'])
                            )
                    elif "joinConversation" in message:
                        join_conversation = message['joinConversation']
                        if all(tag in join_conversation for tag in ['initiatingUserId', 'createdAt']):
                            from_id = join_conversation['initiatingUserId']
                            join_created_at = join_conversation['createdAt']  # example: 2022-01-27T15:58:52.744Z
                            created_at = datetime.datetime.strptime(
                                join_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 20019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)
                            from_handle = escape_markdown(users[from_id].handle) if from_id in users \
                                else user_id_url_template.format(from_id)
                            escaped_username = escape_markdown(username)
                            body_markdown = f'_{from_handle} added {escaped_username} to the group_'
                            message_markdown = f'{from_handle}: ({created_at_local_time_str})\n\n{body_markdown}'
                            messages.append((timestamp, message_markdown))
                    elif "participantsJoin" in message:
                        participants_join = message['participantsJoin']
                        if all(tag in participants_join for tag in ['initiatingUserId', 'userIds', 'createdAt']):
                            from_id = participants_join['initiatingUserId']
                            join_created_at = participants_join['createdAt']  # example: 2022-01-27T15:58:52.744Z
                            created_at = datetime.datetime.strptime(
                                join_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 20019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)
                            from_handle = escape_markdown(users[from_id].handle) if from_id in users \
                                else user_id_url_template.format(from_id)
                            joined_ids = participants_join['userIds']
                            joined_handles = [escape_markdown(users[joined_id].handle) if joined_id in users
                                              else user_id_url_template.format(joined_id) for joined_id in joined_ids]
                            name_list = ', '.join(joined_handles[:-1]) + \
                                        (f' and {joined_handles[-1]}' if len(joined_handles) > 1 else
                                         joined_handles[0])
                            body_markdown = f'_{from_handle} added {name_list} to the group_'
                            message_markdown = f'{from_handle}: ({created_at_local_time_str})\n\n{body_markdown}'
                            messages.append((timestamp, message_markdown))
                    elif "participantsLeave" in message:
                        participants_leave = message['participantsLeave']
                        if all(tag in participants_leave for tag in ['userIds', 'createdAt']):
                            leave_created_at = participants_leave['createdAt']  # example: 2022-01-27T15:58:52.744Z
                            created_at = datetime.datetime.strptime(
                                leave_created_at, '%Y-%m-%dT%X.%fZ').replace(tzinfo=datetime.timezone.utc)
                            timestamp = int(round(created_at.timestamp()))
                            nicer_date_format = '%b %d %Y, %H:%M'  # Mar 19 20019, 14:05
                            created_at_local_time = \
                                datetime.datetime.fromtimestamp(created_at.timestamp(), tz=local_timezone)
                            created_at_local_time_str = created_at_local_time.strftime(nicer_date_format)
                            left_ids = participants_leave['userIds']
                            left_handles = [escape_markdown(users[left_id].handle) if left_id in users
                                            else user_id_url_template.format(left_id) for left_id in left_ids]
                            name_list = ', '.join(left_handles[:-1]) + \
                                        (f' and {left_handles[-1]}' if len(left_handles) > 1 else
                                         left_handles[0])
                            body_markdown = f'_{name_list} left the group_'
                            message_markdown = f'{name_list}: ({created_at_local_time_str})\n\n{body_markdown}'
                            messages.append((timestamp, message_markdown))

            # collect messages per conversation in group_conversations_messages dict
            group_conversations_messages[conversation_id].extend(messages)

    # output as one file per conversation (or part of long conversation)
    num_written_messages = 0
    num_written_files = 0
    for conversation_id, messages in group_conversations_messages.items():
        # sort messages by timestamp
        messages.sort(key=lambda tup: tup[0])
        # create conversation name for use in filename:
        # first, try to find an official name in the parsed conversation data

        # Not-so-fun fact:
        # If the name was set before the archive's owner joined the group, the name is not included
        # in the archive data and can't be found anywhere (except by looking it up from twitter,
        # and that would probably need a cookie). So there are many groups that do actually have a name,
        # but it can't be used here because we don't know it.

        group_conversations_metadata[conversation_id]['conversation_names'].sort(key=lambda tup: tup[0], reverse=True)
        official_name = group_conversations_metadata[conversation_id]['conversation_names'][0][1]
        safe_group_name = make_conversation_name_safe_for_filename(official_name)
        if len(safe_group_name) < 2:
            # discard name if it's too short (because of collision risk)
            group_name = conversation_id
        else:
            group_name = safe_group_name

        if group_name == conversation_id:
            # try to make a nice list of participant handles for the conversation name
            handles = []
            for participant_id, message_count in \
                    group_conversations_metadata[conversation_id]['participant_message_count'].items():
                if participant_id in users:
                    participant_handle = users[participant_id].handle
                    if participant_handle != username:
                        handles.append((participant_handle, message_count))
            # sort alphabetically by handle first, for a more deterministic order
            handles.sort(key=lambda tup: tup[0])
            # sort so that the most active users are at the start of the list
            handles.sort(key=lambda tup: tup[1], reverse=True)
            if len(handles) == 1:
                group_name = \
                    f'{handles[0][0]}_and_{len(group_conversations_metadata[conversation_id]["participants"]) - 1}_more'
            elif len(handles) == 2 and len(group_conversations_metadata[conversation_id]["participants"]) == 3:
                group_name = f'{handles[0][0]}_and_{handles[1][0]}_and_{username}'
            elif len(handles) >= 2:
                group_name = \
                    f'{handles[0][0]}_and_{handles[1][0]}_and' \
                    f'_{len(group_conversations_metadata[conversation_id]["participants"]) - 2}_more'
            else:
                # just use the conversation id
                group_name = conversation_id

        # create a list of names of the form '@name1, @name2 and @name3'
        # to use as a headline in the output file
        escaped_participant_names = [
            escape_markdown(participant_name)
            for participant_name in group_conversations_metadata[conversation_id]['participant_names']
        ]
        name_list = ', '.join(escaped_participant_names[:-1]) + \
                    (f' and {escaped_participant_names[-1]}'
                     if len(escaped_participant_names) > 1
                     else escaped_participant_names[0])

        if len(messages) > 1000:
            for chunk_index, chunk in enumerate(chunks(messages, 1000)):
                markdown = ''
                markdown += f'## {official_name} ##\n\n'
                markdown += f'### Group conversation between {name_list}, part {chunk_index + 1}: ###\n\n'
                markdown += f'Time zone: {local_timezone}\n\n----\n\n'
                markdown += '\n\n----\n\n'.join(md for _, md in chunk)
                conversation_output_filename = paths.create_path_for_file_output_dms(
                    name=group_name, output_format="md", kind="DMs-Group", index=chunk_index + 1
                )
                
                # write part to a markdown file
                with open_and_mkdirs(conversation_output_filename) as f:
                    f.write(markdown)
                print(f'Wrote {len(chunk)} messages to {conversation_output_filename}')
                num_written_files += 1
        else:
            markdown = ''
            markdown += f'## {official_name} ##\n\n'
            markdown += f'### Group conversation between {name_list}: ###\n\n'
            markdown += f'Time zone: {local_timezone}\n\n----\n\n'
            markdown += '\n\n----\n\n'.join(md for _, md in messages)
            conversation_output_filename = \
                paths.create_path_for_file_output_dms(name=group_name, output_format="md", kind="DMs-Group")

            with open_and_mkdirs(conversation_output_filename) as f:
                f.write(markdown)
            print(f'Wrote {len(messages)} messages to {conversation_output_filename}')
            num_written_files += 1

        num_written_messages += len(messages)

    print(f"\nWrote {len(group_conversations_messages)} direct message group conversations "
          f"({num_written_messages} total messages) to {num_written_files} markdown files")


def migrate_old_output(paths: PathConfig):
    """If present, moves media and cache files from the archive root to the new locations in 
    `paths.dir_output_media` and `paths.dir_output_cache`. Then deletes old output files 
    (md, html, txt) from the archive root, if the user consents."""

    # Create new folders, so we can potentially use them to move files there
    os.makedirs(paths.dir_output_media, exist_ok=True)
    os.makedirs(paths.dir_output_cache, exist_ok=True)

    # Move files that we can re-use:
    if os.path.exists(os.path.join(paths.dir_archive, "media")):
        files_to_move = glob.glob(os.path.join(paths.dir_archive, "media", "*"))
        if len(files_to_move) > 0:
            print(f"Moving {len(files_to_move)} files from 'media' to '{paths.dir_output_media}'")
            for file_path_to_move in files_to_move:
                file_name_to_move = os.path.split(file_path_to_move)[1]
                os.rename(file_path_to_move, os.path.join(paths.dir_output_media, file_name_to_move))
        os.rmdir(os.path.join(paths.dir_archive, "media"))

    known_tweets_old_path = os.path.join(paths.dir_archive, "known_tweets.json")
    known_tweets_new_path = os.path.join(paths.dir_output_cache, "known_tweets.json")
    if os.path.exists(known_tweets_old_path):
        os.rename(known_tweets_old_path, known_tweets_new_path)

    # Delete files that would be overwritten anyway (if user consents):
    output_globs = [
        "TweetArchive.html",
        "*Tweet-Archive*.html",
        "*Tweet-Archive*.md",
        "DMs-Archive-*.html",
        "DMs-Archive-*.md",
        "DMs-Group-Archive-*.html",
        "DMs-Group-Archive-*.md",
        "followers.txt",
        "following.txt",
    ]
    files_to_delete = []
    
    for output_glob in output_globs:
        files_to_delete += glob.glob(os.path.join(paths.dir_archive, output_glob))
        
    # TODO maybe remove those files only after the new ones have been generated? This way, the user would never
    # end up with less output than before. On the other hand, they might end up with old *and* new versions
    # of the output, if the script crashes before it reaches the code to delete the old version.
    if len(files_to_delete) > 0:
        print(f"\nThere are {len(files_to_delete)} files in the root of the archive,")
        print("which were probably generated from an older version of this script.")
        print("Since then, the directory layout of twitter-archive-parser has changed")
        print("and these files are generated into the sub-directory 'parser-output' or")
        print("various sub-sub-directories therein. These are the affected files:\n")

        for file_to_delete in files_to_delete:
            print(file_to_delete)

        print()
        if get_consent('OK to delete these files? (If the the directory layout would not have changed, '
                       'they would be overwritten anyway)', key='delete_old_files'):
            for file_to_delete in files_to_delete:
                os.remove(file_to_delete)
            print(f"Files have been deleted. New versions of these files will be generated into 'parser-output' soon.")


def export_user_data(users: dict, extended_user_data: dict, paths: PathConfig):
    """
    save users dict and extended user data to JSON files
    """
    users_dicts: list[dict] = [user_data.to_dict() for user_data in users.values()]
    users_dicts.sort(key=lambda u: int(u['user_id']))
    users_json: str = json.dumps(users_dicts, indent=2)
    with open(os.path.join(paths.dir_output_cache, 'user_data_cache.json'), 'w') as users_file:
        print(f'saving {len(users_dicts)} sets of user data to user_data_cache.json ...')
        users_file.write(users_json)
        print('user data saved.\n')

    extended_users_json: str = json.dumps(extended_user_data, indent=2)
    with open(os.path.join(paths.dir_output_cache, 'extended_user_data_cache.json'), 'w') as extended_users_file:
        print(f'saving {len(extended_user_data.keys())} '
              f'sets of extended user data to extended_user_data_cache.json ...')
        extended_users_file.write(extended_users_json)
        print('extended user data saved.\n')


def is_archive(path):
    """Return true if there is a Twitter archive at the given path"""
    return os.path.isfile(os.path.join(path, 'data', 'account.js'))


def find_archive():
    """
    Search for the archive
    1. First try the working directory.
    2. Then try the script directory.
    3. Finally, prompt the user.
    """
    if is_archive('.'):
        return '.'
    script_dir = os.path.dirname(__file__)
    if script_dir != os.getcwd():
        if is_archive(script_dir):
            return script_dir
    print('Archive not found in working directory or script directory.\n'
          'Please enter the path of your Twitter archive, or just press Enter to exit.\n'
          'On most operating systems, you can also try to drag and drop your archive folder '
          'into the terminal window, and it will paste its path automatically.\n')
    # Give the user as many attempts as they need.
    while True:
        input_path = input('Archive path: ')
        if not input_path:
            exit()
        if is_archive(input_path):
            return input_path
        print(f'Archive not found at {input_path}')


def read_users_from_cache(paths: PathConfig) -> dict:
    """
    try to read user_id -> handle mapping from user_data_cache.json
    """
    # return empty dict if there is no cache file yet
    if not os.path.exists(os.path.join(paths.dir_output_cache, 'user_data_cache.json')):
        return {}

    # else, read data from cache file
    users_dict: dict = {}
    user_list: list = read_json_from_js_file(os.path.join(paths.dir_output_cache, 'user_data_cache.json'))
    print(f'importing {len(user_list)} user handles from user_data_cache.json ...')
    if len(user_list) > 0:
        for user_dict in user_list:
            users_dict[user_dict['user_id']] = UserData(
                user_id=user_dict['user_id'],
                handle=user_dict['handle'],
            )
    print(f'imported {len(users_dict.keys())} user handles.')
    return users_dict


def read_extended_user_data_from_cache(paths: PathConfig) -> dict:
    """
    try to read extended user data from extended_user_data_cache.json
    """
    # return empty dict if there is no cache file yet
    if not os.path.exists(os.path.join(paths.dir_output_cache, 'extended_user_data_cache.json')):
        return {}

    # else, read data from cache file
    with open(os.path.join(paths.dir_output_cache, 'extended_user_data_cache.json'), 'r', encoding='utf8') as f:
        data: list = f.readlines()
        if len(data) <= 1:
            return {}
        print(f'importing extended user data from extended_user_data_cache.json ...')
        joined_data: str = ''.join(data)
        extended_users_dict: dict = json.loads(joined_data)
        print(f'imported {len(extended_users_dict.keys())} sets of extended user data.')
        return extended_users_dict


def download_user_images(
        extended_user_data: dict[str, dict],
        paths: PathConfig,
        media_download_state: dict,
        error_codes_to_exclude: list[str],
) -> None:
    # Change suffix to choose another image size:
    # URL suffix  -> image width and height
    # _normal.ext -> 48
    # _x96.ext    -> 96
    # .ext        -> original or 400 ?
    size_suffix = "_x96"

    to_download: dict[str, str] = {}
    for user in extended_user_data.values():
        if 'profile_image_url_https' in user.keys() and user['profile_image_url_https'] is not None and len(user['profile_image_url_https']) > 0:
            profile_image_url_https = user['profile_image_url_https'].replace("_normal", size_suffix)
            file_extension = os.path.splitext(profile_image_url_https)[1]
            profile_image_file_name = user["id_str"] + file_extension
            profile_image_file_path = os.path.join(paths.dir_output_media, "profile-images", profile_image_file_name)
            user['profile_image_local_path'] = profile_image_file_path
            if not os.path.exists(profile_image_file_path):
                mkdirs_for_file(profile_image_file_path)
                to_download[profile_image_file_path] = profile_image_url_https

    estimated_download_time_str = format_duration(len(to_download) * 0.53)
    estimated_download_size_str = int(len(to_download) * 4.3)

    if len(to_download) > 0:
        if get_consent(f'OK to start downloading up to {len(to_download)} user profile images '
                       f'(approx {estimated_download_size_str:,} KB)? '
                       f'This could take about {estimated_download_time_str}, or less if some '
                       f'of the files are already downloaded before.', key='download_profile_images'):
            download_larger_media(to_download, error_codes_to_exclude, paths, media_download_state)


def export_media_download_state(media_download_state: dict, paths: PathConfig):
    with open(paths.file_media_download_state, 'w') as state_file:
        json.dump(media_download_state, state_file, sort_keys=True, indent=4)


def main():
    p = ArgumentParser(
        description="Parse a Twitter archive and output in various ways"
    )
    p.add_argument("--archive-folder", dest="archive_folder", type=str, default=None,
                   help="path to the twitter archive folder")
    args = p.parse_args()

    # use input folder from cli args if given
    if args.archive_folder and os.path.isdir(args.archive_folder):
        input_folder = args.archive_folder
    else:
        input_folder = find_archive()
        
    paths = PathConfig(dir_archive=input_folder)

    print(f"\n\nWorking on archive: {os.path.abspath(paths.dir_archive)}")
    paths = PathConfig(dir_archive=input_folder)

    # Extract the archive owner's identity from data/account.js
    own_user_data = extract_user_data(paths)
    username = own_user_data.handle

    user_id_url_template = 'https://twitter.com/i/user/{}'

    html_template = {
        "begin": """\
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet"
          href="https://unpkg.com/@picocss/pico@latest/css/pico.min.css">
    <title>Your Twitter archive!</title>
    <style>
        .tweet-pre-header {
            color: lightgrey;
            font-size: 70%;
            margin-bottom: 4px;
        }

        .tweet-header {
            position: relative;
            margin-bottom: 8px;
            margin-left: 56px;
        }

        .upper-line {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
        }

        .lower-line {
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
        }

        .profile-picture {
            flex-grow: 0;
            position: absolute;
            left: -56px;
        }

        .profile-picture img {
            border-radius: 50%;
        }

        .user-name {
            font-weight: 700;
            flex-basis: 80%;
            margin-top: -4px;
        }

        .user-handle {
            color: grey;
            font-size: 70%;
            flex-shrink: 0;
            flex-grow: 0;
        }

       .tweet-timestamp {
            color: grey;
            font-size: 70%;
            flex-shrink: 0;
            flex-grow: 0;
        }

       .retweet-timestamp {
            color: grey;
            font-size: 70%;
            flex-shrink: 0;
            flex-grow: 0;
        }

        .quote-tweet {
            margin-top: 12px;
            border: 1px solid var(--muted-border-color);
            border-radius: 12px;
            padding: 24px;
        }
    </style>
</head>
<body>
    <main class="container">""",
        "end": """    </main>
</body>
</html>""",
    }

    users = read_users_from_cache(paths)
    extended_user_data = read_extended_user_data_from_cache(paths)

    # Use our state store to prevent duplicate downloads
    try:
        with open(paths.file_media_download_state, 'r') as state_file:
            media_download_state = json.load(state_file)
    except (IOError, json.decoder.JSONDecodeError):
        media_download_state = {}

    migrate_old_output(paths)

    # Make a folder to copy the images and videos into.
    os.makedirs(paths.dir_output_media, exist_ok=True)
    if not os.path.isfile(paths.file_tweet_icon):
        shutil.copy(os.path.join(paths.dir_archive, 'assets/images/favicon.ico'), paths.file_tweet_icon)

    # Read tweets from paths.files_input_tweets, write to *.md and *.html.
    # Copy the media used to paths.dir_output_media.
    # Collect user_id:user_handle mappings for later use, in 'users'.
    # Returns the mapping from media filename to best-quality URL.
    tweets = load_tweets(paths)
    tweet_ids_to_download = collect_tweet_ids_from_tweets(tweets)
    download_tweets(tweets, tweet_ids_to_download, paths)

    print('')  # blank line for readability

    user_ids_from_tweets = collect_user_ids_from_tweets(tweets)
    # Make sure to include the owner's user id in the list to look up metadata for:
    # This is mostly useful for accounts who don't ever appear as mentioned or retweeted in their own tweets
    # (for example bots posting generative content and not interacting with others).
    user_ids_from_tweets.append(own_user_data.user_id)
    print(f'found {len(user_ids_from_tweets)} user IDs in tweets.')
    following_ids = collect_user_ids_from_followings(paths)
    print(f'found {len(following_ids)} user IDs in followings.')
    follower_ids = collect_user_ids_from_followers(paths)
    print(f'found {len(follower_ids)} user IDs in followers.')
    dms_user_ids = collect_user_ids_from_direct_messages(paths)
    print(f'found {len(dms_user_ids)} user IDs in direct messages.')
    group_dms_user_ids = collect_user_ids_from_group_direct_messages(paths)
    print(f'found {len(group_dms_user_ids)} user IDs in group direct messages.')

    # bulk lookup for user handles from followers, followings, direct messages and group direct messages
    collected_user_ids_without_followers = list(
        set(following_ids).union(set(dms_user_ids)).union(set(group_dms_user_ids))
    )
    collected_user_ids_only_in_followers: set = set(follower_ids).difference(set(collected_user_ids_without_followers))
    collected_user_ids: list = list(set(collected_user_ids_without_followers)
                                    .union(collected_user_ids_only_in_followers))

    print(f'\nfound {len(collected_user_ids)} user IDs overall.')

    # give the user a choice if followers should be included in the lookup
    # (but only in case they make up a large amount):
    unknown_collected_user_ids: set = set(collected_user_ids).difference(users.keys())
    unknown_follower_user_ids: set = unknown_collected_user_ids.intersection(collected_user_ids_only_in_followers)
    if len(unknown_follower_user_ids) > 5000:
        # Account metadata observed at ~2.1KB on average.
        estimated_follower_lookup_size = int(2.1 * len(unknown_follower_user_ids))
        # we can look up at least 3000 users per minute.
        estimated_max_follower_lookup_time_in_minutes = len(unknown_follower_user_ids) / 3000
        print(
            f'For some user IDs, the @handle is not included in the archive data. '
            f'Unknown user handles can be looked up online.'
            f'{len(unknown_follower_user_ids)} of {len(unknown_collected_user_ids)} total '
            f'user IDs with unknown handles are from your followers. Online lookup would be '
            f'about {estimated_follower_lookup_size:,} KB smaller and up to '
            f'{estimated_max_follower_lookup_time_in_minutes:.1f} minutes faster without them.\n'
        )

        if not get_consent(f'Do you want to include handles of your followers '
                           f'in the online lookup of user handles anyway?',
                           default_to_yes=True,
                           key='lookup_followers'
                           ):
            collected_user_ids = collected_user_ids_without_followers

    extended_user_ids: list = list(set(collected_user_ids).union(set(user_ids_from_tweets)))
    print(f'found {len(extended_user_ids)} user IDs overall, including from tweets.')
    if get_consent(f'Do you want to include users from tweets in the user data download?',
                   key='lookup_tweet_users'
                   ):
        lookup_users(extended_user_ids, users, extended_user_data)
    else:
        lookup_users(collected_user_ids, users, extended_user_data)

    export_user_data(users, extended_user_data, paths)

    # media downloads that returned with these errors before will not be retried
    error_codes_to_exclude = ["status 403", "status 404"]

    download_user_images(extended_user_data, paths, media_download_state, error_codes_to_exclude)

    print('')  # blank line for readability

    parse_followings(users, user_id_url_template, paths)
    parse_followers(users, user_id_url_template, paths)

    print('')  # blank line for readability

    # get local timezone info:
    tzlocal = import_module('tzlocal')
    local_timezone: ZoneInfo = tzlocal.get_localzone()

    parse_direct_messages(username, users, user_id_url_template, local_timezone, paths)
    parse_group_direct_messages(username, users, user_id_url_template, local_timezone, paths)

    print('')  # blank line for readability

    # TODO Maybe this should be split up, so that downloaded media can be used during convert?
    # On the other hand, media in own tweets will be replaced by better versions, and
    # for media from other users' tweets, we can try to use it, since we know the local path
    # even before it is actually downloaded.

    # media_sources = collect_media_sources_from_tweets(...)
    # download_larger_media(...)
    # convert_tweets(...)
    media_sources = convert_tweets(
        own_user_data, users, extended_user_data, html_template, tweets, local_timezone, paths
    )

    # remove media that are already known to have the best quality from the list of media to download.
    # also remove media that are already known to be unavailable.
    new_media_sources = {}
    for filename, online_url in media_sources.items():
        if online_url not in media_download_state.keys() or \
                (media_download_state[online_url]["success"] is not True and
                 media_download_state[online_url]["error"] not in error_codes_to_exclude):

            new_media_sources[filename] = online_url

    media_sources = new_media_sources

    # Download larger images and additional media, if the user agrees
    if len(media_sources) > 0:
        print(f"\nThe archive doesn't contain the original size version of some images from your own tweets.")
        print(f"It also doesn't contain images from retweets, as well as animated gifs and some videos.")
        print(f"We can attempt to download them from twimg.com.")
        print(f'Please be aware that this script may download a lot of data, which will cost you money if you are')
        print(f'paying for bandwidth. Please be aware that the servers might block these requests if they are too')
        print(f'frequent. This script may not work if your account is protected. You may want to set it to public')
        print(f'before starting the download.\n')

        estimated_download_time_str = format_duration(len(media_sources) * 0.4)

        if get_consent(f'OK to start downloading {len(media_sources)} media files? '
                       f'This would take at least {estimated_download_time_str}.', key='download_media'):

            download_larger_media(media_sources, error_codes_to_exclude, paths, media_download_state)
            print('In case you set your account to public before initiating the download, '
                  'do not forget to protect it again.')

    export_media_download_state(media_download_state, paths)


if __name__ == "__main__":
    main()
