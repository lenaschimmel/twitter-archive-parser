#!/usr/bin/env python3
"""
    Copyright (C) 2022
    twitter-archive-parser by Tim Hutton

    contribution by flauschzelle (https://github.com/flauschzelle)

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

import dataclasses
import datetime
import json
import os
import time
from dataclasses import dataclass
from typing import Optional

import requests

# FIXME copied the method here to prevent circular import
# Later everything will be in one file again, so we need neither copied methods nor imports
def read_json_from_js_file(filename):
    """Reads the contents of a Twitter-produced .js file into a dictionary."""
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


@dataclass
class UserData:
    id: str
    link: Optional[str] = None
    handle: Optional[str] = None
    display_name: Optional[str] = None  # only used in some of the datasets
    # TODO: find a source to look up the other display names
    bio: Optional[str] = None  # for later implementation, needs a different source to look up
    avatar_path: Optional[str] = None  # for later implementation with downloaded profile pics

    def to_dict(self):
        return dataclasses.asdict(self)


class UserIdParser:
    """
    A tool to look for user IDs in a twitter archive and find matching handles
    (also finds links and display names for some users).

    Hopefully, the locally saved output of this can later be used for making the archived direct messages
    (which only contain user IDs but no handles or display names) more human-readable, while not depending on
    Twitter still being online.

    To use this, run 'python3 [path_to/]user_id_parser.py' inside the root folder of your unzipped twitter archive.

    On the first run, the parser looks for user IDs in your followings, followers, mentions, and direct messages.
    Some of them can be matched with a handle and display name directly from your archive.

    After that, it tries to get matching handles for any IDs that are still missing them,
    by looking up the ID via the web service 'tweeterid.com'.

    If the service is not reachable, or if it doesn't find all names in the first run,
    it will ask if you want to try again.
    If you don't answer with 'y' (or if it finds everything in the first run),
    it will save the current results to a JSON file.

    tweeterid.com seems to be somewhat unstable and will often return errors even for existing IDs,
    so trying again several times is strongly recommended!

    If you run the parser in the same place again later, it will read the results from the last output file
    and try to continue from there. Each run will create a new output file.
    If you want to save space, you can delete the older ones manually.

    The JSON output file contains two lists:
        - User IDs that could not be matched with a handle ('users_without_name')
        - User IDs that were successfully matched with a handle ('users_with_name')

    Data sets in both lists may additionally contain a display name and/or a link to the user's twitter page.

    They also have fields for a bio (description from the user profile) and a path to the avatar image,
    but retrieving them is not implemented yet.
    """

    def __init__(self):
        # blank dictionary for storing parsed user data
        self.users: dict = {}
        # blank list for storing unmatched user IDs
        self.user_ids_without_name: list = []

    @staticmethod
    def lookup_name_from_tweeterid(user_id: str) -> str:
        data: dict = {'input': user_id}
        response = requests.post(
            url='https://tweeterid.com/ajax.php',
            data=data,
        )
        return response.text

    def parse_user_ids_from_archive(self, tweet_input_filenames):

        # TODO I believe that the files for followings and followers have the same structure,
        # so that a single method could be called twice to parse both
        # parse followings
        print('parsing following.js ...')
        followings_json = read_json_from_js_file('data/following.js')
        # extract account ids
        for following_dict in followings_json:
            new_user_id: str = following_dict['following']['accountId']
            new_user = UserData(
                id=new_user_id,
                link=following_dict['following']['userLink']
            )
            self.users[new_user_id] = new_user

        print(f'found {len(self.users)} user ids in following.js\n')

        # parse followers
        print('parsing follower.js ...')
        followers_json = read_json_from_js_file('data/follower.js')
        count_new: int = 0
        for follower_dict in followers_json:
            new_user_id: str = follower_dict['follower']['accountId']
            if new_user_id not in self.users.keys():
                new_user = UserData(
                    id=new_user_id,
                    link=follower_dict['follower']['userLink']
                )
                self.users[new_user_id] = new_user
                count_new += 1

        print(f'found {count_new} more users ids in follower.js\n')

        # parse mentions in tweets
        print('parsing tweets...')
        count_new: int = 0
        count_name_added: int = 0
        count_skip_duplicates: int = 0
        for tweet_js_filename in tweet_input_filenames:
            tweets_json = read_json_from_js_file(tweet_js_filename)
            # extract account ids
            for tweet_dict in tweets_json:
                tweet = tweet_dict['tweet']
                if 'user_mentions' in tweet['entities'].keys():
                    for mention in tweet['entities']['user_mentions']:
                        new_user_id: str = mention["id"]
                        new_user_display_name: str = mention["name"]
                        new_user_handle: str = mention["screen_name"]
                        if new_user_id in self.users.keys() and self.users[new_user_id].handle is None:
                            self.users[new_user_id].display_name = new_user_display_name
                            self.users[new_user_id].handle = new_user_handle
                            count_name_added += 1
                        elif new_user_id not in self.users.keys():
                            self.users[new_user_id] = UserData(
                                id=new_user_id,
                                display_name=new_user_display_name,
                                handle=new_user_handle
                            )
                            count_new += 1
                        else:
                            count_skip_duplicates += 1

        print(f'found {count_new} new users from mentions.')
        print(f'also added names to {count_name_added} existing users from mentions, '
              f'skipped {count_skip_duplicates} duplicates.\n')

        # parse dms:
        print('parsing direct-messages.js ...')
        dms_json = read_json_from_js_file('data/direct-messages.js')
        # extract account ids
        count_new: int = 0
        for dm_dict in dms_json:
            conversation_dict: dict = dm_dict['dmConversation']
            first_message: dict = conversation_dict['messages'][0]
            if 'messageCreate' not in first_message.keys():
                for message in conversation_dict['messages']:
                    if 'messageCreate' in message.keys():
                        first_message = message
                        break
                else:
                    continue
            recipient_id: str = first_message['messageCreate']['recipientId']
            sender_id: str = first_message['messageCreate']['senderId']

            if recipient_id not in self.users.keys():
                self.users[recipient_id] = UserData(
                    id=recipient_id,
                )
                count_new += 1
            if sender_id not in self.users.keys():
                self.users[sender_id] = UserData(
                    id=sender_id,
                )
                count_new += 1

        print(f'found {count_new} more users ids in direct messages.\n')

        # parse group dms:
        print('parsing direct-messages-group.js ...')
        dms_json = read_json_from_js_file('data/direct-messages-group.js')
        # extract account ids
        count_new: int = 0
        for dm_dict in dms_json:
            conversation_dict: dict = dm_dict['dmConversation']
            for message in conversation_dict['messages']:
                if 'messageCreate' in message.keys():
                    sender_id: str = message['messageCreate']['senderId']

                    if sender_id not in self.users.keys():
                        self.users[sender_id] = UserData(
                            id=sender_id,
                        )
                        count_new += 1

        print(f'found {count_new} more users ids in group direct messages.\n')

    def users_with_handles(self) -> list:
        named_users: list = []
        for user in self.users.values():
            if user.handle is not None:
                named_users.append(user)
        return named_users

    def users_missing_handles(self) -> list:
        unnamed_users: list = []
        for user in self.users.values():
            if user.handle is None:
                unnamed_users.append(user)
        return unnamed_users

    def parse_users_from_json_file(self, json_input: str):
        user_lists: dict = json.loads(json_input)
        if 'users_with_name' in user_lists.keys():
            for user in user_lists['users_with_name']:
                self.users[user['id']] = UserData(**user)
        if 'users_without_name' in user_lists.keys():
            for user in user_lists['users_without_name']:
                self.users[user['id']] = UserData(**user)

    def write_results_to_json_file(self, filename):
        # write results into a JSON file:
        with open(filename, 'w', encoding='utf-8') as ids_file:
            unnamed_users_list = [user.to_dict() for user in self.users_missing_handles()]
            unnamed_users_list.sort(key=lambda u: int(u['id']))

            named_users_list = [user.to_dict() for user in self.users_with_handles()]
            named_users_list.sort(key=lambda u: int(u['id']))

            users_json = json.dumps(
                {
                   'users_without_name': unnamed_users_list,
                   'users_with_name': named_users_list,
                 },
                indent=2,
            )

            ids_file.write(users_json)

    def look_for_missing_usernames(self):

        try_reaching_tweeterid: bool = True
        while try_reaching_tweeterid:
            print('checking if tweeterid.com is functional ...')
            # this is the id of '@twitter', which should definitely be valid:
            result = self.lookup_name_from_tweeterid('783214')
            if result == 'error':
                time.sleep(1.1)  # wait to avoid potential rate limiting
                answer = input('tweeterid.com seems to be having problems. Try again [y/n]?')
                if answer != 'y':
                    try_reaching_tweeterid = False
            else:
                # try was successful, go on :)
                break
        if not try_reaching_tweeterid:
            # input said stop trying
            return
        else:
            continue_looking: bool = True
            while continue_looking:
                print('looking up missing data from tweeterid.com...')

                lookup_limit: int = len(self.user_ids_without_name)
                looked_up: int = 0
                error_count: int = 0
                success_count: int = 0
                found_names_for_user_ids: list = []

                for user_id in self.user_ids_without_name:
                    if looked_up >= lookup_limit:
                        break
                    print(f'{looked_up+1}/{lookup_limit}: looking up user id {user_id} ...')
                    result = self.lookup_name_from_tweeterid(user_id)
                    looked_up += 1
                    if result == 'error':
                        error_count += 1
                    else:
                        print(result)
                        success_count += 1
                        # save handle without @
                        self.users[user_id].handle = result[1:]
                        found_names_for_user_ids.append(user_id)

                    # wait to avoid potential rate limit problems
                    time.sleep(1.1)

                # remove found ids from list:
                for user_id in found_names_for_user_ids:
                    self.user_ids_without_name.remove(user_id)

                print(f'looked up {looked_up} user ids: {success_count} successful, {error_count} errors.')

                if len(self.user_ids_without_name) == 0:
                    continue_looking = False
                else:
                    answer = input(f'retry looking up name for {len(self.user_ids_without_name)} user ids [y/n]?')
                    if answer != 'y':
                        continue_looking = False

    def run_parser(self):

        # look for newest json output file:
        files_in_dir = os.listdir(path='.')
        user_files: list = []
        for filename in files_in_dir:
            if filename.startswith('parsed_users_'):
                user_files.append(filename)

        if len(user_files) > 0:
            # sort so the newest is at the top:
            user_files.sort(reverse=True)
            newest_filename = user_files[0]
            with open(newest_filename, 'r') as past_file:
                # read from past file:
                self.parse_users_from_json_file(past_file.read())

            print(f'found user data in file {newest_filename}:')
            print(f'{len(self.users_with_handles())} users with handles.')
            print(f'{len(self.users_missing_handles())} users without handles.')

        else:
            self.parse_user_ids_from_archive(tweet_input_filenames=['data/tweets.js'])

            print('parsed archive. summary:')
            print(f'{len(self.users)} user ids collected,')
            print(f'including {len(self.users_missing_handles())} user ids without a handle.')

        if len(self.users_missing_handles()) > 0:
            # make a list of user ids to look up:
            self.user_ids_without_name = [user.to_dict()['id'] for user in self.users_missing_handles()]
            self.user_ids_without_name.sort(key=lambda u: int(u))
            # look them up (with tweeterid):
            self.look_for_missing_usernames()

        # finally, write results to a new file:
        now: str = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        filename = f'parsed_users_{now}.json'
        self.write_results_to_json_file(filename)


if __name__ == "__main__":
    user_id_parser = UserIdParser()
    user_id_parser.run_parser()
