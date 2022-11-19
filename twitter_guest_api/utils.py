"""Utilities for downloading from Twitter"""

import json
import logging
import os
import re
import urllib.parse

GUEST_TOKEN_ENDPOINT = "https://api.twitter.com/1.1/guest/activate.json"
STATUS_ENDPOINT = "https://twitter.com/i/api/graphql/"

MISSING_VARIABLE_PATTERN = re.compile("Query violation: Variable '([^']+)'")
MISSING_FEATURE_PATTERN = re.compile("The following features cannot be null: ([a-zA-Z_0-9, ]+)")
# e.g. '{queryId:"BoHLKeBvibdYDiJON1oqTg",operationName:"TweetDetail",operationType:"query",...'
QUERY_ID_PATTERN = re.compile(r'{queryId:"(\w+)",operationName:"(\w+)"')
BEARER_TOKEN_PATTERN = re.compile(r'"(AAA\w+%\w+)"')

RETRY_COUNT = 10

def send_request(url, session_method, headers):
    """Attempt an http request"""
    response = session_method(url, headers=headers, stream=True)
    if response.status_code != 200:
        raise Exception(f"Failed request to {url}: {response.status_code} {response.reason}")

    result = [line.decode("utf-8") for line in response.iter_lines()]
    return "".join(result)

def get_guest_token(session, headers):
    """Request a guest token and add it to the headers"""
    guest_token_resp = send_request(GUEST_TOKEN_ENDPOINT, session.post, headers)
    guest_token = json.loads(guest_token_resp)['guest_token']
    if not guest_token:
        raise Exception(f"Failed to retrieve guest token")
    logging.info("Retrieved guest token %s", guest_token)
    headers['x-guest-token'] = guest_token

def details_filename(query_name):
    """Get the path to the json file containing request details"""
    # find the folder that __file__ is in
    folder = os.path.dirname(__file__)
    return os.path.join(folder, f"{query_name}Request.json")

def read_request_details(query_name):
    """Read details from the json filename"""
    # load json from the file
    with open(details_filename(query_name), "r") as details_file:
        request_details = json.load(details_file)
    return request_details

def write_request_details(query_name, json_features, json_variables):
    """Write details to the json filename"""
    # save the updated variables and features
    with open(details_filename(query_name), "w") as details_file:
        json.dump({"features": json_features, "variables": json_variables}, details_file, indent=4)

def make_params(query_name, json_variables, json_features):
    """Build the query string given variables and features"""
    features = urllib.parse.quote_plus(json.dumps(json_features, separators=(',', ':')))
    variables = urllib.parse.quote_plus(json.dumps(json_variables, separators=(',', ':')))
    return f"{query_name}?variables={variables}&features={features}"

def exploratory_request(url, session, headers, query_name, id_name, tweet_id):
    """
    Attempt an http request. If it fails, add the missing variables and features and retry.
    Update the details json with the missing items.
    """
    request_details = read_request_details(query_name)

    json_features = request_details["features"]
    json_variables = request_details["variables"]

    json_variables[id_name] = str(tweet_id)
    status_params = make_params(query_name, json_variables, json_features)

    response = session.get(url + status_params, headers=headers)
    result = "".join([line.decode("utf-8") for line in response.iter_lines()])

    if response.status_code == 200:
        return result

    if response.status_code == 429:
        # rate limit exceeded?
        logging.warning("Error %i: %s", response.status_code, response.text.strip())
        logging.info("Trying new guest token")
        get_guest_token(session, headers)
        response = session.get(url + status_params, headers=headers)
        if response.status_code == 200:
            result = "".join([line.decode("utf-8") for line in response.iter_lines()])
            return result

    for _ in range(RETRY_COUNT):
        missing_variables = MISSING_VARIABLE_PATTERN.findall(result)
        missing_features = MISSING_FEATURE_PATTERN.findall(result)

        if missing_features:
            missing_features = missing_features[0].split(", ")

        if missing_variables or missing_features:
            for variable in missing_variables:
                json_variables[variable] = False

            for feature in missing_features:
                json_features[feature] = False

            status_params = make_params(query_name, json_variables, json_features)

            response = session.get(url + status_params, headers=headers)
            result = "".join([line.decode("utf-8") for line in response.iter_lines()])

            # If response works, then it means the variables or features we added are good.
            if response.status_code == 200:
                del json_variables[id_name]
                write_request_details(query_name, json_features, json_variables)

                # It worked - no need for additional retries.
                print(f"Success on retry {_}")
                break

    return result

def initialise_headers(session, url):
    """Populate http headers with necessary information for Twitter queries"""
    headers = {}
    query_ids = {}

    # One of the js files from original url holds the bearer token and query id.
    container = send_request(url, session.get, headers)
    js_files = re.findall("src=['\"]([^'\"()]*js)['\"]", container)

    bearer_token = None
    # Search the javascript files for a bearer token and query ids
    for jsfile in js_files:
        logging.debug("Processing %s", jsfile)
        file_content = send_request(jsfile, session.get, headers)
        find_bearer_token = BEARER_TOKEN_PATTERN.search(file_content)

        for find_query_id in QUERY_ID_PATTERN.finditer(file_content):
            query_id = find_query_id.group(1)
            query_name = find_query_id.group(2)
            logging.debug("Retrieved query id %s for query %s", query_id, query_name)
            query_ids[query_name] = query_id

        if find_bearer_token:
            bearer_token = find_bearer_token.group(1)
            logging.info("Retrieved bearer token: %s", bearer_token)

        if bearer_token and query_ids:
            break

    if not bearer_token:
        raise Exception("Did not find bearer token.")
    if not query_ids:
        raise Exception("Did not find query ids.")

    headers['authorization'] = f"Bearer {bearer_token}"

    get_guest_token(session, headers)
    return headers, query_ids

def parse_tweet(tweet_entry):
    """Parse the metadata for a tweet"""
    item_content = tweet_entry["content"]["itemContent"]
    result = item_content["tweet_results"]["result"]
    typename = result["__typename"]
    if typename == "Tweet":
        tweet = result
    elif typename == "TweetWithVisibilityResults":
        tweet = result["tweet"]
    else:
        entryid = tweet_entry["entryid"]
        raise Exception(f"Tweet result for {entryid} has unknown type: {typename}")
    legacy = tweet["legacy"]
    # inject a few user details
    core = tweet["core"]
    user = core["user_results"]["result"]["legacy"]
    basic_user = {}
    for attribute in ["name", "profile_image_url_https", "screen_name", "verified"]:
        basic_user[attribute] = user[attribute]
    # Store off any user urls
    if "url" in user["entities"]:
        basic_user["urls"] = []
        for url in user["entities"]["url"]["urls"]:
            basic_user["urls"].append(url["expanded_url"])
    legacy["user"] = basic_user

    # store card if it exists?
    #card = result["card"]

    return legacy

class TwitterGuestAPI:
    """Class to query Twitter API without a developer account"""
    def __init__(self, session):
        self.headers, self.query_ids = initialise_headers(session, "https://www.twitter.com")

    def get_account(self, session, account_id):
        """Get the json metadata for a single tweet"""
        query_name = "UserByRestId"
        user_query = self.query_ids[query_name]
        status_resp = exploratory_request(
            f"{STATUS_ENDPOINT}{user_query}/", session, self.headers, query_name, "userId", account_id)
        status_json = json.loads(status_resp)
        legacy = status_json["data"]["user"]["result"]["legacy"]
        return legacy

    def get_tweet(self, session, tweet_id):
        """Get the json metadata for a single tweet"""
        query_name = "TweetDetail"
        tweet_details_query = self.query_ids[query_name]
        status_resp = exploratory_request(
            f"{STATUS_ENDPOINT}{tweet_details_query}/", session, self.headers, query_name, "focalTweetId", tweet_id)
        status_json = json.loads(status_resp)
        if "errors" in status_json:
            logging.error(
                "Error getting tweet %s: %s", tweet_id, status_json["errors"][0]["message"])
            return None
        instructions = status_json['data']['threaded_conversation_with_injections']['instructions']
        entries = instructions[0]["entries"]
        tweet_entryid = f"tweet-{tweet_id}"
        tombstone_entryid = f"tombstone-{tweet_id}"
        for entry in entries:
            entryid = entry["entryId"]
            if entryid == tombstone_entryid:
                logging.warning("Tweet %s has been removed", tweet_id)
                break
            if entryid != tweet_entryid:
                # Different tweet in thread
                # Might be nice to optionally return the whole thread?
                continue
            return parse_tweet(entry)
        return None
