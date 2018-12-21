import config,json,csv
from bs4 import BeautifulSoup
import time,random
import re
import database_driver

session = config.get_session()

def get_url(ig_handle):
    return config.BASE_URL + "/" + ig_handle

def get_webpage(url):
    return session.get(url).text

def decode_string(text):
    return re.sub('[^ a-zA-Z0-9]', '', text)

#takes the raw js string from ig page and returns a formatted json
def get_json_from_string(js_text):
    js_text = js_text.strip()
    try:
        start_index = js_text.index('{')
        json_string = js_text[start_index:-1]
        return json_string
    except:
        return None
#parses raw html page for the data json on that instagram page
def get_data_json(webpage):
    soup = BeautifulSoup(webpage,"html.parser")
    script = soup.select("body script")[0].get_text()
    json_obj = get_json_from_string(script)
    if json_obj:
        try:
            return json.loads(json_obj)
        except:
            pass
    return None

#parses a caption for mentions and hashtags
def parse_caption(caption_text):
    mentions = re.findall(r'@[\w]*[\s#@.!?]?',caption_text)
    hashtags = re.findall(r"#[\w]*[\s#@.!?]?",caption_text,re.IGNORECASE)
    if mentions:
        mentions = [mention.strip() for mention in mentions]
    if hashtags:
        hashtags = [hashtag.strip() for hashtag in hashtags]
    return mentions,hashtags

#takes one ig post and formats it
def format_post_data(ig_post):
    caption = ig_post['node']['edge_media_to_caption']['edges'][0]['node']['text']
    mentions,hashtags = parse_caption(caption)
    return {
        "id":ig_post['node']['id'],
        "caption":decode_string(caption),
        "timestamp":ig_post['node']['taken_at_timestamp'],
        "likes":ig_post['node']['edge_liked_by']['count'],
        "mentions":mentions,
        "hashtags":hashtags
    }

#filters out data from the raw ig_json and returns a clean object
def get_ig_object(ig_json):
    if not ig_json:
        return None
    user_info = ig_json['entry_data']['ProfilePage'][0]['graphql']['user']
    ig_object = {
        'bio':decode_string(user_info['biography']),
        'id':decode_string(user_info['id']),
        'username':decode_string(user_info['username']),
        'followers':user_info['edge_followed_by']['count'],
        'full_name':decode_string(user_info['full_name']),
        'posts':[format_post_data(post) for post in user_info["edge_owner_to_timeline_media"]['edges']]
    }
    return ig_object

#given valid ig handle returns an ig_object
def get_ig_page(handle):
    url = get_url(handle)
    webpage = get_webpage(url)
    return get_ig_object(get_data_json(webpage))

def get_handles(input_file):
    handle_list = []
    with open(input_file,'r',encoding='utf-8-sig') as infile:
        csv_reader = csv.reader(infile, delimiter=',')
        for row in csv_reader:
            handle_list.append(row[0])
    return handle_list

def write_json(output_file,data):
    with open(output_file,'w') as outfile:
        json.dump(data,outfile,ensure_ascii=True)

def read_json(input_file):
    with open(input_file,'r') as infile:
        a = json.load(infile)
        return a

def get_all_profiles(handle_list):
    for handle in handle_list:
        time.sleep(random.randint(1,10))
        yield get_ig_page(handle)

def main():
    neo_driver = database_driver.DatabaseDriver(config.DATABASE_URI,config.DATABASE_USERNAME,config.DATABASE_PASSWORD)
    handle_list = get_handles(config.INPUT_FILEPATH)
    for idx,ig_object in enumerate(get_all_profiles(handle_list)):
        if ig_object:
            neo_driver.create_user(ig_object)
            print("added ", ig_object['username'], " to the database")
        else:
            print("unable to add ",handle_list[idx], " to the database")

main()