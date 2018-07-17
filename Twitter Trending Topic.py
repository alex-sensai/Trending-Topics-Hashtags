import json
import pandas as pd
import twitter
import datetime as dt
import time

## Authorization
print('Authorizing...')
with open("twitter_credentials.json", "r") as file:  
    credentials = json.load(file)
consumer_key = credentials['CONSUMER_KEY']
consumer_secret = credentials['CONSUMER_SECRET']
access_token = credentials['ACCESS_TOKEN']
access_token_secret = credentials['ACCESS_SECRET']
bearer_access_token = credentials['BEARER_ACCESS_TOKEN']
auth = twitter.oauth.OAuth(access_token, access_token_secret,consumer_key,consumer_secret)
twitter_api = twitter.Twitter(auth=auth)
twitter_api

with open("twitter_credentials_2.json", "r") as file:  
    credentials = json.load(file)
bearer_access_token_2 = credentials['BEARER_ACCESS_TOKEN']


# get avialable US location and world location
print('Pulling locations...')
available_location = twitter_api.trends.available()
US_location = [i for i in available_location if (i['country']=='United States')|(i['name']=='Worldwide')]
US_location = pd.DataFrame(US_location)
US_location = US_location[['name','woeid']]

# get trends
print('Pulling trends...')
trends = pd.DataFrame()
for i in US_location['woeid']:
    data = twitter_api.trends.place(_id=i)[0]
    trend = data['trends']
    trend = pd.DataFrame(trend)
    trend['as_of'] = data['as_of']
    trend['location'] = data['locations'][0]['name']
    trend['woeid'] = data['locations'][0]['woeid']
    trends = trends.append(trend)
trends = trends[['as_of','name','tweet_volume','location','woeid']]
trends['Type'] = trends['name'].apply(lambda x: 'Hashtag' if str(x).startswith('#') else 'Topic')

# find comment trend with world
world_trend = list(set(trends.loc[trends['woeid']==1,'name']))
us_trend = list(set(trends.loc[trends['woeid']==23424977,'name']))
#location_trend = list(set(trends.loc[(trends['woeid']!=23424977)&(trends['woeid']!=1),'name']))
trends['World_trend'] = trends['name'].isin(world_trend)
trends.loc[trends['woeid']==1,'World_trend'] = trends.loc[trends['woeid']==1,'name'].isin(us_trend)
trends['as_of'] = trends['as_of'].str.replace('T',' ').str.replace('Z','')


## 1 hour tweets
end_time = dt.datetime.strptime(trends['as_of'].min(), "%Y-%m-%d %H:%M:%S")
start_time = end_time - dt.timedelta(hours=1)

# get post
print('Pulling posts...')
import requests
search_headers_1 = {'Authorization': 'Bearer {}'.format(bearer_access_token)}
search_headers_2 = {'Authorization': 'Bearer {}'.format(bearer_access_token_2)}


def search_topic(q,count):
    n_tweets = 0
    q = q.replace('#','%23')
    # first page
    url = 'https://api.twitter.com/1.1/search/tweets.json?q=%s&result_type=recent&count=%s' %(q,count)
    try:
        search_results = requests.get(url,headers=search_headers_1).json()
        statuses = search_results['statuses']
        test = 'token 1'
    except:
        search_results = requests.get(url,headers=search_headers_2).json()
        statuses = search_results['statuses']
        test = 'token 2'
    # most recent used time
    recent_time = dt.datetime.strptime(statuses[0]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
    last_time = dt.datetime.strptime(statuses[-1]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
    # next page
    while last_time >= start_time:
        try:
            next_results = search_results['search_metadata']['next_results']
        except KeyError as e:
            break
        kwargs = next_results[1:]
        url = url = 'https://api.twitter.com/1.1/search/tweets.json?%s' %(kwargs)
        while True:
            try:
                try:
                    search_results = requests.get(url,headers=search_headers_1).json()
                    statuses += search_results['statuses']
                    test = 'token 1'
                except:
                    search_results = requests.get(url,headers=search_headers_2).json()
                    statuses += search_results['statuses']
                    test = 'token 2'
                last_time = dt.datetime.strptime(statuses[-1]['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
                break
            except:
                time.sleep(905)
    for i in statuses:
        check_time = dt.datetime.strptime(i['created_at'], "%a %b %d %H:%M:%S %z %Y").replace(tzinfo=None)
        if (check_time>= start_time) & (check_time<= end_time):
            n_tweets = n_tweets+1
    df = pd.DataFrame(data=[[q,n_tweets,recent_time]],columns=['name','n_tweets','recent_time'])
    return df

# check only for recent trends
count = 100
df_all = pd.DataFrame()
for q in trends.loc[pd.isnull(trends['tweet_volume']),'name'].unique():
    while True:
        try:
            df = search_topic(q,count)
            df_all = df_all.append(df)
            break
        except:
            time.sleep(905)


df_all['name'] = df_all['name'].str.replace('%23','#')
trends_all = trends.merge(df_all, how='left', on='name')
trends_all['tweets'] = trends_all['tweet_volume'].fillna(trends_all['n_tweets'])
trends_all.loc[pd.isnull(trends_all['tweet_volume']), 'Categories'] = 'Recent'
trends_all.loc[pd.isnull(trends_all['tweet_volume'])==False, 'Categories'] = 'Popular'
trends_all['as_of'] = trends_all['as_of'].min()

## save as json file
print('Output to json file...')
trends_all.to_json('twitter_trending_topics.json',orient='table')