
import pandas as pd
from datetime import datetime
import s3fs 
import requests
import json
import praw


def extract_data():
    
    client_id = 'Dzt9MOJoiPTha5Cw4QZG_Q'  
    client_secret = 'R2PZfWJcyGXJ6Bx3Og7sZZkyVQUkDQ'  
    username = 'Abdo0_00'
    password = 'Abdo0*1234'

    auth = requests.auth.HTTPBasicAuth(client_id, client_secret)

    data = {
        'grant_type': 'password',
        'username': username,
        'password': password
    }

    headers = {
        'User-Agent': 'etl_airflow_project/0.0.1 by Abdo0_00'
    }

    res = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)
    
    
    if res.status_code != 200:
        print("Error:", res.status_code)
        print("Response:", res.json())
        return None

    token = res.json().get('access_token')
    if not token:
        print("Error: No access token found.")
        return None

    headers['Authorization'] = f'bearer {token}'

    
    subreddit = 'python'
    url = f'https://oauth.reddit.com/r/{subreddit}/hot'

    res = requests.get(url, headers=headers)
    posts = res.json().get('data', {}).get('children', [])

    
    posts_data = []

    for post in posts:
        post_data = post['data']
        posts_data.append({
            'title': post_data['title'],
            'score': post_data['score'],
            'id': post_data['id'],
            'url': post_data['url'],
            'num_comments': post_data['num_comments'],
            'created': post_data['created'],
            'body': post_data.get('selftext', '')  
        })  
        
    df = pd.DataFrame(posts_data)   
    df.to_csv("Etl_Reddit.csv",index=False)
 

def transform_data():
    df= pd.read_csv("Etl_Reddit.csv")
    df['created'] = pd.to_datetime(df['created'], unit='s')
    df.to_csv('transformed_data.csv',index=False)

def load_data():
    df=pd.read_csv("transformed_data.csv")
    df.to_csv(r"s3://abdodataengenieer/transformed_data.csv",index=False)

    




