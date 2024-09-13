import pandas as pd 
import requests


def fetch_data(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError for bad responses
    if response.ok:
        return response.json()
    else:
        print(f"HTTP error occurred: Status code {response.status_code}")
        return None

def main():
    #URLs of Interest
    posts_url = 'https://jsonplaceholder.typicode.com/posts'
    comment_url_template = 'https://jsonplaceholder.typicode.com/posts/{}/comments'

    p_response = fetch_data(posts_url)
    post_df = pd.DataFrame(p_response)

    # Create an empty dataframe to append to it later
    comment_dfs = []
        
    # Iterate over each post to generate the request.get link
    for post_id in post_df['id']:
        comment_url = comment_url_template.format(post_id)
        c_response = fetch_data(comment_url)
        if c_response:
            comment_df = pd.DataFrame(c_response)
            comment_dfs.append(comment_df)


        # Obtain the comment list of dictionaries for each post and append to the comment df 
    if comment_dfs:
        comment_df = pd.concat(comment_dfs,ignore_index=True)
        normalize_df(comment_df)
    else:
        comment_df = pd.DataFrame()
    
    print(comment_df.head(40))
    print(post_df.head(40))


def normalize_df(df):
    if 'email' in df.columns:
        df['email'] = df['email'].str.lower()

    if 'name' in df.columns:
        if df['name'].notna().all():
            print('All Names are present')
        else:
            missing_names_count = df['name'].isna().sum()
            print(f'There are {missing_names_count} missing names in the df')


main()
