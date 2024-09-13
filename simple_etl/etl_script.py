import pandas as pd 
import requests

posts_url = 'https://jsonplaceholder.typicode.com/posts'
comment_url_template = 'https://jsonplaceholder.typicode.com/posts/{}/comments'
p_response = requests.get(posts_url)

if p_response.ok:
    pdf = pd.DataFrame(p_response.json())

    # Create an empty dataframe to append to it later
    cdf = pd.DataFrame()
    
    # Iterate over each post to generate the request.get link
    for post_id in pdf['id']:
        comment_url = comment_url_template.format(post_id)
        c_response = requests.get(comment_url)
     
    # Obtain the comment list of dictionaries for each post and append to the comment df
        if c_response.ok:
            cdf_temp = pd.DataFrame(c_response.json())
            cdf = pd.concat([cdf,cdf_temp])

else:
    raise Exception('request not working')


#Validate the data

#Add 2 tables to a sqlite db