import json
import pandas as pd
import requests as r
import re 
from bs4 import BeautifulSoup
import boto3
from io import StringIO
client = boto3.client('ssm')
url = 'https://en.wikipedia.org/wiki/List_of_dinosaur_genera'
s3 = boto3.resource('s3')
s3_bucket_name = 'tripelrouput'
s3_key = 'scrapping-Files/dino_data.csv'
html = r.get(url)
soup = BeautifulSoup(html.text,'html.parser')
urls = soup.find_all('a', href=True)
links_and_names = [(url['href'], url.text) for url in urls]
dino_data_clean = [links_and_names[link] for link in range(len(links_and_names)) if links_and_names[link][0].startswith("/wiki/")]
dino_data_clean = dino_data_clean[:2317:]
dino_df = pd.DataFrame(dino_data_clean, columns = ['url', 'dinosaur'])
dino_df['dinosaur'] = dino_df['dinosaur'].replace('', None)
dino_df = dino_df.dropna(axis = 0, subset = ['dinosaur'])
dino_data_clean = dino_df.set_index('url')['dinosaur'].to_dict()
dino_data = [('https://en.wikipedia.org'+ url, dinosaur) for url, dinosaur in dino_data_clean.items()]
dino_data = dino_data[33::]
dino_urls = [element for pair in dino_data for element in pair if element.startswith('https://en.wikipedia.org')]
def lambda_handler(event, context):
    dino_info = []
    for url in range(200):
        html = r.get(dino_urls[url],timeout=120)
        soup = BeautifulSoup(html.text, 'html.parser')
        paragraphs = soup.select('p')
        clean_paragraphs = [paragraph.text.strip() for paragraph in paragraphs]
        clean_paragraphs = clean_paragraphs[:4:]    
        dino_info.append(' '.join(clean_paragraphs))
    dino_df = pd.DataFrame(dino_data, columns = ['URL','Dinosaur'])
    dino_details = pd.DataFrame(dino_info, columns = ['Info'])
    dino_df = pd.concat([dino_df, dino_details], ignore_index = True, axis = 1)
    print(dino_df)
    file_name = r's3://tripelrouput/scrapping-Files/'
    csv_buffer = StringIO()
    dino_df.to_csv(csv_buffer, index=False)
    #Upload the Excel file to S3
    s3.Object(s3_bucket_name, s3_key).put(Body=csv_buffer.getvalue())
    print('DataFrame is written to csv File successfully!')
    ## reading 
    response = s3_read.get_object(Bucket=s3_bucket_name, Key=s3_key)
    # Convert csv content to DataFrame
    dino_df = pd.read_csv(response['Body'])
    dino_df.columns = ['URLs', 'Dinosaur', 'Info']
    dino_info = dino_df['Info'].to_dict()
    dino_info = dino_info.values()
    heights = []
    for element in dino_info:
        if re.findall('\d+\smeters', str(element)):
            height = re.findall('\d+\smeters', str(element))
            heights.append(height)
        else:
            heights.append(list('-'))
            heights_clean = []
    for element in heights:
        for height in enumerate(element):
            if height[0] == 0:
                heights_clean.append(height[1])
    weights = []
    for element in dino_info:
        if re.findall('\d+\stonnes|\d+\skilograms', str(element)):
            weight = re.findall('\d+\stonnes|\d+\skilograms', str(element))
            weights.append(weight)
        else:
            weights.append(list('-'))
    weights_clean = []
    for element in weights:
        for weight in enumerate(element):
            if weight[0] == 0:
                weights_clean.append(weight[1])
    dino_df.drop('Info', inplace=True, axis=1)
    heights_df = pd.DataFrame(heights_clean, columns=['Height'])
    weights_df = pd.DataFrame(weights_clean, columns=['Weight'])
    dino_df = pd.concat([dino_df, heights_df, weights_df], ignore_index=True, axis=1)
    dino_df.columns = ['URL', 'Dinosaur', 'Height', 'Weight']
    # dino_df = dino_df
    # print(dino_df)
    file_name = r's3://tripelrouput/scrapping-Files/'
    csv_buffer = StringIO()
    dino_df.to_csv(csv_buffer, index=False)
    #Upload the Excel file to S3
    s3.Object(s3_bucket_name, s3_key).put(Body=csv_buffer.getvalue())
    print('Heights amd weights are written to csv File successfully!')
   
