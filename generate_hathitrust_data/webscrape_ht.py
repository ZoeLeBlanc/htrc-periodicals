from bs4 import BeautifulSoup
import pandas as pd
import requests
import os
import shutil
from progress.bar import IncrementalBar

def get_hathi_links(url, output_path, annotated_df):
    '''This function scrapes volume links from a Hathi Trust record page.'''
    result = requests.get(url)
    ht_page = result.content
    soup = BeautifulSoup(ht_page, 'html.parser')
    
    write_dataframe(soup, output_path, annotated_df)
   

def write_dataframe(soup, output_path, annotated_df):
    """This function writes a dataframe containing the correct hathitrust id and link, along with listed volumes to a csv file. We check for what volumes we want to keep using the original annotated dataset file"""
    links = soup.find_all('tr')
    
    vols = annotated_df.original_volumes.unique().tolist()
    vols = [vol for vol in vols if str(vol) != 'nan']
    for l in links:
        if any( vol in l.get_text() for vol in vols):
            date = l.find(attrs={"class":"IndItem"})
            link = l.find(attrs={"class":"rights-Array searchonly"}).get('href')
            htid = link.split('/')[-1]
            new_df = {}
            new_df['link'] = link
            new_df['date'] = date.text
            new_df['htid']= htid
        
            dl = pd.DataFrame([new_df])
            if os.path.exists(output_path):
                dl.to_csv(output_path, mode='a', header=False, index=False)
            else:
                dl.to_csv(output_path, header=True, index=False)

def get_catalog_records():
    """This function takes an annotation spreadsheet and scrapes the relevant Hathi Trust catalog page and returns a dataframe containing the Hathi Trust id and link"""
    output_file = 'annotation_metadata_mapping.csv'
    if os.path.exists(output_file):
        os.remove(output_file)
    if os.path.exists('../metadatas'):
        shutil.rmtree('../metadatas')
    os.makedirs('../metadatas') 
    for subdir, dirs, files in os.walk('../annotated_datasets'):
        get_files = IncrementalBar('getting metadata links', max=len(files))
        for f in files:
            get_files.next()
            record_ids = []
            if (f.endswith('.csv')) and ('freedomways' not in f):
                annotation_df = pd.read_csv(subdir+ '/'+f, encoding = "utf-8")
                annotation_df.columns = ['_'.join(x.lower().split(' ')) for x in annotation_df.columns]
                annotation_df.notes.fillna('', inplace=True)
                records = annotation_df[annotation_df.notes.str.lower().str.contains('record')].notes.tolist()
                for record in records:
                    [record_ids.append(chunk) for chunk in record.split(' ') if chunk.isdigit()]
                for record_id in record_ids:
                    combined_ids = '_'.join(record_ids)
                    output_path = '../metadatas' + '/' + f.split('_annotated')[0] + f'_{combined_ids}.csv'
                    url = f'https://catalog.hathitrust.org/Record/{record_id}'
                    get_hathi_links(url, output_path, annotation_df)
                    df = pd.DataFrame([{'annotation_file': subdir+ '/'+f, 'metadata_file': output_path, 'magazine_name': f.split('_annotated')[0]}])
                    if os.path.exists(output_file):
                        df.to_csv(output_file, mode='a', header=False, index=False)
                    else:
                        df.to_csv(output_file, header=True, index=False)
        get_files.finish()
if __name__ ==  "__main__" :
    get_catalog_records()

