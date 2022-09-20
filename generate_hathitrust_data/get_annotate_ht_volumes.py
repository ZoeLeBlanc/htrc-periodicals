import pandas as pd 
from htrc_features import FeatureReader
import os
from progress.bar import IncrementalBar
from thefuzz import fuzz
from datetime import datetime

def transform_annotated_dates(rows):
    """Transform metadata dates into mergeable dates"""
    date = rows[0:1].dates.values[0].replace('-', ' ').split(' ')
    day = date[1] if (len(date) == 3) and ('-' not in rows[0:1].dates.values[0]) else '1'
    start_month = date[0] 
    end_month = date[1] if (len(date) > 2) and ('-' in rows[0:1].dates.values[0]) else start_month
    year = date[-1]

    start_date = day +' ' + start_month + ' ' + year
    end_date = day +' ' + end_month + ' ' + year
    rows['start_issue'] = datetime.strptime(start_date, '%d %B %Y')
    rows['end_issue'] = datetime.strptime(end_date, '%d %B %Y')
    return rows

def cut_vols(rows):
    # Remove duplicates from issues and also make sure that sequence doesn't include first page for some reason I can't remember now...
    ends = rows[rows.type_of_page == 'end_of_issue'].sequence.tolist()[-1]
    rows = rows[rows.sequence <= ends]
    final_rows = rows
    # if any(rows.type_of_page == 'scanner_page') ==:
    first_page = rows[rows.type_of_page == 'cover_page'].sequence.values.tolist()
    if len(first_page) > 0:
        first_page = first_page[0]
    else:
        first_page = rows[rows.type_of_page == 'toc'].sequence.values.tolist()[0]
    final_rows = rows[rows.sequence > first_page -1]
    if any(rows.type_of_page == 'duplicates'):
        pages = rows[rows.type_of_page == 'duplicates'].notes.values[0]
        final_rows = rows[~rows.sequence.between(int(pages.split('-')[0]), int(pages.split('-')[1]))]
    return final_rows


def clean_annotated_df(annotated_df):
    """Clean and normalize dates in the annotated datasets that were created manually in Notion"""
    annotated_df.columns = [x.lower().replace(' ', '_') for x in annotated_df.columns]
    annotated_df.notes = annotated_df['notes'].fillna('')
    annotated_df = annotated_df.fillna(method='ffill')
    
    annotated_df = annotated_df.groupby('dates').apply(transform_annotated_dates)
    return annotated_df
    

def merge_datasets(annotated_df, df):
    """Merge extracted features dataset with the annotated one"""
    final_anno = df.merge(annotated_df, on=['original_volumes', 'sequence'], how='outer')
    final_anno = final_anno.sort_values(by=['original_volumes', 'sequence'])
    final_anno.type_of_page.fillna('content', inplace=True)

    final_anno.update(final_anno[['token','notes', 'section', 'pos']].fillna(''))
    final_anno.update(final_anno[['count']].fillna(0))
    final_anno.fillna(method='ffill', inplace=True)
    final_anno.fillna(method='bfill', inplace=True)
    # final_anno['implied_zero'] = final_anno['page_number'] - final_anno['number'] 
    
    # final_anno = final_anno.drop(columns=['index'])
    # final_anno = final_anno.drop_duplicates(subset=['date_vols', 'page_number'], keep='last')
    final_anno.reset_index(drop=True)
    # final_anno = final_anno.groupby('date', as_index=False).apply(cut_vols).reset_index()
    # final_anno = final_anno.loc[:, ~final_anno.columns.str.contains('^level')]
    return final_anno

def read_ids(md, folder, annotated_df):
    '''This function reads in the list of ids scraped from Hathi Trust and the folder destination. It gets the volume and tokenlist from Hathi Trust, and then calls spread table which separates out by page all tokens.'''
    directory = os.path.dirname(folder)
    if not os.path.exists(directory):
        os.makedirs(directory)

    volids = md['htid'].tolist()
    fr = FeatureReader(ids=volids)

    for vol in fr:
        row = md.loc[md['htid'] == vol.id].copy()
        # print(row, vol.title)
        title = vol.title if ':' not in vol.title else vol.title.split(':')[0]
        
        title = title.lower().replace('.', '').split(' ')
        magazine_title = "_".join(title)
        title = "_".join(title)+'_'+ '_'.join(str(row.date.values[0]).split(' '))
        title = title.replace(',', '_')
        title = title.replace('__', '_')
        file_name = folder+ '/' + title + '.csv'
        date_vols = row.date.values[0]
        subset_annotated_df = annotated_df.loc[annotated_df.original_volumes == date_vols]
        
        if not os.path.exists(file_name):
            # print(f'processing {file_name}')
            volume_df = vol.tokenlist(section='all')
            volume_df = volume_df.reset_index()
            file_name = file_name
            volume_df['magazine_title'] = magazine_title
            volume_df['title'] = title
            volume_df['htid'] = row.htid.values[0]
            volume_df['link'] = row.link.values[0]
            volume_df['original_volumes'] = date_vols
            volume_df = volume_df.rename(columns={'lowercase': 'token', 'page': 'sequence'})
            subset_annotated_df = subset_annotated_df.rename(columns={'page_number': 'sequence'})
            merged_df = merge_datasets(subset_annotated_df, volume_df)
            merged_df.to_csv(file_name, index=False)
        #     # spread_table(title, file_name) #Run this line if you want to group characters on pages into single rows
        # elif os.path.exists(file_name.split('.csv')[0] + '_grouped.csv'):
        #     add_volumes_dates(title, file_name, magazine_title, date_vols)
        # else:
        #     print(f'{file_name} already exists')

def add_volumes_dates(title, file_name, magazine_title, date_vols):
    output_file = file_name.split('.csv')[0] + '_grouped.csv'
    df = pd.read_csv(output_file)
    df['date_vols'] = date_vols
    df['magazine_title'] = magazine_title
    df['title'] = title

    df.to_csv(output_file, index=False)

# def spread_table(title, file_name):
#     df = pd.read_csv(file_name)
#     pages = df['page'].unique()
#     final_df = df[0:0]
#     get_data = IncrementalBar('spreading table', max=len(pages))
#     for i, page in enumerate(pages):
#         get_data.next()
#         page_df = df[0:0]
#         selected_rows = df.loc[df['page'] == page].copy()
#         for index, row in selected_rows.iterrows():
#             token_count = row['count']
#             for i in range(0, token_count):
#                 page_df = page_df.append(row, ignore_index=True)
#         final_df = final_df.append(page_df, ignore_index=True)
#     get_data.finish()
#     final_df = final_df.drop(columns=['section', 'count'])

#     final_df['lowercase'] = final_df['lowercase'].astype(str)
#     groupby_df = final_df.groupby('page')['lowercase'].apply(' '.join).reset_index()
#     final_df = final_df.drop_duplicates(subset=['page'], keep='first')
#     final_df = final_df.drop(columns='lowercase')
#     final = pd.merge(final_df, groupby_df, on='page', how='outer')
#     final_df = final[['page', 'lowercase']]
    
#     final_df.to_csv(title + '_grouped.csv')

def process_metadatas():
    '''This function is used to process the metadata files (i.e. the htids) and combine them with the annotated files. It reads in the metadata files and then calls the read_ids function.'''
    # Get all directories of hathi_ef_datasets
    dir_list = [subdir for subdir, _, _ in os.walk('../ht_ef_datasets/')]
    annotated_mapping_df = pd.read_csv('annotation_metadata_mapping.csv')
    dfs = []
    
    for subdir, _, files in os.walk('../metadatas'):
        get_files = IncrementalBar('processing metadata', max=len(files))
        for f in files:
            get_files.next()
            if '.csv' in f:
                # Get relevant annotation file
                annotation_row = annotated_mapping_df.loc[annotated_mapping_df['metadata_file'] == subdir + '/' + f].copy()
                # Start creating file name and joining directories for files
                file_name = f.split('.')[0]
                filenames = file_name.split('_')
                filenames = [ fi for fi in filenames if fi.isdigit() == False]

                filenames = '_'.join(filenames)
                # print(filenames)
                md = pd.read_csv(subdir+ '/'+f, encoding = "utf-8")
                annotated_df = pd.read_csv(annotation_row.annotation_file.values[0])
                final_dir = ''
                for dir_name in dir_list:
                    dirnames = dir_name.split('/')[-1].split('_')
                    dirnames = [ di.lower() for di in dirnames if di.isdigit() == False]
                    dirnames = '_'.join(dirnames)
                    dirnames = dirnames.split('_hathitrust')[0]
                    fuzziness =fuzz.ratio(filenames, dirnames)
                    if fuzziness > 70:
                        final_dir = dir_name
                        df = pd.DataFrame([{'local_dir': dir_name.split('/')[-1], 'file_name': filenames, 'fuzzy_ratio': fuzziness, 'metadata_file': subdir+ '/'+f, 'final_dir': final_dir}])
                        dfs.append(df)
                annotated_df.Dates = annotated_df.Dates.str.replace('Decmeber', 'December')
                annotated_df.Dates = annotated_df.Dates.str.replace('Summer', 'July')
                annotated_df = clean_annotated_df(annotated_df)
                read_ids(md, final_dir, annotated_df)
            get_files.finish()
    dfs = pd.concat(dfs)
    final_df = pd.merge(annotated_mapping_df, dfs, on='metadata_file')
    final_df.to_csv('directory_annotation_metadata_mapping.csv', index=False)


if __name__ ==  "__main__" :
    process_metadatas()

    