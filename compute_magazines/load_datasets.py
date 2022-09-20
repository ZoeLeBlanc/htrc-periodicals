import os
import pandas as pd
import warnings
warnings.filterwarnings('ignore')

def cut_scanners(rows):
    first_page = rows.loc[ (rows.type_of_page == 'split_issue')].page_number.values.tolist()
    print(first_page)
    if len(first_page) > 0:
        first_page = first_page[0]
        pages = rows.loc[rows.page_number > first_page]
    else:
        # first_page = rows.loc[(rows.type_of_page == 'toc')].page_number.values.tolist()[0]
        pages = rows
    return pages

def clean_df(df):
    df = df.groupby('start_issue', as_index=False).apply(cut_scanners).reset_index()
    df = df.loc[:, ~df.columns.str.contains('^level')]
    return df

def clean_arab_observer_df(arabobserver_df):
    arabobserver_df.loc[(arabobserver_df.start_issue == '1965-06-07') & (arabobserver_df.page_number == 328), 'notes'] = 'Not actually a cover'

    arabobserver_df.loc[(arabobserver_df.start_issue == '1965-06-07') & (arabobserver_df.page_number == 328), 'type_of_page'] = 'cover_page'
    arabobserver_df.loc[(arabobserver_df.start_issue == '1965-06-07') & (arabobserver_df.page_number == 328), 'start_issue'] = '1965-06-14'
    return arabobserver_df

def clean_afro_asian_df(afroasian_df):
    afroasian_df.loc[(afroasian_df.start_issue == '1967-06-01') & (afroasian_df.page_number == 2), 'notes'] = 'Not actually a cover'
    afroasian_df.loc[(afroasian_df.start_issue == '1967-06-01') & (afroasian_df.page_number == 2), 'type_of_page'] = 'cover_page'
    afroasian_df.loc[(afroasian_df.start_issue == '1967-09-01') & (afroasian_df.page_number == 4), 'notes'] = 'Not actually a cover'
    afroasian_df.loc[(afroasian_df.start_issue == '1967-09-01') & (afroasian_df.page_number == 4), 'type_of_page'] = 'cover_page'
    return afroasian_df

def get_full_combined_dataset(output_path, output_directory):
    if os.path.exists(output_path):
        df = pd.read_csv(output_path)

    else:
        dfs = []
        for root, dirs, files in os.walk(output_directory):
            print(root)
            for f in files:
                if ('.csv' in f) and (root != output_directory):
                    df = pd.read_csv(os.path.join(root, f), low_memory=False)
                    if "Arab_Observer" in f:
                        df = clean_arab_observer_df(df)
                    if "Afro_Asian_Bulletin" in f:
                        df = clean_afro_asian_df(df)

                    dfs.append(df)
        df = pd.concat(dfs)
        df = df.rename(columns={'title': 'ht_generated_title', 'magazine_title': 'cleaned_magazine_title', 'link': 'hdl_link', 'volumes': 'volume_number', 'original_volumes': 'cleaned_volume'})
        df['datetime'] = pd.to_datetime(df.start_issue, format='%Y-%m-%d', errors='coerce')
        df.to_csv(output_path, index=False)
    return df

def get_serial_htids(output_path):
    if os.path.exists(output_path):
        serial_htid_df = pd.read_csv(output_path)
    else:
        dir_list = [subdir for subdir, _, _ in os.walk('../ht_ef_datasets/')]
        annotated_df = pd.read_csv('../generate_hathitrust_data/annotation_metadata_mapping.csv')
        dfs = []
        for subdir, dirs, files in os.walk('../metadatas'):
                for f in files:
                    if '.csv' in f:
                        annotation_row = annotated_df.loc[annotated_df['metadata_file'] == subdir + '/' + f].copy()
                        file_name = f.split('.')[0]
                        filenames = file_name.split('_')
                        filenames = [ fi for fi in filenames if fi.isdigit() == False]

                        filenames = '_'.join(filenames)
                        md = pd.read_csv(subdir+ '/'+f, encoding = "utf-8")
                        md['magazine_title'] = filenames
                        dfs.append(md)
        serial_htid_df = pd.concat(dfs)
        serial_htid_df.rename(columns={'vol_id': 'htid'}, inplace=True)
        serial_htid_df.to_csv(output_path, index=False)
    return serial_htid_df

def get_combined_issues(output_path, uncombined_df_path):
    if os.path.exists(output_path):
        issue_df = pd.read_csv(output_path)
    else:
        df = pd.read_csv(uncombined_df_path, low_memory=False)
        df.token = df.token.astype(str)
        df.volume_number = df.volume_number.fillna(0)
        issue_df = df.groupby(['cleaned_magazine_title', 'ht_generated_title', 'volume_number', 'htid', 'hdl_link','cleaned_volume', 'start_issue', 'end_issue', 'datetime','dates', 'issue_number', 'type_of_page', 'sequence'], as_index = False).agg({'token': ' '.join, 'pos': list, 'count': list, 'section': list})
    return issue_df