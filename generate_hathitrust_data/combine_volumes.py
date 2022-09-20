import pandas as pd 

def remove_duplicates(rows):
    final_rows = rows.copy()
    if any(rows.type_of_page == 'duplicates'):
        pages = rows[rows.type_of_page == 'duplicates'].notes.values[0]
        final_rows = rows[~rows.sequence.between(int(pages.split('-')[0]), int(pages.split('-')[1]))]
    return final_rows


def combine_volumes():
    pass