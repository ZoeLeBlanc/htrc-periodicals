
import pandas as pd
import numpy as np
import altair as alt
 
import warnings
warnings.filterwarnings('ignore')

def chart_overall_frequency(df):
    '''Chart overall frequency for all magazines in the corpus. Tooltips show dates, title, and size of magazine. Circles are sized by counts of words in the magazine.'''
    
    selection = alt.selection_single(empty='all', fields=['datetime'])
    chart = alt.Chart(df[['datetime', 'original_counts', 'magazine_title']]).mark_circle(
        opacity=0.5,
        stroke='black',
        strokeWidth=1
    ).encode(
        x='datetime:T',
        y='magazine_title:O',
        size=alt.Size('count()', scale=alt.Scale(range=[0, 2000]),),
        color=alt.Color('magazine_title:O',
                        scale=alt.Scale(scheme='plasma')),
#         row=alt.Row('magazine_title:O', 
#                     title=None,
#                     header=alt.Header(labelAngle=0,
#                                       labelAlign='left',
#                                       labelAnchor='middle'),
#                     spacing=10)
        opacity=alt.condition(
            selection, alt.value(1), alt.value(0.2)),
        tooltip=['datetime', alt.Tooltip('count()', title='Size of magazine'), alt.Tooltip('sum(original_counts)', title='Number of words'), 'magazine_title']
        
    ).add_selection(
        selection
    ).properties(
        width=1500,
        height=200
    )
    return chart

def chart_coverage_frequency(df, group_column, counts_column, text_column, term):
    """Method for calculating rate of coverage for either one term or a group of terms (grouped). Generates frequency graphs for term/terms, as well as the publication as a whole. Then compares rate of coverage to rate of publishing."""
    
    total_words = df.groupby([group_column])[counts_column].sum().reset_index()
    total_words['type'] = 'total_counts'
    
    total_term_words = df[(df[text_column].str.contains('|'.join(term)))& (df[text_column].isna()==False)].groupby([group_column])[counts_column].sum().reset_index()
    total_term_words['type'] = 'term_counts'
    
    totals = pd.concat([total_words, total_term_words])
    
    chart = alt.Chart(totals).mark_bar().encode(
        x='{}:T'.format(group_column),
        y='{}:Q'.format(counts_column),
        color='type:N'
    ).facet(
        row='type:N',
    ).resolve_scale(y='independent')
    
    perc_words = pd.merge(total_words, total_term_words, on=['datetime'], how='left')
    perc_words.original_counts_y.fillna(0, inplace=True)
    perc_words.type_y.fillna(method='ffill', inplace=True)

    chart_1 = alt.Chart(perc_words).mark_point().encode(
        x='{}_x:Q'.format(counts_column),
        y='{}_y:Q'.format(counts_column),
        color='year({}):O'.format(group_column)
    )

    chart_2 = chart_1 + chart_1.transform_regression('{}_x'.format(counts_column), '{}_y'.format(counts_column)).mark_line()
    
    final_charts = alt.hconcat(chart, chart_2).resolve_scale(color='independent')
    
    
    return final_charts

def compare_pub_counts(df, group_columns, counts_column, text_column, terms):
    '''Get frequency for a set of terms in a group of publications'''
    
    total_sum = df.groupby(group_columns)[counts_column].sum().reset_index()
    dfs = []
    for term in terms:
        total_term_words = df[(df[text_column].str.contains(term)== True)]
        total_term_words['term_counts']= total_term_words[text_column].apply(lambda x: x.count(term))
        total_pages = total_term_words.groupby(group_columns)['page_number'].apply(lambda x: list(x)).reset_index()
        total_term_counts = total_term_words.groupby(group_columns)['term_counts'].sum().reset_index()
        total_counts = total_term_words.groupby(group_columns)[counts_column].sum().reset_index(name='page_counts')

        totals = pd.merge(total_pages, total_counts, on=group_columns)
        totals = pd.merge(total_term_counts, totals, on=group_columns)
        totals = pd.merge(total_sum, totals, on=group_columns, how='outer')
        totals[['term_counts', 'page_counts']] = totals[['term_counts', 'page_counts']].fillna(0)
        totals['term'] = f'{term}'
        dfs.append(totals)
    concat_df = pd.concat(dfs)
    return concat_df
    
def create_line_graph_term_frequencies(term_counts):
    '''Create line graphs of term frequencies by publication. Terms are rows, titles are colors.'''
    selection = alt.selection_multi(fields=['title'], bind='legend')
    chart = alt.Chart(term_counts).mark_bar(size=2).encode(
        x=alt.X('datetime:T', axis=alt.Axis(title='')),
        y=alt.Y('term_counts:Q', axis=alt.Axis(title='')),
        color=alt.Color('magazine_title:N', scale=alt.Scale(scheme='plasma')),
        row=alt.Row('term:N', 
            title=None,
            header=alt.Header(labelAngle=0,
                            labelAlign='left',
                            labelAnchor='end'),
            spacing=10),
        opacity=alt.condition(selection, alt.value(1), alt.value(0.2))
    ).add_selection(
        selection
    ).properties(
        width=500,
        height=100
    ).resolve_scale(
        x='independent',
        y='independent'
    )
    return chart

def create_regression_graph_term_frequencies(term_counts):
    selection = alt.selection_multi(fields=['year'], bind='legend')
    term_counts['year'] = term_counts.datetime.dt.year
    term_counts = term_counts.sort_values(by=['year'])
    years = term_counts.year.unique().tolist()
    base = alt.Chart(term_counts[['datetime', 'term_counts', 'term', 'page_counts', 'magazine_title', 'page_number', 'year']]).mark_point().encode(
        x=alt.X('page_counts:Q', axis=alt.Axis(title='')),
        y=alt.Y('term_counts:Q', axis=alt.Axis(title='')),
        
    )

    column_field = 'magazine_title:O' if len(term_counts.magazine_title.unique().tolist()) > len(term_counts.term.unique().tolist()) else 'term:O'
    row_field = 'term:O' if len(term_counts.magazine_title.unique().tolist()) > len(term_counts.term.unique().tolist()) else 'magazine_title:O'

    chart = alt.layer(
        base.mark_circle(
            opacity=0.5,
            stroke='black',
            strokeWidth=1
        ).encode(
            size=alt.Size('term_counts', scale=alt.Scale(range=[20, 100])),
            color=alt.Color('year:O',
                scale=alt.Scale(scheme='plasma'),
                legend=alt.Legend(columns=4), sort=years), 
            opacity=alt.condition(selection, alt.value(1), alt.value(0.2)),
            tooltip=['datetime', 'term', 'term_counts', 'page_number', 'page_counts', 'magazine_title', 'year']
        ).add_selection(selection).properties(width=75, height=75),
        base.transform_regression(
            'page_counts', 'term_counts',
        ).mark_line(color='grey')
    ).facet(
        column=alt.Column(column_field),
        row=alt.Row(row_field,
                    title=None,
                    header=alt.Header(labelAngle=0,
                            labelAlign='left',
                            labelAnchor='middle')
                ),
    ).resolve_scale(
        x='independent',
        y='independent'
    )
    return chart
