import os
import pandas as pd
import requests
import zipfile
import io
from sklearn.preprocessing import MultiLabelBinarizer
import matplotlib.pyplot as plt
import plotly.express as px
from plotly.subplots import make_subplots


def get_full_path(_path):
    full_path = os.path.expanduser(_path)
    return full_path

def download_and_extract_raw_data(max_survey_year, survey_raw_data_dir):

    for year in range(2011, max_survey_year + 1):
        out_dir = survey_raw_data_dir + str(year) + '/'

        # don't re-download if we already have it
        if os.path.exists(out_dir):
            continue

        # download the file
        url = f'https://info.stackoverflowsolutions.com/rs/719-EMH-566/images/stack-overflow-developer-survey-{year}.zip'
        print(f'Downloading survey data for {year}')
        res = requests.get(url)
        if not res.ok:
            raise Exception(f'The following zip file url failed to download: {url}')

        # convert to zipfile object
        zip_file = zipfile.ZipFile(io.BytesIO(res.content))

        # write
        os.makedirs(out_dir)
        zip_file.extractall(out_dir)
        print(f'done downloading and extracting survey results for {year}')

def safe_rename(df, columns):
    '''
    Renames columns in a df in a more strict manner. Old column names must exist,
    new ones must not, and the rename dict can't have duplicate keys or values.
    :param df: dataframe to have its columns renamed
    :param columns: dict of {column_old_name: column_new_name}
    '''
    try:
        assert len(set(columns.keys())) == len(columns.keys())
    except:
        raise Exception('Rename dict contains duplicate keys')

    try:
        assert len(set(columns.values())) == len(columns.values())
    except:
            raise Exception('Rename dict contains duplicate values')


    for old_name, new_name in columns.items():
        # avoiding renaming to a column that already exists in the df
        try:
            assert new_name not in df
        except:
            raise Exception(f'{old_name} already present in df')

        # old name must exist
        try:
            assert old_name in df
        except:
            raise Exception(f'{old_name} not in df')

    df.rename(columns=columns, inplace=True)

    return df

def get_dummies_from_series_with_list_values(ser):
    '''Get dummies (aka multihot encoding) from a pandas series that contains lists'''
    ser = ser.copy()

    # build dummies
    mlb = MultiLabelBinarizer()
    dummies_col_names = [x for x in mlb.fit(ser).classes_]
    dummies = pd.DataFrame(mlb.fit_transform(ser), columns=dummies_col_names)

    return dummies

def get_rename_dict(year):
    renames = {'2022': {'OpSysProfessional use': 'OpSysProfessionalUse', # no space
                        },
               '2021': {'OpSys': 'OpSysProfessionalUse',
                        },
               '2020': {'DatabaseWorkedWith': 'DatabaseHaveWorkedWith',
                        'DatabaseDesireNextYear': 'DatabaseWantToWorkWith',
                        'LanguageWorkedWith': 'LanguageHaveWorkedWith',
                        'LanguageDesireNextYear': 'LanguageWantToWorkWith',
                        'NEWCollabToolsWorkedWith': 'NEWCollabToolsHaveWorkedWith',
                        'NEWCollabToolsDesireNextYear': 'NEWCollabToolsWantToWorkWith',
                        'OpSys': 'OpSysProfessionalUse',
                        'PlatformWorkedWith': 'PlatformHaveWorkedWith',
                        'PlatformDesireNextYear': 'PlatformWantToWorkWith',
                        'MiscTechWorkedWith': 'MiscTechHaveWorkedWith',
                        'MiscTechDesireNextYear': 'MiscTechWantToWorkWith',
                        'WebframeWorkedWith': 'WebframeHaveWorkedWith',
                        'WebframeDesireNextYear': 'WebframeWantToWorkWith',
                        },
               '2019': {'DatabaseWorkedWith': 'DatabaseHaveWorkedWith',
                        'DatabaseDesireNextYear': 'DatabaseWantToWorkWith',
                        'LanguageWorkedWith': 'LanguageHaveWorkedWith',
                        'LanguageDesireNextYear': 'LanguageWantToWorkWith',
                        'OpSys': 'OpSysProfessionalUse',
                        'PlatformWorkedWith': 'PlatformHaveWorkedWith',
                        'PlatformDesireNextYear': 'PlatformWantToWorkWith',
                        'MiscTechWorkedWith': 'MiscTechHaveWorkedWith',
                        'MiscTechDesireNextYear': 'MiscTechWantToWorkWith',
                        'WebFrameWorkedWith': 'WebframeHaveWorkedWith',
                        'WebFrameDesireNextYear': 'WebframeWantToWorkWith',},
               '2018': {'DatabaseWorkedWith': 'DatabaseHaveWorkedWith',
                        'DatabaseDesireNextYear': 'DatabaseWantToWorkWith',
                        'LanguageWorkedWith': 'LanguageHaveWorkedWith',
                        'LanguageDesireNextYear': 'LanguageWantToWorkWith',
                        'OperatingSystem': 'OpSysProfessionalUse',
                        'PlatformWorkedWith': 'PlatformHaveWorkedWith',
                        'PlatformDesireNextYear': 'PlatformWantToWorkWith',},
               }
    rename_dict = renames[year]

    return rename_dict

def convert_col_values_to_lists(ser, delimiter=';'):
    '''
    :param ser: series that is the column of a dataframe containing list-like values
    :param delimiter: delimiter dividing elements of the list_like values in the series
    '''
    ser = ser.str.split(delimiter)

    return ser

def fillna_col_of_lists(ser):
    '''
    :param ser: series that is the column of a dataframe
    '''
    # fill nas with an empty list, standard methods like fillna dont allow this
    ser = ser.fillna('').apply(list)

    return ser

def assert_no_duplicate_cols(df):
    dupe_cols = df.columns[df.columns.duplicated()]
    if len(dupe_cols) > 0:
        print(dupe_cols)
        raise Exception(f'Duplicate columns exist in the dataframe: {dupe_cols}')

def convert_to_list_if_not_list(obj):
    if (type(obj) != list) & (obj != None):
        out = [obj]
    else:
        out = obj
    return out

def safe_merge(left, right, how='inner', on=None, left_on=None, right_on=None,
               left_index=False, right_index=False, sort=False,
               suffixes=None, copy=True, indicator=False, validate=None):
    '''Pandas merge wrapper with safety features and forced explicitness
       Features:
            - Suffixes defaults to None. If None and there are non-merge-key columns
              with the same name in left and right, throws an error  '''
    assert_no_duplicate_cols(left)
    assert_no_duplicate_cols(right)

    # convert "ons" to lists for easier handling

    on = convert_to_list_if_not_list(on)
    left_on = convert_to_list_if_not_list(left_on)
    right_on = convert_to_list_if_not_list(right_on)

    if ((left_on is not None) & (right_on is None)) | ((right_on is not None) & (left_on is None)):
        raise Exception('If left_on or right_on is specified, the other must be as well')

    if on is None and left_on is None:
        raise Exception('Either on must be specified or left_on/right_on must be specified')

    if on is not None and left_on is not None:
        raise Exception('on and left_on/right_on cannot be specified simultaneously')

    if on:
        left_non_merge_keys = [x for x in left if x not in on]
        right_non_merge_keys = [x for x in right if x not in on]
    else:
        left_non_merge_keys = [x for x in left if x not in left_on]
        right_non_merge_keys = [x for x in right if x not in right_on]


    shared_non_merge_keys = [x for x in left_non_merge_keys if x in right_non_merge_keys]
    if suffixes == None:
        if len(shared_non_merge_keys) > 0:
            raise Exception(f'Suffixes argument is None but the following '
                            f'non-merge-key columns are shared between the '
                            f'left and right dataframes: {shared_non_merge_keys}')
        # change suffixes to the default for pandas to minimize dependency
        # on merge api: at this point suffixes won't be doing anything
        suffixes = ('_x', '_y')

    out = pd.merge(left, right, how, on, left_on, right_on, left_index,
                   right_index, sort, suffixes, copy, indicator, validate)

    return out

    # categories = df[col].str.split(';').explode().unique().tolist()
    # for null_val in [np.nan, None]:
    #     if null_val in categories:
    #         categories.remove(null_val)
    # for category in categories:
    #     msk = df[col].str.contains(category)

def transpose_to_long_year_wide_category(plot_df):
    # pivot to be wide by category and long by year
    plot_df = plot_df.transpose().reset_index()

    # move column headers out of the first row
    plot_df.columns = plot_df.head(1).values.tolist()[0]
    plot_df.drop(axis=0, index=0, inplace=True)
    plot_df.rename(columns={'category': 'year'}, inplace=True)

    return plot_df

def get_report_cols():
    report_cols = \
        [
            # 'Accessibility',
            # 'Age',
            # 'Blockchain',
            # 'BuyNewTool',
            # 'CodingActivities',
            # 'CompFreq',
            # 'CompTotal',
            # 'ConvertedCompYearly',
            # 'Country',
            # 'Currency',
            'DatabaseHaveWorkedWith',
            'DatabaseWantToWorkWith',
            # 'DevType',
            # 'EdLevel',
            # 'Employment',
            # 'Ethnicity',
            # 'Frequency_1',
            # 'Frequency_2',
            # 'Frequency_3',
            # 'Gender',
            # 'ICorPM',
            # 'Knowledge_1',
            # 'Knowledge_2',
            # 'Knowledge_3',
            # 'Knowledge_4',
            # 'Knowledge_5',
            # 'Knowledge_6',
            # 'Knowledge_7',
            'LanguageHaveWorkedWith',
            'LanguageWantToWorkWith',
            # 'LearnCode',
            # 'LearnCodeCoursesCert',
            # 'LearnCodeOnline',
            # 'MainBranch',
            # 'MentalHealth',
            'MiscTechHaveWorkedWith',
            'MiscTechWantToWorkWith',
            'NEWCollabToolsHaveWorkedWith',
            'NEWCollabToolsWantToWorkWith',
            # 'NEWSOSites',
            # 'OfficeStackAsyncHaveWorkedWith',
            # 'OfficeStackAsyncWantToWorkWith',
            # 'OfficeStackSyncHaveWorkedWith',
            # 'OfficeStackSyncWantToWorkWith',
            # 'Onboarding',
            # 'OpSysPersonal use',
            'OpSysProfessionalUse',
            # 'OrgSize',
            'PlatformHaveWorkedWith',
            'PlatformWantToWorkWith',
            # 'ProfessionalTech',
            # 'PurchaseInfluence',
            # 'RemoteWork',
            # 'ResponseId',
            # 'SOAccount',
            # 'SOComm',
            # 'SOPartFreq',
            # 'SOVisitFreq',
            # 'Sexuality',
            # 'SurveyEase',
            # 'SurveyLength',
            # 'TBranch',
            # 'TimeAnswering',
            # 'TimeSearching',
            # 'ToolsTechHaveWorkedWith',
            # 'ToolsTechWantToWorkWith',
            # 'Trans',
            # 'TrueFalse_1',
            # 'TrueFalse_2',
            # 'TrueFalse_3',
            # 'VCHostingPersonal use',
            # 'VCHostingProfessional use',
            # 'VCInteraction',
            # 'VersionControlSystem',
            'WebframeHaveWorkedWith',
            'WebframeWantToWorkWith',
            # 'WorkExp',
            # 'YearsCode',
            # 'YearsCodePro'
         ]

    return report_cols

def get_report_cols_subset(report_cols, year):
    report_cols = report_cols.copy()
    if year < '2020':
        for col in ['NEWCollabToolsHaveWorkedWith', 'NEWCollabToolsWantToWorkWith']:
            report_cols.remove(col)
    if year < '2019':
        for col in ['MiscTechHaveWorkedWith', 'MiscTechWantToWorkWith',
                    'WebframeHaveWorkedWith', 'WebframeWantToWorkWith',]:
            report_cols.remove(col)

    return report_cols

max_survey_year = 2022
survey_raw_data_dir = get_full_path('~/Documents/stack_overflow_survey_data/')
os.makedirs(survey_raw_data_dir, exist_ok=True)

download_and_extract_raw_data(max_survey_year, survey_raw_data_dir)

report_years = ['2019', '2020', '2021', '2022']
report_cols = get_report_cols()
fig_dict = {}
for col in report_cols:
    plot_df = pd.DataFrame()
    for year in report_years:
        print(f'Prepping {col} data for {year}')
        df = pd.read_csv(f'{survey_raw_data_dir}{year}/survey_results_public.csv', dtype=str)
        df = safe_rename(df, get_rename_dict(year))
        keep_cols = get_report_cols_subset(report_cols, year)
        df = df[keep_cols].copy()
        if col not in df:
            continue
        df[col] = convert_col_values_to_lists(df[col])
        df[col] = fillna_col_of_lists(df[col])
        dummies = get_dummies_from_series_with_list_values(df[col])
        n_responses = len(dummies)
        sum_df = pd.DataFrame(dummies.sum()).reset_index().rename(columns={'index': 'category', 0: str('pct')})

        # convert absolute numbers to percentages
        sum_df['pct'] = sum_df['pct'] / n_responses * 100
        sum_df['year'] = year

        if len(plot_df) == 0:
            plot_df = sum_df
        else:
            plot_df = pd.concat([plot_df, sum_df])

    fig = px.line(plot_df, x='year', y='pct', color='category', title=col, markers=True)
    fig_dict[col] = fig

output_path = '/home/amundy/Desktop/test.html'
if os.path.exists(output_path):
    os.remove(output_path)
with open(output_path, 'a') as report:
    for col, fig in fig_dict.items():
        fig.write_html(report, full_html=False)

print('Done writing report')
