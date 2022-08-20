import os
import pandas as pd
import requests
import zipfile
import io
import numpy as np

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



max_survey_year = 2022
survey_raw_data_dir = get_full_path('~/Documents/stack_overflow_survey_data/')
os.makedirs(survey_raw_data_dir, exist_ok=True)

download_and_extract_raw_data(max_survey_year, survey_raw_data_dir)


# todo split out multiple answers wide with one hot encoding for each language
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

# todo in 2022 primary operating system professional allows multiple options, figure out how to handle


# ['2018', '2019', '2020', '2021', '2022']
for year in ['2022']:
    print(f'Prepping data for {year}')
    df = pd.read_csv(f'{survey_raw_data_dir}{year}/survey_results_public.csv', dtype=str)
    df = safe_rename(df, get_rename_dict(year))
    keep_cols = \
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
            'Gender',
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
            # 'MiscTechHaveWorkedWith',
            # 'MiscTechWantToWorkWith',
            # 'NEWCollabToolsHaveWorkedWith',
            # 'NEWCollabToolsWantToWorkWith',
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
            # 'WebframeHaveWorkedWith',
            # 'WebframeWantToWorkWith',
            # 'WorkExp',
            # 'YearsCode',
            # 'YearsCodePro'
         ]
    if year >= '2020':
        keep_cols += ['NEWCollabToolsHaveWorkedWith', 'NEWCollabToolsWantToWorkWith']
    if year >= '2019':
        keep_cols += ['MiscTechHaveWorkedWith', 'MiscTechWantToWorkWith',
                      'WebframeHaveWorkedWith', 'WebframeWantToWorkWith',]
    df = df[keep_cols].copy()

    col = 'DatabaseHaveWorkedWith'
    df['test'] = df[col]
    df['test'] = df['test'].str.split(';')
    # fill nas with an empty list, standard methods like fillna dont allow this
    msk = df['test'].isnull()
    df.loc[msk, 'test'] = [[]]
    from sklearn.preprocessing import MultiLabelBinarizer
    mlb = MultiLabelBinarizer()
    mlb.fit(df['test'])
    dummies_col_names = [col + '_' + x for x in mlb.classes_]
    dummies = mlb.fit_transform(df['test'], columns=dummies_col_names)
    df = pd.concat([df, dummies], axis=1)

    # categories = df[col].str.split(';').explode().unique().tolist()
    # for null_val in [np.nan, None]:
    #     if null_val in categories:
    #         categories.remove(null_val)
    # for category in categories:
    #     msk = df[col].str.contains(category)

