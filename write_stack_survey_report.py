import os
import pandas as pd
import requests
import zipfile
import io

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
        res = requests.get(url)
        if not res.ok:
            raise Exception(f'The following zip file url failed to download: {url}')

        # convert to zipfile object
        zip_file = zipfile.ZipFile(io.BytesIO(res.content))

        # write
        os.makedirs(out_dir)
        zip_file.extractall(out_dir)
        print(f'done downloading and extracting survey results for {year}')

max_survey_year = 2022
survey_raw_data_dir = get_full_path('~/Documents/stack_overflow_survey_data/')
os.makedirs(survey_raw_data_dir, exist_ok=True)

download_and_extract_raw_data(max_survey_year, survey_raw_data_dir)



