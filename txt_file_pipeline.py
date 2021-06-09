import boto3
from pprint import pprint
import pandas as pd

# Loading in a specific file for the use of testing
s3_client = boto3.client('s3')
bucket_list = s3_client.list_buckets()
buckets = bucket_list['Buckets']
bucket_name = 'data21-final-project'

s3_object = s3_client.get_object(
    Bucket=bucket_name,
    Key='Talent/Sparta Day 1 August 2019.txt'
)

# Reformatting the tet file to get a list of string values
contents = s3_object['Body'].read()
text_lines_list = contents.decode("utf-8").split('\r\n')


# pprint(text_lines_list)


# Reformatting into strings into different fields
def formatting(file_body):
    # Replacing specific symbols and replacing them to commas to separate fields
    dashseperation = [item.replace("-", ",") for item in file_body]
    slashseperation = [item.replace("/", ",") for item in dashseperation]
    colonseperation = [item.replace(":", ",") for item in slashseperation]
    # Removal of the location and date headers
    minustopthreelines = colonseperation[3:]
    # Removing the last line if it is an empty string
    if minustopthreelines[-1] == '':
        minustopthreelines.remove(minustopthreelines[-1])
    return minustopthreelines


# Separating one long list into a list of lists
def list_of_lists(long_list):
    separated_list = []
    for index in long_list:
        temp_list = [index]
        separated_list.append(temp_list)
    return separated_list


# Separating each list index into it's separate fields
def separating_by_comma(list_of_str_lists):
    comma_sep = []
    for index in list_of_str_lists:
        index = str(index)
        index = index.strip()
        index = index.split(',')
        index = [item.replace("'", "") for item in index]
        index = [item.replace("[", "") for item in index]
        index = [item.replace("]", "") for item in index]
        comma_sep.append(index)
    return comma_sep


#
extraction_to_list = formatting(text_lines_list)
list_lists = list_of_lists(extraction_to_list)
separating_fields = separating_by_comma(list_lists)
# pprint(separating_fields)

# Loading the data into a dataframe
df = pd.DataFrame(separating_fields, columns=['Name', 'Psychometric',
                                              'Psychometric Score', 'Max Psychometric score',
                                              'Presentation', ' Presentation Score', 'Max Presentation Score'])
df.drop(df.columns[[1, 4]], axis=1, inplace=True)


# print(df)


def rem_whitespace(formatted_dataset):
    for line in formatted_dataset:
        for index in line:
            index.strip()
    return formatted_dataset


# def name_separated(formatted_dataset):
#     separate_names = []
#     for index in formatted_dataset:
#         index[0] = [item.replace(" ", ",") for item in index[0]]
#         print(index[0])

test = rem_whitespace(separating_fields)
pprint(test)