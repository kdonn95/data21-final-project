import boto3
import pandas as pd
import sqlalchemy


class TextFilePipeline:

    def __init__(self, database):
        # Setting up connection to sql server.
        server = 'localhost,1433'
        user = 'SA'
        password = 'Passw0rd2018'
        driver = 'SQL+Server'
        self.engine = sqlalchemy.create_engine(f"mssql+pyodbc://{user}:{password}@{server}/{database}?driver={driver}")

        # Connecting to the sql server.
        connection = self.engine.connect()

        # Setting up connection to s3 resources from boto3.
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')

    def get_txt_file_key_list(self, bucket_name):
        # Extract all objects in the bucket specified by the user.
        files = self.s3_resource.Bucket(bucket_name).objects.all()

        # Creating an empty list to store the text file objects.
        txt_files = []

        # Looking through every object in the bucket and getting only the txt files.
        for file in files:
            if ".txt" == file.key[-4:]:
                txt_files.append(file.key)
        return txt_files

    def update_candidate_id(self, data_frame):
        # Get list of names of candidates entered in the candidate table.
        names_list = list(self.engine.execute('SELECT candidate_name FROM candidate'))

        # Check to see if candidate has an entry in the candidate table.

        for index in list(data_frame.index):
            name = data_frame.loc[index, "name"]

            # If the name isn't in the candidate table, an entry in that table will be created for him.
            if name not in names_list:
                df = pd.DataFrame([name], columns=['candidate_name'])
                df.to_sql('candidate', self.engine, if_exists='append', index=False)

            # Updating candidate's ID.
            candidate_id = self.__get_candidate_id(name)
            data_frame.loc[index, "candidate_id"] = candidate_id

    def __get_candidate_id(self, name:str):
        # Get list of candidate names and ids.
        id_name_list = list(self.engine.execute('SELECT candidate_id, candidate_name FROM candidate'))

        # Finds the candidate id and returns it.
        for row in id_name_list:
            if name == row[1]:
                return row[0]

    def load_data_into_sql(self, data_frame):
        data_frame.to_sql("test7", self.engine, if_exists='append', index=False)  # Need to change table name in the future.

    def text_to_dataframe(self, bucket_name, key):
        # Getting the text file object from s3.
        s3_object = self.s3_client.get_object(
            Bucket=bucket_name,
            Key=key
        )

        # Reformatting the text file to get a list of string values
        contents = s3_object['Body'].read()
        text_lines_list = contents.decode("utf-8").split('\r\n')

        # Reformatting into strings into different fields
        extraction_to_list = self.__formatting(text_lines_list)
        list_lists = self.__list_of_lists(extraction_to_list)
        separating_fields = self.__separating_by_comma(list_lists)

        # Loading the data into a dataframe
        df = pd.DataFrame(separating_fields, columns=['Name', 'Psychometric',
                                                      'Psychometric Score', 'Max Psychometric score',
                                                      'Presentation', ' Presentation Score', 'Max Presentation Score'])
        # Droping columns which aren't needed.
        df.drop(df.columns[[1, 4]], axis=1, inplace=True)

        return df

    def __formatting(self, file_body):
        # Replacing specific symbols and replacing them to commas to separate fields.
        dashseperation = [item.replace("-", ",") for item in file_body]
        slashseperation = [item.replace("/", ",") for item in dashseperation]
        colonseperation = [item.replace(":", ",") for item in slashseperation]

        # Removal of the location and date headers
        minustopthreelines = colonseperation[3:]

        # Removing the last line if it is an empty string
        if minustopthreelines[-1] == '':
            minustopthreelines.remove(minustopthreelines[-1])
        return minustopthreelines

    def __list_of_lists(self, long_list):
        # Separating one long list into a list of lists.
        separated_list = []
        for index in long_list:
            temp_list = [index]
            separated_list.append(temp_list)
        return separated_list

    def __separating_by_comma(self, list_of_str_lists):
        # Separating each list index into it's separate fields.
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


# ---
txt_file = TextFilePipeline("testing")

list_of_text_files = txt_file.get_txt_file_key_list("data21-final-project")

for x in list_of_text_files[0:2]:
    data_frame = txt_file.text_to_dataframe("data21-final-project", x)
    txt_file.load_data_into_sql(data_frame)
