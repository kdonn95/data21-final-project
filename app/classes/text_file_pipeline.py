import boto3
import pandas
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
        data_frame.to_sql('test', self.engine, if_exists='append', index=False)


txt_file = TextFilePipeline()
txt_list = txt_file.get_txt_file_key_list("data21-final-project")
print(txt_list)

s3_client = boto3.client('s3')

s3_object = s3_client.get_object(
    Bucket="data21-final-project",
    Key=txt_list[0]
)
#

# #print(s3_object)
# #print(s3_object['Body'].read())
# text_lines_list = s3_object['Body'].read().decode("utf-8").split('\r\n')
# print(text_lines_list)
