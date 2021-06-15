import boto3
import pandas as pd
from datetime import datetime
from app.classes.logger import Logger
import re


class TextFilePipeline(Logger):
    def __init__(self, engine, logging_level):
        # Initializing Logger.
        Logger.__init__(self, logging_level)

        # Setting up connection to sql server.
        self.engine = engine

        # Connecting to the sql server.
        connection = self.engine.connect()

        # Setting up connection to s3 resources from boto3.
        self.s3_resource = boto3.resource('s3')
        self.s3_client = boto3.client('s3')

    def get_txt_file_key_list(self, bucket_name):
        self.log_print("Connecting to bucket to get keys.", "INFO")

        # Extract all objects in the bucket specified by the user.
        files = self.s3_resource.Bucket(bucket_name).objects.all()

        # Creating an empty list to store the text file objects.
        txt_files = []

        # Looking through every object in the bucket 
        # and getting only the txt files.
        for file in files:
            if ".txt" == file.key[-4:]:
                txt_files.append(file.key)
        return txt_files

    def update_candidate_id(self, data_frame):
        self.log_print("Starting process to change IDs.", "INFO")
        # Get list of names of candidates entered in the candidate table.
        names_list = [i[0] for i in list(self.engine.execute("""
                                                            SELECT candidate_name 
                                                            FROM candidate
                                                            """))]

        # Check to see if candidate has an entry in the candidate table.
        for index in list(data_frame.index):
            name = data_frame.loc[index, "Name"].title()
            self.log_print(name, "DEBUG")

            # If the name isn't in the candidate table, 
            # an entry in that table will be created for them.
            if name not in names_list:
                df = pd.DataFrame([name], columns=['candidate_name'])
                df.to_sql('candidate', self.engine, 
                            if_exists='append', index=False)

            # Updating candidate's ID in the .
            candidate_id = self.__get_candidate_id(name)
            data_frame.loc[index, "candidate_id"] = candidate_id

        # Removes the Name column from the data frame 
        # as it is no longer needed.
        data_frame = data_frame.drop(columns="Name")
        self.log_pprint(data_frame, "DEBUG")
        return data_frame

    def transform_string_to_int(self, data_frame, columns_list):
        # Takes a dataframe with numbers as strings 
        # and converts them to an int type.
        for index in list(data_frame.index):
            for column_name in columns_list:
                data_frame.loc[index, column_name] = int(data_frame.loc[index, 
                                                                column_name].replace('"', ''))
        return data_frame

    def __get_candidate_id(self, name: str):
        # Get list of candidate names and ids.
        id_name_list = list(self.engine.execute("""
                                                SELECT candidate_id, candidate_name FROM candidate
                                                """))

        # Finds the candidate id and returns it.
        for row in id_name_list:
            if name == row[1]:
                self.log_print(row[0], "DEBUG")
                return row[0]

    def text_to_dataframe(self, bucket_name, key):
        # Getting the text file object from s3.
        s3_object = self.s3_client.get_object(
            Bucket=bucket_name,
            Key=key
        )

        # Reformatting the text file to get a list of string values
        contents = s3_object['Body'].read()
        text_lines_list = contents.decode("utf-8").split('\r\n')

        # Extracting the location and date of the tests and presentation.
        loc_date = self.__get_date_location(text_lines_list)

        # Reformatting into strings into different fields
        extraction_to_list = self.__formatting(text_lines_list)
        self.log_pprint(extraction_to_list, "DEBUG")

        list_lists = self.__list_of_lists(extraction_to_list)
        self.log_pprint(list_lists, "DEBUG")

        separating_fields = self.__separating_by_comma(list_lists)
        self.log_pprint(separating_fields, "DEBUG")

        final_list = self.__inserting_loc_date(loc_date, separating_fields)
        self.log_pprint(final_list, "DEBUG")

        # If the name had a hyphen in the middle of it, it is now joined back together.
        for row_index in range(len(final_list)):
            if len(final_list[row_index]) != 10:
                self.log_print(final_list[row_index], "DEBUG")
                final_list[row_index] = [final_list[row_index][0] + "-" + final_list[row_index][4]] + \
                                        final_list[row_index][1:4] + \
                                        final_list[row_index][5:]

        # Loading the data into a dataframe
        df = pd.DataFrame(final_list, columns=[
                                            'Name', 
                                            'candidate_id', 
                                            'date', 
                                            'location_id',
                                            'Psychometric',
                                            'psychometrics', 
                                            'psychometrics_max',
                                            'Presentation', 
                                            'presentation', 
                                            'presentation_max'
                                            ])
        self.log_pprint(df, "DEBUG")

        # Dropping columns which aren't needed.
        df.drop(df.columns[[4, 7]], axis=1, inplace=True)
        self.log_pprint(df, "DEBUG")
        return df

    def __formatting(self, file_body):
        # Replacing specific symbols and replacing them 
        # to commas to separate fields.
        dashseperation = [item.replace("- ", ",") for item in file_body]
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
            index = index[2:-2]
            index = index.split(',')
            index = [item.strip() for item in index]
            index = [re.sub("['`´‘’]", "'", item) for item in index]
            index = [item.replace("' ", "'") for item in index]
            index = [item.replace(" '", "'") for item in index]
            index = [item.replace("' ", "'") for item in index]
            comma_sep.append(index)
        return comma_sep

    def __get_date_location(self, file_body):
        new_list = []
        # Separating location and date
        top_two_lines = file_body[:2]
        date_str = top_two_lines[0]
        location = top_two_lines[1].title()

        # Converting the string into a datetime format
        date_str = date_str.split(' ', 1)[1]
        date = datetime.strptime(date_str, '%d %B %Y')
        date = date.strftime('%Y-%m-%d')

        # Inserting date and location back into a list
        new_list.append(date)
        new_list.append(self.__get_location_id(location))
        return new_list

    def __get_location_id(self, location):
        self.log_print("Starting to get location_id", "DEBUG")

        # Getting a list of all locations in the location table.
        location_list = [i[0] for i in list(self.engine.execute("""
                                            SELECT location
                                            FROM location
                                            """))]
        self.log_print(location_list, "DEBUG")

        # Creating the location information if not available  in the location table.
        if location not in location_list:
            self.log_print("Starting to insert a new location in the location table.", "DEBUG")
            self.engine.execute(f"""
            INSERT INTO location (location)
            VALUES ('{location}')""")

        self.log_print("Finished inserting a new location in the location table.", "DEBUG")

        # Return the location id.
        location_id = list(self.engine.execute(f""" SELECT location_id 
                                                    FROM location 
                                                    WHERE location = '{location}'"""))[0][0]
        self.log_print(location_id, "DEBUG")
        return location_id

    def __inserting_loc_date(self, date_loc, other_fields):
        for line in other_fields:
            line.insert(1, date_loc[0])
            line.insert(2, date_loc[1])
            line.insert(1, 0)
        return other_fields

    def upload_all_txt_files(self, bucket_name):

        # Getting a list of keys with all text files in the s3 bucket.
        list_of_text_files = self.get_txt_file_key_list(bucket_name)
        self.log_print(list_of_text_files, "DEBUG")

        # Gathering data, transforming and uploading in to sql database, one at a time.
        for text_file in list_of_text_files:  # NEEDS CHANGING
            self.log_print(str(text_file), "INFO")

            # Converting Text file in a data frame.
            data_frame = self.text_to_dataframe(bucket_name, text_file)
            self.log_pprint(data_frame, "DEBUG")

            # Transforming the dataframe by converting numbers to int, and adding the Candidate ID.
            data_frame = self.transform_string_to_int(data_frame, ['psychometrics', 'psychometrics_max',
                                                                                'presentation', 'presentation_max'])
            self.log_pprint(data_frame, "DEBUG")

            data_frame = self.update_candidate_id(data_frame)
            self.log_pprint(data_frame, "DEBUG")

            data_frame = self.update_data_in_database(data_frame)
            self.log_print("Finished updating rows into sql database.", "INFO")

            self.load_data_into_sql(data_frame)
            self.log_print("Finished uploading rows into sql database.", "INFO")

    def update_data_in_database(self, data_frame):
        # Extracting dates from the sql database from sparta day table.
        sql_dates = [i[0] for i in list(self.engine.execute("""
                                                            SELECT DISTINCT date
                                                            FROM sparta_day
                                                            """))]
        # Extracting date from data_frame:
        df_date = data_frame['date'].head(1)[0]

        # Checking to see if any entries for the current date is in the database.
        if df_date in sql_dates:
            # Getting a list of candidate ids who already have an entry for this date.
            sql_ids = [i[0] for i in list(self.engine.execute(f"""
                                                             SELECT candidate_id
                                                             FROM sparta_day
                                                             WHERE date = '{df_date}'
                                                             """))]

            # Creating a list to keep track of which rows to delete.
            index_to_drop = []

            for index in list(data_frame.index):
                if data_frame.loc[index, "candidate_id"] in sql_ids:
                    index_to_drop.append(index)

                    # Updating candidate details in sparta day table.
                    self.engine.execute(f"""
                        UPDATE sparta_day
                        SET
                            location_id = {data_frame.loc[index, "location_id"]},
                            psychometrics = {data_frame.loc[index, "psychometrics"]},
                            psychometrics_max = {data_frame.loc[index, "psychometrics_max"]},
                            presentation = {data_frame.loc[index, "presentation"]},
                            presentation_max = {data_frame.loc[index, "presentation_max"]}
                        WHERE candidate_id = '{data_frame.loc[index, "candidate_id"]}' 
                        AND date = '{data_frame.loc[index, "date"]}'
                        """)

            # Removing the rows which which had have been entered into the database.
            df = data_frame.drop(index_to_drop)
        else:
            df = data_frame

        return df

    def load_data_into_sql(self, data_frame):
        # Loads dataframe into sql database.
        if len(data_frame) > 0:
            data_frame.to_sql("sparta_day", self.engine, if_exists='append', index=False)



