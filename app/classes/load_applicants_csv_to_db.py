from app.classes.text_file_pipeline import TextFilePipeline
import pandas as pd


class loadApplicantsCSVs(TextFilePipeline):

    def __init__(self,engine, logging_level):
      
      # Initializing TextdFilePipeline.
        TextFilePipeline.__init__(self,engine,logging_level)

    def update_staff_table(self,dataframe):

        staff_list_df = dataframe['staff_name'].unique()
        staff_list_db = [i[0] for i in list(self.engine.execute("""
                                                                SELECT staff_name 
                                                                FROM staff
                                                                """))]
        for name in staff_list_df :
            if name in staff_list_db:
                staff_list_db.remove(name)

        
        for name in staff_list_df:
            self.engine.execute(f'''
                                  INSERT INTO staff(
                                      staff_name,
                                      department
                                  )  
                                  VALUES(
                                      '{name}','talent'
                                  )
                                 ''')
    
    def insert_staff_id(self,dataframe):
        # candidate_table = dataframe['staff_id'] = [0 for i in range (len(dataframe))]
        dataframe['staff_id'] = [0 for i in range (len(dataframe))]
        candidate_table = dataframe

        for index in list(candidate_table.index):
            name = str(candidate_table.loc[index, "staff_name"]).title()
            self.log_print(name, "DEBUG")


            # Updating staff's ID in the .
            staff_id = self.__get_staff_id(name)
            candidate_table.loc[index, "staff_id"] = staff_id

        # Removes the staff_name column from the data frame 
        # as it is no longer needed.
        candidate_table = candidate_table.drop(columns="staff_name")
        self.log_pprint(candidate_table, "DEBUG")
        
        return candidate_table        
        

        
    def __get_staff_id(self,name):

         # Get list of staff names and ids.
        id_name_list = list(self.engine.execute("""
                                                SELECT staff_id, staff_name FROM staff
                                                """))

        # Finds the staff id and returns it.
        for row in id_name_list:
            if name == row[1]:
                self.log_print(row[0], "DEBUG")
                return row[0]
    
    def loading_into_candidate_table(self,dataframe):

        data_frame = dataframe.drop(columns="date")
       
        names_list_db = [i[0] for i in list(self.engine.execute("""
                                                            SELECT candidate_name
                                                            FROM candidate
                                                            """))]

        # Creating a list to keep track of which rows to delete.
        index_to_drop = []

        for index in list(data_frame.index):
            if data_frame.loc[index, "candidate_name"] in names_list_db:
                index_to_drop.append(index)

                if str(data_frame.loc[index, 'dob']) == 'NaT':
                    dob = 'NULL'
                else:
                    dob = "'"+str(data_frame.loc[index, 'dob'])+"'"

                if str(data_frame.loc[index, 'staff_id']) == 'nan':
                    staff_id = 'NULL'
                else:
                    staff_id = data_frame.loc[index, 'staff_id']
                # Updating candidate details in candidate table.
                self.engine.execute(f"""
                    UPDATE candidate
                    SET
                        gender = '{data_frame.loc[index,'gender']}',
                        dob = {dob},
                        email = '{data_frame.loc[index,'email']}',
                        city = '{data_frame.loc[index,'city']}',
                        address = '{data_frame.loc[index,'address']}',
                        postcode = '{data_frame.loc[index,'postcode']}',
                        phone_number = '{data_frame.loc[index,'phone_number']}',
                        uni_name = '{str(data_frame.loc[index,'uni_name']).replace("'", "''")}',
                        degree_result = '{data_frame.loc[index,'degree_result']}',
                        staff_id = {staff_id}
                       
                    WHERE candidate_name = '{str(data_frame.loc[index, 'candidate_name']).replace("'", "''")}' 

                    """)

        # Removing the rows which which had have been entered into the database.
        df = data_frame.drop(index_to_drop)
    
        self.log_pprint(df, 'INFO')

        # Loads dataframe into sql database.
        if len(df) > 0:
            df.to_sql("candidate", self.engine, if_exists='append', index=False)
        self.log_print('done', 'INFO')
    
    def load_into_sparta_day(self,data_frame):

        data_frame = data_frame[['candidate_name','date']][pd.notnull(data_frame['date'])]


        id_date_list = list(self.engine.execute("""
                                                    SELECT candidate_id,date 
                                                    FROM sparta_day
                                                    """))

        data_frame['candidate_id'] = [0 for i in range(len(data_frame))]


        for index in list(data_frame.index):
            name = data_frame.loc[index,'candidate_name']
             # Updating candidate's ID 
            candidate_id = self.get_candidate_id(name)
            data_frame.loc[index, "candidate_id"] = candidate_id

        data_frame = data_frame.drop(columns="candidate_name")

        index_to_drop = []
        for index in list(data_frame.index):

            for record in id_date_list:
                if data_frame.loc[index,'candidate_id'] == record[0] and data_frame.loc[index,'date'] == record[1]:
                    index_to_drop.append(index)
        
        data_frame = data_frame.drop(index_to_drop)

        # Loads dataframe into sql database.
        if len(data_frame) > 0:
            data_frame.to_sql("sparta_day", self.engine, if_exists='append', index=False)

    
    def upload_applicants_csv_to_db(self, dataframe):

        self.update_staff_table(dataframe)

        dataframe = self.insert_staff_id(dataframe) 

        self.loading_into_candidate_table(dataframe)

        self.load_into_sparta_day(dataframe)
        