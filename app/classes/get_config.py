from configparser import ConfigParser
import os


class GetConfig():
    def __init__(self):
        config_file = os.path.join(
                                    os.path.dirname(
                                        os.path.dirname(
                                            os.path.dirname(__file__)   
                                            )
                                        ), 
                                    f'config.ini')

        __config = ConfigParser()
        __config.read(config_file)

        self.server = __config['SERVER DETAILS']['SERVER']
        self.database = __config['SERVER DETAILS']['DATABASE']
        self.user = __config['SERVER DETAILS']['USER']
        self.driver = __config['SERVER DETAILS']['DRIVER']
        
        if __config['SERVER DETAILS']['PASSWORD'] == '.\password.ini':
            password_file = os.path.join(
                                    os.path.dirname(
                                        os.path.dirname(
                                            os.path.dirname(__file__)   
                                            )
                                        ), 
                                    f'password.ini')
            __config.read(password_file)

        self.password = __config['SERVER DETAILS']['PASSWORD']

        self.logging_level = __config['LOGGING DETAILS']['LOGGING LEVEL']

        self.s3_bucket = __config['S3 DETAILS']['BUCKET NAME']
