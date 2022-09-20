from functions import *

if __name__ == '__main__':
    USER = PW = 'postgres'
    HOST = 'localhost'
    PORT = 5432
    DB = 'config_db'
    SCHEMA = 'smx'
    file_path = "/Users/omarnour/Downloads/Production_Citizen_SMX.xlsx"
    load_excelFile_to_db(file_path, USER, PW, HOST, DB, SCHEMA, PORT)
