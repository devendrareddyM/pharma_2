"""
File                :   csv_upload.py

Description         :   This will upload csv/xml files for the selected algorithm

Author              :   LivNSense Technologies

Date Created        :   06-01-2020

Date Last modified  :   06-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import ast
import json
import traceback
import time
from xml.etree import ElementTree
import joblib
import jwt
from django.utils.html import escape
import pandas
from cassandra.query import BatchStatement, SimpleStatement
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
# from utilities.Configuration import FLAG
from ApplicationInterface.Database.Configuration import NAME, TABLE_NAME
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    POST_REQUEST, UPLOADED_SUCCESSFULLY, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _CassandraConnection, _TokenValidation
import os.path
import xml.etree.cElementTree as et
import xmltodict
import ModelMaker


class UploadAlgorithmFile(_CassandraConnection):
    """
    This class is responsible for uploading the csv files for the algorithm
    """

    def __init__(self, files, algo_name, description, file_names):
        """
        :param algo_name : this will have the algorithm name
        :param files : this will have the csv files
        :param description : this will have the description data
        This will call the parent class to validate the connection and initialize the values
        """

        super().__init__()
        self.files = files
        self.algo_name = algo_name
        self.description = json.loads(description)
        self.file_names = ast.literal_eval(file_names)

    def upload_file(self):
        """
        This will upload all the selected csv/xml files in the database for the given algorithm.
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            batch = BatchStatement()
            error = False
            error_message = None
            FLAG = 0
            algo_tag = "TMT"
            """if not self.files:
                error = True
                error_message = "No files to upload"

            file_names_list = self.file_names
            # select_query = SELECT_ALGORITHM_NAME_QUERY.format(self.algo_name, ",".join(map(lambda x: "'" + x + "'", file_names_list)))
            select_query = "SELECT algorithm_name from csv.configuration_details WHERE algorithm_name = '{}' ALLOW FILTERING".format(
                self.algo_name)
            result_set = self._csql_session.execute(select_query)

            if result_set[0]['count'] == 0 or result_set[0]['count'] < len(file_names_list):
                error_message = "Please give the existing algorithm or file name"
                return JsonResponse({MESSAGE_KEY: error_message}, status=HTTP_500_INTERNAL_SERVER_ERROR)"""


            for file in self.files:

                """if file.name not in self.file_names:
                    error = True
                    error_message = "Uploaded file name("+file.name+") not found in given file name list"
                    break"""

                description = None
                if file.name in self.description:
                    description = self.description[file.name]
                    # print(self.description[f.name])
                LAST_MODIFIED_DATE = str(round(time.time() * 1000))
                #file_data = str(file.read(), 'utf-8')

                extension = os.path.splitext(file.name)[1]
                json_data = ""
                if extension == ".csv":
                    file_data = pandas.read_csv(file, encoding='ISO-8859-1')
                    json_data = file_data.to_json()
                elif extension == ".xml":
                    file_data = et.parse(file)
                    xml_str = ElementTree.tostring(file_data.getroot(), encoding='unicode')
                    json_data = json.dumps(xmltodict.parse(xml_str))
                elif extension == ".joblib":

                    #json_data = joblib.load((os.path.realpath(file.name())))
                    #print(json_data)
                    #print(file.)
                    json_datas = joblib.load(file)
                    # json_datas = pickle.load(open(file, "rb"))
                    print(json_datas)

                    json_data = escape(str(json_datas))
                    print(json_data)
                    # json_data = json.dumps(file)
                    # json_datas = joblib.load(file)
                    # json_datas = pickle.load(open(file, "rb"))
                    # print(json_datas)

                    # json_data = escape(str(json_datas))
                    # print(json_data)
                    # json_data = file
                    #print(json_data)
                    #print("ggggggg")
                    #json_data = file.read().decode()

                # insert query into cassandra table
                # insert_query = """ INSERT into csv.configuration_details (algorithm_name, file_param_name, description, """ \
                #                """file_content, last_modified_date, type, flag) values ('{}', '{}', '{}', {}, {}, 2, {})""".format(
                #                                                                                 self.algo_name,
                #                                                                                 file.name,
                #                                                                                 description,
                #                                                                                 "textAsBlob('" + json_data + "')",
                #                                                                                 LAST_MODIFIED_DATE,
                #                                                                                 FLAG)

                insert_query = """ INSERT into {}.{} (algorithm_name, file_param_name, description, """ \
                               """file_content, last_modified_date, type, flag, algo_tag) values ('{}', '{}', '{}', {}, {}, 2, {}, '{}')""".format(NAME, TABLE_NAME,
                    self.algo_name,
                    file.name,
                    description,
                    "textAsBlob('" + json_data + "')",
                    LAST_MODIFIED_DATE,
                    FLAG,
                    algo_tag)

                # batch insert initialisation
                batch.add(SimpleStatement(insert_query))

            if error is True:
                return JsonResponse({MESSAGE_KEY: error_message}, status=HTTP_500_INTERNAL_SERVER_ERROR)

            self._csql_session.execute(batch, timeout=200.0)
            return JsonResponse({MESSAGE_KEY: UPLOADED_SUCCESSFULLY}, safe=False)

        except AssertionError as e:
            print(e)
            log_error(e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            print("Error while inserting data - " + str(e))
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def upload_algorithm_file(request, algorithm_name):
    """
    This function will upload csv and xml files, file name and description and will return error if generated.
    :param request: request django object
    :param algorithm_name : this will have the algorithm name
    :return: json response
    """
    obj = None

    try:
        if request.method == POST_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = UploadAlgorithmFile(request.FILES.getlist('files'), algorithm_name,
                                          request.POST.get('description'),
                                          request.POST.get('file_names'))
                return obj.upload_file()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        role = jwt.decode(token, verify=False)
        ob = HashingSalting()
        if role['role'] == 'Admin':
            ob.decreasing_admin_login_count()
        if role['role'] == 'Non Admin':
            ob.decreasing_Non_Admin_login_count()
        if role['role'] == 'Super Admin':
            ob.decreasing_super_Admin_login_count()
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)

    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
