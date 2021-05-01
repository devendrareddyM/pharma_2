"""
File                :   file_download.py

Description         :   This will download csv/joblib files for the selected algorithm

Author              :   LivNSense Technologies

Date Created        :   12-06-2020

Date Last modified  :   12-06-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import json
import os
import traceback
import zipfile
from html import unescape

import jwt
from xml.dom.minidom import parseString
import dicttoxml
import joblib
import pandas as pd
from django.http import JsonResponse, HttpResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Configuration import NAME, TABLE_NAME
from ApplicationInterface.Database.Queries import SINGLE_FILE_DOWNLOAD_QUERY, MULTIPLE_FILES_DOWNLOAD_QUERY
from ApplicationInterface.Database.Utility import _CassandraConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_403_FORBIDDEN, \
    json_MethodNotAllowed, HTTP_401_UNAUTHORIZED
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    GET_REQUEST, FILES_NAME_REQUEST, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class DownloadAlgorithmFile(_CassandraConnection):
    """
    This class is responsible for downloading the csv files for the algorithm
    """

    def __init__(self, algo_name, query_params):
        """
        :param algo_name : this will have the algorithm name
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.algorithm_name = algo_name
        self.query_params = query_params

    """
    Download the zip file with csv/xml  
    """

    def download_file(self):
        """
        This will download all the zip files (contains xml/csv) for the selected algorithm.
        :return: zip file to download
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            files = self.query_params["files"].split(",")

            if len(files) == 1:
                query = SINGLE_FILE_DOWNLOAD_QUERY.format(NAME, TABLE_NAME, self.algorithm_name, files[0])
            else:
                query = MULTIPLE_FILES_DOWNLOAD_QUERY.format(NAME, TABLE_NAME, self.algorithm_name, tuple(files))

            result_set = self._csql_session.execute(query)
            result_set_list = list(result_set)

            if len(result_set_list) != len(files):
                return JsonResponse("Please enter the correct file names", safe=False)

            df_data = pd.DataFrame(result_set_list)

            """
            Creates Zip File
            """
            file_to_download = self.algorithm_name + ".zip"
            zip_object = zipfile.ZipFile(file_to_download, "w")

            """
            Iterate each file(row entry from DB) and creates csv, xml files inside zip file with the file name  
            """

            for index, row in df_data.iterrows():

                extension = os.path.splitext(row['file_param_name'])[1]
                if extension == ".csv":
                    df = pd.read_json(row['value'])
                    zip_object.writestr(row['file_param_name'], df.to_csv(index=False))
                elif extension == ".xml":
                    obj = json.loads(row['value'])
                    xml = dicttoxml.dicttoxml(obj, root=False, attr_type=False)
                    xml.partition(b'<?xml version="1.0" encoding="UTF-8" ?>')  # added the xml version
                    dom = parseString(xml)
                    zip_object.writestr(row['file_param_name'], dom.toprettyxml())
                elif extension == ".joblib":
                    #obj = json.loads(unescape(row['value']))
                    value = unescape(row['value'])
                    # print(value)
                    # print(joblib.dump(value, "hgi.joblib"))
                    # file_name = os.path.splitext(row['file_param_name'])[0]
                    # print(joblib.dump(bytes(value, 'utf-8'), row['file_param_name']))
                    # zip_object.writestr(row['file_param_name'], joblib.dump(value, row['file_param_name']))
                    # n = os.path.splitext(row['file_param_name'])[0]
                    # a = n+'.sav'
                    print(joblib.dump(value, row['file_param_name']))
                    # zip_object.writestr(row['file_param_name'], joblib.dump(value, a))
                    zip_object.writestr(row['file_param_name'], value)
                    # for row in modelDetails.index:
                    #     functionName = str(modelDetails.loc[row, 'functionName'])
                    #     modelName = str(modelDetails.loc[row, 'modelName']) + ".joblib"
                    #     modelPath = os.path.join(self.folder, "lib", modelName)
                    #     model = joblib.load(modelPath)
                    #     self.savedModels[functionName] = model
            #         , joblib.dump(value, row['file_param_name'])

            zip_object.close()

            """Download Created Zip File"""

            with open(file_to_download, 'rb') as fh:
                response = HttpResponse(fh.read(), content_type="application/zip")
                response['Content-Disposition'] = 'attachment; file_name=' + os.path.basename(file_to_download)
                return response

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def download_algorithm_file(request, algorithm_name):
    """
    This function will download csv, xml files and will return error if generated.
    :param request: request django object
    :param algorithm_name : this will have the algorithm name
    :return: json response
    """
    query_params = obj = None
    try:

        query_params = {
            FILES_NAME_REQUEST: request.GET[FILES_NAME_REQUEST]
        }

    except:
        pass
    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            permissions = loggedin_user_details['permissions']['Settings']
            permission_name = 'Upload/Download Configuration Files'
            if permission_name in permissions:
                if loggedin_user_details:
                    obj = DownloadAlgorithmFile(algorithm_name, query_params)
                    return obj.download_file()
            else:
                return JsonResponse({MESSAGE_KEY: "Forbidden Error"}, status=HTTP_403_FORBIDDEN)
        log_debug(METHOD_NOT_ALLOWED)
        return json_MethodNotAllowed
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
