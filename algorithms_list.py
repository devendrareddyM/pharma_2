"""
File                :   algorithms_list.py

Description         :   This will return all the algorithm names and algorithm data

Author              :   LivNSense Technologies

Date Created        :   12-06-2020

Date Last modified  :   12-06-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Configuration import NAME, TABLE_NAME
from ApplicationInterface.Database.Queries import SELECT_FILE_DATA, \
    SELECT_ALGORITHM_NAME, SELECT_ALGORITHM_STATUS_DATA
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _CassandraConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    RECORDS, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class AllAlgorithmList(_CassandraConnection):
    """
    This class is responsible for getting the data and respond for the algorithm list
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()

    """
    Gets the algorithm data by algorithm_name and type
    """

    def get_algorithms_details(self):

        """
        This will return all the list of the algorithm in json format and algorithm status from the Database .
        :return: Json Responses
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            final_response = {"algorithm_status": self.get_algorithm_status(),
                              "algorithm_name": self.get_algorithm_list()}

            return JsonResponse(final_response, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s", e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])
        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    """
    Method to get the algorithm status
    """

    def get_algorithm_status(self):
        ps_obj = _PostGreSqlConnection()
        file_query = SELECT_ALGORITHM_STATUS_DATA
        ps_obj._psql_session.execute(file_query)
        result_set = ps_obj._psql_session.fetchall()
        df = pd.DataFrame(result_set)
        if not df.empty:
            df = df.where(pd.notnull(df) == True, None)
            algorithm_data = yaml.safe_load(df.to_json(orient=RECORDS))
        else:
            algorithm_data = []
        return algorithm_data

    """
        Method to get the file details by algorithm name
    """

    def get_file_data_by_algorithm_name(self, algorithm_name):
        file_query = SELECT_FILE_DATA.format(NAME, TABLE_NAME, algorithm_name)
        result_set = self._csql_session.execute(file_query)
        df_data = pd.DataFrame(result_set)
        files_data = []
        if result_set is not None and df_data.shape[0]:
            for algorithm_name in df_data["algorithm_name"].unique():
                df_data = df_data.where(pd.notnull(df_data) == True, None)
                df_temp = df_data[df_data["algorithm_name"] == algorithm_name]
                files_data = yaml.safe_load(df_temp.to_json(orient=RECORDS))
        else:
            files_data = []
        return JsonResponse(files_data, safe=False)

    """
        Method to get all the algorithm list (DISTINCT DATA)
    """

    def get_algorithm_list(self):
        result_set = self._csql_session.execute(SELECT_ALGORITHM_NAME.format(NAME, TABLE_NAME))
        df_data = pd.DataFrame(result_set)
        if result_set is not None and df_data.shape[0]:
            df = df_data[df_data.algorithm_name != 'email_notification']
            algo_list = df["algorithm_name"].tolist()
        else:
            algo_list = []
        return algo_list

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def get_algorithm_list(request, algorithm_name=None):
    """
    This function will return the algorithm list and will return error if generated.
    :param request: request django object
    :param algorithm_name : this either can be none or else it will have the algorithm name
    :return: json response
    """
    obj = None
    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    obj = AllAlgorithmList()
                    if algorithm_name:
                            return obj.get_file_data_by_algorithm_name(algorithm_name)
                    else:
                        return obj.get_algorithms_details()
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
        excMsg = "get_access_list API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
