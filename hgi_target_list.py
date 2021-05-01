"""
File                :   hgi_target_list.py

Description         :   This will return all the target names

Author              :   LivNSense Technologies

Date Created        :   13-05-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import json
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Configuration import ListofTargetHGI, TABLE_NAME, NAME
from ApplicationInterface.Database.Queries import TARGET_HGI_LIST, UPDATED_HGI_TARGET_DATE, TARGET_UPDATED_VALUE
from ApplicationInterface.Database.Utility import _CassandraConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_401_UNAUTHORIZED
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    TIMESTAMP_KEY, PARAM_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class GetHgiList(_CassandraConnection):
    """
    This class is responsible for getting the target_list for HGI
    """

    def __init__(self, algorithm_name):
        """
        This will call the parent class to validate the connection
        """
        super().__init__()
        self.algorithm_name = algorithm_name

    def get_hgi_values(self):
        """
        This will return the lat updated and target list and offset value
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            df_data = pd.DataFrame(
                self._csql_session.execute(TARGET_HGI_LIST.format(NAME, TABLE_NAME, self.algorithm_name)))
            dict_data = {"target_hgi": ListofTargetHGI,
                         "offset": None,
                         "last_updated_target": self.get_updated_data_by_algorithm_name(self.algorithm_name)}
            if not df_data.empty:
                df = pd.DataFrame.from_dict(json.loads(df_data['value'][0]))
                for i, row in df.iterrows():
                    dict_data["offset"] = int(row['HgiDelta'])
            return JsonResponse(dict_data, safe=False)
        except AssertionError as e:
            log_error("Assertion error due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    """
    Method to get the updated target hgi by algorithm name
    """

    def get_updated_data_by_algorithm_name(self, algorithm_name):
        updated_time_query = UPDATED_HGI_TARGET_DATE.format(NAME, TABLE_NAME, algorithm_name)
        result = self._csql_session.execute(updated_time_query)
        df_result = pd.DataFrame(result)
        updated_time = df_result[TIMESTAMP_KEY].iloc[0]
        file_query = TARGET_UPDATED_VALUE.format(NAME, TABLE_NAME, algorithm_name, updated_time)
        result_set = self._csql_session.execute(file_query)
        df_data = pd.DataFrame(result_set)
        df_data = df_data.where(pd.notnull(df_data) == True, None)

        if result_set is not None and df_data.shape[0]:
            df = int(df_data[PARAM_VALUE].iloc[0])
            return df
        else:
            return None

    def __del__(self):
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def get_hgi_list(request, algorithm_name=None):
    """
    This function will return all the target list

    :param algorithm_name:
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = GetHgiList(algorithm_name)
                return obj.get_hgi_values()

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
        excMsg = "get_hgi_list API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
