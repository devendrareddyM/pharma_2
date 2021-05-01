"""
File                :   dynamic_benchmarking.py

Description         :   This file will return the dynamic benchmarking result for the particular equipments

Author              :   LivNSense Technologies

Date Created        :   18-07-2019

Date Last modified :    19-07-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import datetime
import traceback
import time as t
from datetime import datetime

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import BENCHMARKING_TARGETS, BENCHMARKING_RESULT_TAGS, \
    BENCHMARKING_PERFORMANCE_TAGS, CONFIGURATION_TARGETS_TAGS, CONFIGURATION_MATCH_TAGS, \
    CONFIGURATION_NOISE_TAGS, CONFIGURATON_SINGLE_PERF, CONFIGURATION_PERF_TAGS_LIST
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, TIMESTAMP_KEY, \
    DYNAMIC_BENCHMARKING, FLAG_STATUS_VALUE, HOT_CONSOLE_1_VALUE, TAG_NAME_REQUEST, START_DATE_REQUEST, \
    END_DATE_REQUEST, HOT_CONSOLE_2_VALUE, \
    COLD_CONSOLE_1_VALUE, COLD_CONSOLE_2_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance

from utilities.LoggerFile import log_error, log_debug

from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class ConfigScreen(_PostGreSqlConnection):
    """
    This class is responsible for reading data from the Database and perform operation according to LBT algo
    and return JSON
    """

    def __init__(self):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()

    def get_values(self):
        """
        This will get query from the Database for the console and equipment name
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            self._psql_session.execute(CONFIGURATON_SINGLE_PERF)
            perf = pd.DataFrame(self._psql_session.fetchall())
            if not perf.empty:
                value = perf['case_name'].to_string()
                result = ''.join([i for i in value if not i.isdigit()]).strip()
            else:
                result = []

            dict_data = {
                "Targets": [],
                "Performance_tag": result,
                "Match_tags": [],
                "Noise_Tags": [],
                "Performance_tags_list": []
            }

            dynamic_benchmarking_df = pd.DataFrame()

            try:
                self._psql_session.execute(CONFIGURATION_TARGETS_TAGS)
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data["Targets"] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)
            try:
                self._psql_session.execute(CONFIGURATION_MATCH_TAGS)

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)

                    dict_data["Match_tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)

            try:
                self._psql_session.execute(CONFIGURATION_PERF_TAGS_LIST)
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data["Performance_tags_list"] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)

            try:
                self._psql_session.execute(CONFIGURATION_NOISE_TAGS)
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data["Noise_Tags"] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)

            return JsonResponse(dict_data, safe=False)

        except AssertionError as e:
            log_error(e)
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error(traceback.format_exc())
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_configscreen_data(request):
    print("hi")
    """
    This function will get the values for the equipment level as well as console level dynamic benchmarking
    :param request: request django object
    :param unit: unit name
    :param console: console name will be provided
    :param equipment: equipment name will be provided
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = ConfigScreen()
                return obj.get_values()

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
