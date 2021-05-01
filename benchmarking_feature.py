"""
File                :   benchmarking_feature.py

Description         :   This file will return the benchmarking feature result for the maximum timestamp

Author              :   LivNSense Technologies

Date Created        :   20-05-2020

Date Last modified :    25-05-2020

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
    BENCHMARKING_PERFORMANCE_TAGS, CATEGORIES_FOR_BENCHMARKING, CURRENT_TIMESTAMP, HIST_BEST_TIME, LBT_ERROR_CODE
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, EXCEPTION_CAUSE, GET_REQUEST, \
    METHOD_NOT_ALLOWED, TIMESTAMP_KEY, \
    DYNAMIC_BENCHMARKING, FLAG_STATUS_VALUE, HOT_CONSOLE_1_VALUE, TAG_NAME_REQUEST, START_DATE_REQUEST, \
    END_DATE_REQUEST, HOT_CONSOLE_2_VALUE, \
    COLD_CONSOLE_1_VALUE, COLD_CONSOLE_2_VALUE, DEBUG, ONE, ZERO, ALERT_LIST, TARGETS, CATEGORIES_NAME, \
    RESULT_TAG, PERFORMANCE_TAG, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance

from utilities.LoggerFile import log_error, log_debug

from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class BenchmarkingFeature(_PostGreSqlConnection):
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

            self._psql_session.execute(CURRENT_TIMESTAMP)
            curr_timestamp = pd.DataFrame(self._psql_session.fetchall())
            if not curr_timestamp.empty:
                time = curr_timestamp['timestamp'].iloc[0]
            else:
                time = None

            self._psql_session.execute(HIST_BEST_TIME)
            hist_best = pd.DataFrame(self._psql_session.fetchall())
            if not hist_best.empty:
                best_date = hist_best['timestamp'].iloc[0]
            else:
                best_date = None
            error_details = []
            Alert_status = 1
            dict_data = {
                "timestamp": time,
                "best_date": best_date,
                "Categories": [],
                "Targets": [],
                "Performance_tag": [],
                "Result_tag": [],
                "errors": error_details,
                "status": Alert_status
            }

            dynamic_benchmarking_df = pd.DataFrame()

            try:
                self._psql_session.execute(CATEGORIES_FOR_BENCHMARKING)
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data[CATEGORIES_NAME] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)

            try:
                self._psql_session.execute(BENCHMARKING_TARGETS)
                df = pd.DataFrame(self._psql_session.fetchall())
                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    df.loc[df['is_active'] == False, 'target_value'] = None
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data[TARGETS] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)
            try:
                self._psql_session.execute(BENCHMARKING_RESULT_TAGS)

                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)

                    dict_data[RESULT_TAG] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)

            try:
                self._psql_session.execute(BENCHMARKING_PERFORMANCE_TAGS)
                df = pd.DataFrame(self._psql_session.fetchall())

                if not df.empty:
                    df = df.where(pd.notnull(df) == True, None)
                    dynamic_benchmarking_df = dynamic_benchmarking_df.append(df, ignore_index=True)
                    dict_data[PERFORMANCE_TAG] = yaml.safe_load(df.to_json(orient=RECORDS))

            except Exception as e:
                log_error(e)
            try:
                self._psql_session.execute(LBT_ERROR_CODE.format(time))
                df_error = pd.DataFrame(self._psql_session.fetchall())
                if not df_error.empty:
                    df_error = df_error.where(pd.notnull(df_error) == True, None)
                    df_error = df_error.drop_duplicates()
                    dict_data["errors"] = dict_data["errors"] + yaml.safe_load(df_error.to_json(orient=RECORDS))
                    error_code = df_error["error_code"].iloc[0]
                    if error_code in ALERT_LIST:
                        Alert_status = 3
                    elif error_code not in ALERT_LIST:
                        Alert_status = 2
                    else:
                        Alert_status = 1
                    dict_data['status'] = Alert_status
                else:
                    if DEBUG == ZERO:
                        print("Currently no error details!")
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
def get_benchmarking_data(request):
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
                obj = BenchmarkingFeature()
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
