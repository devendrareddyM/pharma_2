"""
File                :   benchmarking_update.py

Description         :   This will update all the benchmarking configuration details

Author              :   LivNSense Technologies

Date Created        :   23-05-2020

Date Last modified  :   23-05-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""

import json

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import SETTING_PERF_TAG_TO_FALSE, \
    UPDATING_PERFORMANCE_TAGS, UPDATING_TARGET_TAGS, UPDATING_MATCH_TAGS, UPDATING_NOISE_TAGS, LBT_TAG_DATA_VALIDATION, \
    LBT_MATCH_TAG_DATA_VALIDATION, LBT_NOISE_TAG_DATA_VALIDATION
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, sucess_message, \
    asert_res, json_InternalServerError, json_MethodNotAllowed, HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, DB_ERROR, \
    PUT_REQUEST, UTF8_FORMAT, PERFORMANCE_TAG, DESCRIPTION, TARGETS, MATCH_TAGS, \
    NOISE_TAGS, TAG_NAME_REQUEST, TARGET_VALUE, IS_ACTIVE, MIN, MAX, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class BenchmarkingUpdateConfiguration(_PostGreSqlConnection):
    """
    This class is responsible for updating the configuration details for the benchmarking
    """

    def __init__(self, request_payload):
        """
        :param request_payload : request
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self._request_payload = request_payload

    """
    Updates the benchmarking configuration details
    """

    def update_benchmarking(self):
        """
        This will update the benchmarking configuration details in the database
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            try:
                a = []

                if TARGETS in self._request_payload:
                    for each in self._request_payload[TARGETS]:
                        target_min = float(each[MIN])
                        target_max = float(each[MAX])
                        target_description = each[DESCRIPTION]
                        target_tag = each[TAG_NAME_REQUEST]
                        target_value = float(each[TARGET_VALUE])
                        target_active = each[IS_ACTIVE]
                        if TARGETS in self._request_payload:
                            self._psql_session.execute(LBT_TAG_DATA_VALIDATION.format(target_tag))
                            df = pd.DataFrame(self._psql_session.fetchall())
                            df['condition'] = df['condition'].str[1:]
                            df['condition'] = df['condition'].astype(float)
                            df_target = df.loc[(df[TAG_NAME_REQUEST] == target_tag)]['condition'].iloc[0]
                            if df_target <= target_value <= target_max and target_value >= target_min >= df_target and target_min <= target_value and target_max >= target_min and target_max >= target_value:
                                update_target_tags_query = UPDATING_TARGET_TAGS.format(target_min, target_max,
                                                                                       target_value,
                                                                                       target_active,
                                                                                       self._request_payload[
                                                                                           PERFORMANCE_TAG],
                                                                                       target_description,
                                                                                       target_tag
                                                                                       )

                                self._psql_session.execute(update_target_tags_query)
                            else:
                                a.append(target_description)
                if MATCH_TAGS in self._request_payload:
                    for each in self._request_payload[MATCH_TAGS]:
                        match_min = float(each[MIN])
                        match_max = float(each[MAX])
                        match_description = each[DESCRIPTION]
                        match_tag = each[TAG_NAME_REQUEST]
                        match_active = each[IS_ACTIVE]
                        if MATCH_TAGS in self._request_payload:
                            update_match_tags_query = UPDATING_MATCH_TAGS.format(match_min, match_max, match_active,
                                                                                 self._request_payload[
                                                                                     PERFORMANCE_TAG],
                                                                                 match_description, match_tag
                                                                                 )
                            self._psql_session.execute(update_match_tags_query)
                if NOISE_TAGS in self._request_payload:
                    for each in self._request_payload[NOISE_TAGS]:
                        noise_min = float(each[MIN])
                        noise_max = float(each[MAX])
                        noise_description = each[DESCRIPTION]
                        noise_tag = each[TAG_NAME_REQUEST]
                        noise_active = each[IS_ACTIVE]
                        if NOISE_TAGS in self._request_payload:
                            self._psql_session.execute(LBT_NOISE_TAG_DATA_VALIDATION.format(noise_tag))
                            df = pd.DataFrame(self._psql_session.fetchall())
                            df['condition'] = df['condition'].str[2:]
                            df['condition'] = df['condition'].astype(float)
                            df_noise = df.loc[(df[TAG_NAME_REQUEST] == noise_tag)]['condition'].iloc[0]
                            if df_noise <= noise_min <= noise_max and noise_max >= noise_min:
                                update_noise_tags_query = UPDATING_NOISE_TAGS.format(noise_min, noise_max, noise_active,
                                                                                     self._request_payload[
                                                                                         PERFORMANCE_TAG],
                                                                                     noise_description, noise_tag
                                                                                     )
                                self._psql_session.execute(update_noise_tags_query)
                            else:
                                a.append(noise_description)
                self._psql_session.execute(SETTING_PERF_TAG_TO_FALSE)
                if PERFORMANCE_TAG in self._request_payload:
                    update_performance_tags_query = UPDATING_PERFORMANCE_TAGS.format(
                        self._request_payload[PERFORMANCE_TAG])

                    self._psql_session.execute(update_performance_tags_query)

            except Exception as e:
                log_error("Exception due to : %s" + str(e))
                return asert_res(e)
            if len(a) != 0 and len(a) == 1:
                tag = ','.join(a)
                return JsonResponse({MESSAGE_KEY: "Set Max and Min values within the range for the tag:" + tag},
                                    status=HTTP_400_BAD_REQUEST)
            elif len(a) >= 1:
                tags = ','.join(a)
                return JsonResponse({MESSAGE_KEY: "Set Max and Min values within the range for the tags:" + tags},
                                    status=HTTP_400_BAD_REQUEST)
            else:
                return JsonResponse(sucess_message, safe=False)

        except AssertionError as e:
            log_error("Assertion error due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def update_benchmarking_configuration(request):
    """
    This function will update the benchmarking configuration details with the passed json values.
    :param request: request django object
    :return: json response
    """
    obj = None
    try:
        if request.method == PUT_REQUEST:
            request_payload = json.loads(request.body.decode(UTF8_FORMAT))
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = BenchmarkingUpdateConfiguration(request_payload)
                return obj.update_benchmarking()

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
        excMsg = "Updated API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
