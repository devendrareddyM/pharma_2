"""
File                :   benchmarking_update.py

Description         :   This will update all the benchmarking configuration details

Author              :   LivNSense Technologies

Date Created        :   19-06-2020

Date Last modified  :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""

import json

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import ACCESS_UPDATE, NOTIFICATION_UPDATE
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, _RequestValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, sucess_message, \
    asert_res, json_InternalServerError, json_MethodNotAllowed, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, DB_ERROR, \
    PUT_REQUEST, UTF8_FORMAT, SUPER_ADMIN, ADMIN, NON_ADMIN, SECTION, FEATURE, \
    VALUE, DEFAULT_NOTIFICATION_VIEW, SETTING, MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD, PASSWORD_EXPIRY_PERIOD, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class AccessUpdate(_PostGreSqlConnection):
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
    Updates the Access details
    """

    def update_access(self):
        """
        This will update the Access details in the Users
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            try:
                """
                Getting the features and access list of users and updating
                
                """
                for each in self._request_payload:
                    if each not in [PASSWORD_EXPIRY_PERIOD, MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD,
                                    DEFAULT_NOTIFICATION_VIEW]:
                        for each1 in self._request_payload[each]:
                            try:
                                access_update = ACCESS_UPDATE.format(SUPER_ADMIN, each1[SUPER_ADMIN],
                                                                     ADMIN, each1[ADMIN],
                                                                     NON_ADMIN, each1[NON_ADMIN],
                                                                     SECTION, each,
                                                                     FEATURE, each1[FEATURE])
                                self._psql_session.execute(access_update)
                            except Exception as e:
                                log_error("Exception due to : %s" + str(e))
                                return JsonResponse({MESSAGE_KEY: "error due {}".format(e)})
                    if each in [DEFAULT_NOTIFICATION_VIEW, MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD,
                                PASSWORD_EXPIRY_PERIOD]:
                        try:
                            if DEFAULT_NOTIFICATION_VIEW == each:
                                access_update = NOTIFICATION_UPDATE.format(VALUE, int(
                                    self._request_payload[DEFAULT_NOTIFICATION_VIEW]),
                                                                           SETTING, DEFAULT_NOTIFICATION_VIEW)
                                self._psql_session.execute(access_update)
                        except Exception as e:
                            log_error("Exception due to : %s" + str(e))
                            return JsonResponse(
                                {MESSAGE_KEY: "error occur during updating the {}".format(DEFAULT_NOTIFICATION_VIEW)})
                    if MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD == each:
                        try:
                            access_update = NOTIFICATION_UPDATE.format(VALUE, int(
                                self._request_payload[MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD]),
                                                                       SETTING,
                                                                       MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD)
                            self._psql_session.execute(access_update)
                        except Exception as e:
                            log_error("Exception due to : %s" + str(e))
                            return JsonResponse(
                                {MESSAGE_KEY: "error occur during updating the {}".format(
                                    MAXIMUM_VALUES_NOTIFICATION_DOWNLOAD_TIME_PERIOD)})
                    if PASSWORD_EXPIRY_PERIOD == each:
                        try:
                            access_update = NOTIFICATION_UPDATE.format(VALUE, int(
                                self._request_payload[PASSWORD_EXPIRY_PERIOD]),
                                                                       SETTING, PASSWORD_EXPIRY_PERIOD)
                            self._psql_session.execute(access_update)
                        except Exception as e:
                            log_error("Exception due to : %s" + str(e))
                            return JsonResponse(
                                {MESSAGE_KEY: "error occur during updating the {}".format(PASSWORD_EXPIRY_PERIOD)})

            except Exception as e:

                log_error("Exception due to : %s" + str(e))

                return asert_res(e)

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
def update_access_configuration(request):
    """
    This function will update the Access of users details with the passed json values.
    :param request: request django object
    :return: json response
    """
    obj = None
    try:
        if request.method == PUT_REQUEST:
            request_payload = json.loads(request.body.decode(UTF8_FORMAT))
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, request_payload)
                    obj = AccessUpdate(request_payload)
                    return obj.update_access()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)

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
        excMsg = "Access_Update API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
