"""
File                :  clear_notifications.py

Description         :   This file will return the access_list of p66

Author              :   LivNSense Technologies

Date Created        :   24-06-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import json

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, \
    GET_PERMISSION, CLEAR_NOTIFICATIONS
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, _RequestValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, METHOD_NOT_ALLOWED, LOGIN_ID, POST_REQUEST, \
    UTF8_FORMAT, TAG_NAME, TIMESTAMP, USER_ID, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class ClearNotifications(_PostGreSqlConnection):
    """
    This class is responsible for reading data from the Database and perform operation according to LBT algo
    and return JSON
    """

    def __init__(self, loggedin_userid_details, request_payload=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param  request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self._request_payload = request_payload

    def clear_notifications(self):
        """
        This will get query from the Database for LBT algo
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
            user = pd.DataFrame(self._psql_session.fetchall())
            if not self._psql_session.rowcount:
                return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)
            self._psql_session.execute(GET_PERMISSION.format(user['user_type'].iloc[0]))
            permission = pd.DataFrame(self._psql_session.fetchall())
            if not permission.empty:
                permissions = list(permission["feature"])
            else:
                permissions = []
            if 'Notification Clear Functionality' in permissions:
                for each in self._request_payload:
                    try:
                        clear_notification = CLEAR_NOTIFICATIONS.format(TAG_NAME, TIMESTAMP, USER_ID, each[TAG_NAME],
                                                                        each[TIMESTAMP],
                                                                        self.loggedin_userid_details[LOGIN_ID])
                        self._psql_session.execute(clear_notification)

                    except Exception as e:
                        log_error("Exception occurred due to" + str(e))
                        return json_InternalServerError

                return JsonResponse({"MESSAGE": "NOTIFICATIONS SUCCESSFULLY CLEARED!"}, safe=False)
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
        except AssertionError as e:
            log_error("Exception occurred due to" + str(e))
            return asert_res(e)

        except Exception as e:
            log_error("Exception occurred due to" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def clear_notifications_data(request):
    """
    This function will get the values for dynamic benchmarking
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == POST_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                request_payload = json.loads(request.body.decode(UTF8_FORMAT))
                obj = ClearNotifications(loggedin_user_details, request_payload)
                return obj.clear_notifications()
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
        excMsg = "clear_notifications API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
