"""
File                :   modify_user_count.py

Description         :   This file will have modify count of the users program

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   12/6/2020

Date Modified       :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, UPDATE_USER_COUNT
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, _RequestValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import PUT_REQUEST, PARALLEL_SESSIONS, STANDARD_USER, ADMIN_USER, MESSAGE_KEY, \
    METHOD_NOT_ALLOWED, UPDATED_SUCCESSFULLY, UPDATE_ERROR, LOGIN_ID, STATUS_KEY, DB_ERROR, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_debug, log_error


class Updateusercount(_PostGreSqlConnection):
    """
    This class will help to update details for the existing user
    """

    def __init__(self, loggedin_user_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_user_details: loggedin userid details
        :param request_payload: request payload
        """
        super().__init__()

        self.loggedin_userid_details = loggedin_user_details
        self._request_payload = request_payload

    def update_user_count(self):
        """
        This function will update the count of the user
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__update_user_count_query()

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __update_user_count_query(self):
        """
        This function will execute the query for updating the maximum count of the users
        :return: Json object
        """

        self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
        result_set = self._psql_session.fetchall()

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)

        Permission = pd.DataFrame(result_set)
        if Permission['user_type'].iloc[0] == 'Super Admin':
            for each in self._request_payload:
                limit_value = self._request_payload[each]
                self._psql_session.execute(UPDATE_USER_COUNT.format(limit_value, each))
        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: UPDATE_ERROR}, status=HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY})

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def Modify_user_count(request):
    """
    This function will update the user Count
    :param request: request django object
    :return: jsonobject
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [
                        PARALLEL_SESSIONS, STANDARD_USER, ADMIN_USER])

                    obj = Updateusercount(loggedin_user_details, request_payload)

                    return obj.update_user_count()
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
        excMsg = "modify_count_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
