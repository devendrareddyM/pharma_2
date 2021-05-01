"""
File                :   user_authentication 

Description         :   This file will contain the user login handle

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   16/3/2020

Date Modified       :   12/6/2020

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Configuration import ENV
from ApplicationInterface.Database.Queries import USER_AUTHETICATION_QUERY, USER_PROD_AUTHETICATION_QUERY, \
    PERMISSION_QUERY, PASSWORD_RESET_EXPIRY_CHECK, USER_PASSWORD_EXPIRY_CHECK, PERMISSION_QUERY_1, \
    GET_PARALLEL_SESSION_COUNT, GET_ACTIVE_SESSIONS_COUNT, GET_STANDARD_USER_COUNT, GET_ACTIVE_STANDARD_USER_COUNT
from ApplicationInterface.Database.Utility import _RequestValidation, _PostGreSqlConnection
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_401_UNAUTHORIZED, asert_res, \
    json_InternalServerError, json_MethodNotAllowed, HTTP_403_FORBIDDEN
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, POST_REQUEST, METHOD_NOT_ALLOWED, USERPASSWORD_KEY, \
    SALT_KEY, LOGGEDINUSERID_KEY, USERID_KEY, \
    TOKEN_KEY, PASSWORD_WRONG, USERNAME_NOT_REGISTERED, STATUS_VALUE, LOGIN_ID, USERNAME_KEY, PASSWORD_EXPIRY_PERIOD
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.TokenManagement import TokenManagement
import datetime


class UserAuthentication(_PostGreSqlConnection):
    """
    This class is responsible for authenticating the user
    """

    def __init__(self, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param request_payload: request payload
        """
        super().__init__()
        self._request_payload = request_payload

    def handle_login(self):
        """
        This will get query from the database for the username and validation
        :return: Json Response
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            session_count = None
            active_sessions = 0
            standard_user_count = 0
            active_standard = 0
            if ENV:
                self._psql_session.execute(USER_AUTHETICATION_QUERY.format(self._request_payload[LOGIN_ID]))
            else:
                self._psql_session.execute(USER_PROD_AUTHETICATION_QUERY.format(self._request_payload[LOGIN_ID]))

            result_set = self._psql_session.fetchone()
            if result_set:
                obj = HashingSalting()
                self._psql_session.execute(GET_PARALLEL_SESSION_COUNT)
                s_count = pd.DataFrame(self._psql_session.fetchall())
                if not s_count.empty:
                    session_count = s_count['limit_value'].iloc[0]
                self._psql_session.execute(GET_ACTIVE_SESSIONS_COUNT)
                active_count = pd.DataFrame(self._psql_session.fetchall())
                if not active_count.empty:
                    active_sessions = active_count['value'].iloc[0]
                if active_sessions < session_count:
                    user_type = result_set['user_type']
                    if user_type == 'Non Admin':
                        self._psql_session.execute(GET_STANDARD_USER_COUNT)
                        all_operator = pd.DataFrame(self._psql_session.fetchall())
                        if not all_operator.empty:
                            standard_user_count = all_operator['limit_value'].iloc[0].item()
                        else:
                            standard_user_count = None
                        self._psql_session.execute(GET_ACTIVE_STANDARD_USER_COUNT)
                        active_operator = pd.DataFrame(self._psql_session.fetchall())
                        if not active_operator.empty:
                            active_standard = active_operator['value'].iloc[0]
                        else:
                            active_standard = None
                        if active_standard < standard_user_count:
                            obj.active_parallel_standard_sessions_increase()
                        else:
                            return JsonResponse(
                                {
                                    MESSAGE_KEY: 'Could not login as the maximum number of parallel user logins have '
                                                 'been exceeded'},
                                status=HTTP_403_FORBIDDEN)

                    if user_type == 'Super Admin':
                        obj.active_parallel_sessions_increase()
                    if user_type == 'Admin':
                        obj.active_parallel_admin_sessions_increase()

                    self._psql_session.execute(PASSWORD_RESET_EXPIRY_CHECK.format(PASSWORD_EXPIRY_PERIOD))
                    password = pd.DataFrame(self._psql_session.fetchall())
                    if not password.empty:
                        expiry = password['value'].iloc[0]
                    else:
                        expiry = None
                    self._psql_session.execute(USER_PASSWORD_EXPIRY_CHECK.format(self._request_payload[LOGIN_ID]))
                    expiry_value = pd.DataFrame(self._psql_session.fetchall())
                    if not expiry_value.empty:
                        user_pwd_expiry = expiry_value['value'].iloc[0]
                    else:
                        user_pwd_expiry = None
                    if user_pwd_expiry <= expiry:
                        if obj.check_password(self._request_payload[USERPASSWORD_KEY], result_set[SALT_KEY],
                                              result_set[USERPASSWORD_KEY]):
                            if not result_set['status']:
                                return JsonResponse({MESSAGE_KEY: STATUS_VALUE}, status=HTTP_401_UNAUTHORIZED)
                            self._psql_session.execute(PERMISSION_QUERY_1.format(result_set['user_type']))
                            permissions = pd.DataFrame(self._psql_session.fetchall())
                            role = str(result_set['user_type'])
                            dict_data = {}
                            if not permissions.empty:
                                data_now = permissions.groupby('section')
                                for name, group in data_now:
                                    dict_data[name] = list(group['feature'])
                            jwt_token = TokenManagement().add_jwt(
                                {
                                    LOGGEDINUSERID_KEY: result_set[USERID_KEY],
                                    LOGIN_ID: result_set[LOGIN_ID],
                                    USERNAME_KEY: result_set['name'],
                                    'role': role,
                                    'permissions': dict_data,
                                    'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=86400)
                                }
                            )

                            return JsonResponse({TOKEN_KEY: jwt_token})

                        return JsonResponse({MESSAGE_KEY: PASSWORD_WRONG}, status=HTTP_401_UNAUTHORIZED)
                    return JsonResponse({MESSAGE_KEY: "PASSWORD EXPIRED! PLEASE CONTACT YOUR SUPER ADMIN"},
                                        status=HTTP_403_FORBIDDEN)
                return JsonResponse(
                    {MESSAGE_KEY: 'Could not login as the maximum number of parallel user logins have been exceeded'},
                    status=HTTP_403_FORBIDDEN)

            return JsonResponse({MESSAGE_KEY: USERNAME_NOT_REGISTERED}, status=HTTP_401_UNAUTHORIZED)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def authenticate_user(request):
    """
    This function will validate the user and on successful response it will generate the JWT token
    :param request: request django object
    :return: json response
    """

    obj = None
    try:
        if request.method == POST_REQUEST:
            request_payload = _RequestValidation().validate_request(request, [LOGIN_ID, USERPASSWORD_KEY])
            obj = UserAuthentication(request_payload)
            return obj.handle_login()

        log_debug(METHOD_NOT_ALLOWED)
        return json_MethodNotAllowed
    except Exception as e:
        excMsg = "Authentication API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
