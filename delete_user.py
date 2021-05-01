"""
File                :   delete_user.py

Description         :   This file will handle the delete operations

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   12/6/2020

Date Modified       :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, CHECK_DELETE_USER, DELETE_USER, \
    CHECK_LOGIN_ID
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import json_MethodNotAllowed, HTTP_400_BAD_REQUEST, HTTP_500_INTERNAL_SERVER_ERROR, \
    asert_res, json_InternalServerError, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, DELETE_REQUEST, DELETED_SUCCESSFULLY, \
    DELETED_ERROR, STATUS_KEY, LOGIN_ID, DB_ERROR, USER_TYPE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class DeleteUser(_PostGreSqlConnection):
    """
    This class will help to delete the existing user
    """

    def __init__(self, loggedin_userid_details, login_id=None):
        """
        This will call the parent class to validate the connection and request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self.login_id = login_id

    def delete_user(self):
        """
        This function will delete the user details
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__delete_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __delete_user_query(self):
        """
        This function will execute the query for deleting the requested user
        :return: Json object
        """

        self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
        result_set = self._psql_session.fetchall()
        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)
        Permission = pd.DataFrame(result_set)
        if Permission[USER_TYPE].iloc[0] == 'Super Admin':
            self._psql_session.execute(CHECK_LOGIN_ID)
            loginids = self._psql_session.fetchall()
            login_ids = pd.DataFrame(loginids)
            log_in_id = list(login_ids["login_id"])
            if self.login_id in log_in_id:
                self._psql_session.execute(CHECK_DELETE_USER.format(self.login_id))
                del_user = pd.DataFrame(self._psql_session.fetchall())
                if not del_user.empty:
                    if del_user[USER_TYPE].iloc[0] != 'Super Admin':
                        self._psql_session.execute(DELETE_USER.format(self.login_id))
                    else:
                        return JsonResponse({MESSAGE_KEY: DELETED_ERROR})
            else:
                return JsonResponse({MESSAGE_KEY: "USER IS NOT REGISTER WITH US"})

        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: DELETED_ERROR}, status=HTTP_500_INTERNAL_SERVER_ERROR)

        return JsonResponse({MESSAGE_KEY: DELETED_SUCCESSFULLY})

    def __is_user_not_authorised(self):
        """
        This will query  , whether the person who is deleting the user is authenticated to delete the user or not
        :return: boolean object
        """
        pass

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def delete_user(request, login_id=None):
    """
    This function will delete the existing user
    :param login_id: it will give the log_in id of the deleting user
    :param request: request django object
    :param username : username that need to be deleted
    :return: json object
    """
    obj = None

    try:
        if request.method == DELETE_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    obj = DeleteUser(loggedin_user_details, login_id)
                    return obj.delete_user()
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
        excMsg = "delete_users_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
