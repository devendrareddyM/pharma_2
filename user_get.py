"""
File                :   user_get.py

Description         :   This file is for getting the details for the particular username

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   1/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
import yaml
from django.http import JsonResponse
import pandas as pd
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, SINGLE_USER_GET_DETAILS, \
    PASSWORD_RESET_EXPIRY_CHECK
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import asert_res, json_InternalServerError, json_MethodNotAllowed, \
    HTTP_500_INTERNAL_SERVER_ERROR, HTTP_401_UNAUTHORIZED, HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN
from utilities.Constants import MESSAGE_KEY, STATUS_KEY, DB_ERROR, GET_REQUEST, METHOD_NOT_ALLOWED, LOGIN_ID, RECORDS, \
    PASSWORD_EXPIRY_PERIOD, HTTP_AUTHORIZATION_TOKEN, ENCRYPT_ALGO
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import *
from utilities.TokenManagement import seceret_key


class GetUSerData(_PostGreSqlConnection):
    """
    This class will help to get the details for the existing users
    """

    def __init__(self, loggedin_userid_details):
        """
        This will call the parent class to validate the connection and request payload
        :param username: this will control the query a particular user
        :param loggedin_userid_details : loggedin userid details
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details

    def getuser_list(self):
        """
        This function will get the user list
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            return self.__get_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)

        except Exception as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

    def __get_user_query(self):
        """
        This function will execute the query for getting the details for the requested user or all users
        :return: Json object
        """
        try:
            self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
            result_set = self._psql_session.fetchall()
            user = []
            if result_set:
                self._psql_session.execute(SINGLE_USER_GET_DETAILS.format(self.loggedin_userid_details[LOGIN_ID]))
                user_data = pd.DataFrame(self._psql_session.fetchall())
                expiry_peroid = user_data['Days Left for password to expire'].iloc[0]
                user_data.drop(columns=['Days Left for password to expire'], inplace=True)
                self._psql_session.execute(PASSWORD_RESET_EXPIRY_CHECK.format(PASSWORD_EXPIRY_PERIOD))
                password = pd.DataFrame(self._psql_session.fetchall())
                if not password.empty:
                    expiry = password['value'].iloc[0]
                else:
                    expiry = None

                user_data['Days Left for password to expire'] = expiry - expiry_peroid
                user = yaml.safe_load(user_data.to_json(orient=RECORDS))
                return JsonResponse(user, safe=False)
            return JsonResponse(user, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)

        except Exception as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def get_user_list(request):
    """
    This function will help to get all the users
    :param request: request django object
    :param username : for particular username
    :return: json object
    """

    obj = None
    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin', 'Admin']
            if r_name in names:
                if loggedin_user_details:
                    obj = GetUSerData(loggedin_user_details)
                    return obj.getuser_list()
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
        excMsg = "user_list API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
