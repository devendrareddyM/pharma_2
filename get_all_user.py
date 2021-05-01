"""
File                :   benchmarking_feature.py

Description         :   This file will return the benchmarking feature result for the maximum timestamp

Author              :   LivNSense Technologies

Date Created        :   20-05-2020

Date Last modified :    25-05-2020

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import GET_ALL_USER, GET_ADMIN_COUNT, GET_ADMIN_USER, GET_PARALLEL_SESSION, \
    GET_STANDARD_USER_COUNT
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    HTTP_400_BAD_REQUEST, json_InternalServerError, asert_res, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, RECORDS, GET_REQUEST, \
    METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class user_get(_PostGreSqlConnection):
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

    def get_user_values(self):
        """
        This will get query from the Database for the console and equipment name
        :return: Json Response
        """
        admin_count = None
        admin_user = None
        standard_user_count = None
        par_value = None
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            try:
                self._psql_session.execute(GET_ADMIN_USER)
                admin = pd.DataFrame(self._psql_session.fetchall())
                if not admin.empty:
                    admin_user = admin['limit_value'].iloc[0].item()
                else:
                    admin_user = None
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            try:
                self._psql_session.execute(GET_ADMIN_COUNT)
                all_admin = pd.DataFrame(self._psql_session.fetchall())
                if not all_admin.empty:
                    admin_count = all_admin['count'].iloc[0].item()
                else:
                    admin_count = None
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            try:
                self._psql_session.execute(GET_STANDARD_USER_COUNT)
                all_operator = pd.DataFrame(self._psql_session.fetchall())
                if not all_operator.empty:
                    standard_user_count = all_operator['limit_value'].iloc[0].item()
                else:
                    standard_user_count = None
            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            try:
                self._psql_session.execute(GET_PARALLEL_SESSION)
                parallel = pd.DataFrame(self._psql_session.fetchall())
                if not parallel.empty:
                    par_value = parallel['limit_value'].iloc[0].item()
                else:
                    par_value = None
            except Exception as e:
                log_error('The Exception is' + str(e))

            temp = {
                "users": [],
                "admin_count": admin_user,
                "total_admin": admin_count,
                "standard": standard_user_count,
                "parallel_sessions": par_value
            }

            try:
                self._psql_session.execute(GET_ALL_USER)
                df = pd.DataFrame(self._psql_session.fetchall())
                temp['users'] = yaml.safe_load(
                    df.to_json(orient=RECORDS))

            except Exception as e:
                log_error("Exception due to : %s" + str(e))
            return JsonResponse(temp, safe=False)

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
def get_user_data(request):
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
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    obj = user_get()
                    return obj.get_user_values()
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
        excMsg = "get_all_users_data API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
