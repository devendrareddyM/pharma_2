"""
File                :   configuration_update.py

Description         :   This will update parameters for the selected algorithm

Author              :   LivNSense Technologies

Date Created        :   12-06-2020

Date Last modified  :   12-06-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import json
import traceback
from datetime import datetime

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Queries import UPDATE_CONFIGURATION_DEACTIVE_STATUS_DATA, \
    UPDATE_CONFIGURATION_ACTIVE_STATUS_DATA
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_403_FORBIDDEN, \
    json_MethodNotAllowed, HTTP_401_UNAUTHORIZED
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    PUT_REQUEST, UPDATED_SUCCESSFULLY, UTF8_FORMAT, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class UpdateAlgorithmParams(_PostGreSqlConnection):
    """
    This class is responsible for updating the params for the algorithm
    """

    def __init__(self, request_payload):
        """
        :param algorithm_name : this will have the algorithm name
        :param request_payload : request
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.request_payload = request_payload

    """
    Updates the algorithm details
    """

    def update_algorithms(self):
        """
        This will return all the list of the algorithm in json format from the Database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            LAST_MODIFIED_DATE = datetime.now()
            for algo_data in self.request_payload:
                try:
                    if algo_data["status"]:
                        query = UPDATE_CONFIGURATION_ACTIVE_STATUS_DATA.format(algo_data["status"],
                                                                               LAST_MODIFIED_DATE,
                                                                               algo_data["module"])
                    else:
                        query = UPDATE_CONFIGURATION_DEACTIVE_STATUS_DATA.format(algo_data["status"],
                                                                                 LAST_MODIFIED_DATE,
                                                                                 algo_data["module"])

                    self._psql_session.execute(query)
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                    return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                        status=e.args[0][STATUS_KEY])

            return JsonResponse({MESSAGE_KEY: UPDATED_SUCCESSFULLY}, safe=False)

        except AssertionError as e:
            log_error("Exception due to : %s", e)
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
def update_algorithm_params(request):
    """
    This function will update the algorithm with the passed json values.
    :param request: request django object
    :param algorithm_name : this either can be none or else it will have the algorithm name
    :return: json response
    """
    obj = None
    try:
        if request.method == PUT_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            permissions = loggedin_user_details['permissions']['Settings']
            permission_name = 'Module On/OFF'
            if permission_name in permissions:
                if loggedin_user_details:
                    request_payload = json.loads(request.body.decode(UTF8_FORMAT))
                    obj = UpdateAlgorithmParams(request_payload)
                return obj.update_algorithms()
            else:
                return JsonResponse({MESSAGE_KEY: "Forbidden Error"}, status=HTTP_403_FORBIDDEN)

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
        return error_instance(e)

    finally:
        if obj:
            del obj
