"""
File                :   hgi_target_update.py

Description         :   This will update all the outage target value

Author              :   LivNSense Technologies

Date Created        :   13-05-2020

Date Last modified  :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""

import json
import time
import traceback

import jwt

from ApplicationInterface.Database.Configuration import FLAG, NAME, TABLE_NAME
from ApplicationInterface.Database.Queries import UPDATE_PARAM_DATA, TARGET_HGI_UPDATE
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    PUT_REQUEST, UPDATED_SUCCESSFULLY, UTF8_FORMAT, PARAM_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, asert_res, \
    sucess_message, json_InternalServerError, json_MethodNotAllowed, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _CassandraConnection, _TokenValidation


class HgiTargetUpdateParams(_CassandraConnection):
    """
    This class is responsible for updating the params for the target
    """

    def __init__(self, request_payload, algorithm_name):
        """
        :param algorithm_name: this will have the target name
        :param request_payload : request
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.algorithm_name = algorithm_name
        self._request_payload = request_payload

    """
    Updates the target Hgi details
    """

    def update_targets(self):
        """
        This will return all the list of the targets in json format from the database .
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            LAST_MODIFIED_DATE = str(round(time.time() * 1000))

            try:
                query = TARGET_HGI_UPDATE.format(NAME, TABLE_NAME, self._request_payload[PARAM_VALUE],
                                                 LAST_MODIFIED_DATE,
                                                 FLAG,
                                                 self.algorithm_name)

                self._csql_session.execute(query)
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
        if self._csql_session:
            self._csql_session.close()


@csrf_exempt
def update_hgi_target(request, algorithm_name):
    """
    This function will update the target with the passed json values.
    :param request: request django object
    :param algorithm_name : this either can be none or else it will have the target name
    :return: json response
    """
    obj = None
    try:
        if request.method == PUT_REQUEST:
            request_payload = json.loads(request.body.decode(UTF8_FORMAT))
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = HgiTargetUpdateParams(request_payload, algorithm_name)
                return obj.update_targets()

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
        excMsg = "get_hgi_list API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
