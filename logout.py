import json

import jwt
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _RequestValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, POST_REQUEST, METHOD_NOT_ALLOWED, \
    HTTP_AUTHORIZATION_TOKEN, TOKEN, UTF8_FORMAT
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class Logout(_PostGreSqlConnection):

    def __init__(self, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: logged in user id
        :param request_payload: request payload
        """
        super().__init__()
        self._request_payload = request_payload

    def get_logout_user(self):
        try:
            token = self._request_payload[TOKEN]
            role = jwt.decode(token, verify=False)
            obj = HashingSalting()
            if role['role'] == 'Admin':
                obj.decreasing_admin_login_count()
            if role['role'] == 'Non Admin':
                obj.decreasing_Non_Admin_login_count()
            if role['role'] == 'Super Admin':
                obj.decreasing_super_Admin_login_count()
            return JsonResponse({MESSAGE_KEY: "Logout Successfully"})
        except Exception as e:
            log_error(e)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def logout_user(request):
    """
        This function will validate the user and on successful response it will generate the JWT token
        :param request: request django object
        :return: json response
        """

    obj = None
    try:
        if request.method == POST_REQUEST:
            request_payload = json.loads(request.body.decode(UTF8_FORMAT))

            obj = Logout(request_payload)
            return obj.get_logout_user()

        log_debug(METHOD_NOT_ALLOWED)
        return json_MethodNotAllowed
    except Exception as e:
        excMsg = "Authentication API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
