""""
File                :   add_user.py

Description         :   This will contain all the user creating programs

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :  12/6/2020

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

import jwt
import pandas as pd
from django.db import transaction, IntegrityError
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import ADD_USER, CHECK_AUTHENTICATION_QUERY, CHECK_LOGIN_ID, GET_ADMIN_COUNT, \
    GET_ADMIN_USER
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, _RequestValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_400_BAD_REQUEST, \
    json_MethodNotAllowed, json_InternalServerError, asert_res, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import POST_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, USERPASSWORD_KEY, USEREMAIL_KEY, \
    CREATED_SUCCESSFULLY, \
    DB_ERROR, LOGIN_ID, NAME, DESIGNATION, USER_TYPE, STATUS_KEY, EAMIL_NOTIFICATION, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


def NOT_AUTHORISED(args):
    pass


class CreateUser(_PostGreSqlConnection):
    """
    This class will help to create or add the new user
    """

    def __init__(self, loggedin_user_details, request_payload):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_user_details: loggedin details
        :param request_payload: request payload
        """
        super().__init__()
        self.loggedin_user_details = loggedin_user_details
        self._request_payload = request_payload

    def add_user(self):
        """
        This function will add the user details
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__add_user_query()

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    @transaction.atomic
    def __add_user_query(self):
        """
        This function will execute the query for creating a new user
        :return: Json paylaod
        """
        self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_user_details[LOGIN_ID]))
        result_set = self._psql_session.fetchall()
        if not self._psql_session.rowcount:
            return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)
        Permission = pd.DataFrame(result_set)
        if Permission[USER_TYPE].iloc[0] == 'Super Admin':
            if self._request_payload[USER_TYPE] == 'Admin':
                self._psql_session.execute(GET_ADMIN_COUNT)
                all_admin = pd.DataFrame(self._psql_session.fetchall())
                if not all_admin.empty:
                    admin_count = all_admin['count'].iloc[0].item()
                else:
                    admin_count = None
                self._psql_session.execute(GET_ADMIN_USER)
                admin = pd.DataFrame(self._psql_session.fetchall())
                if not admin.empty:
                    admin_user = admin['limit_value'].iloc[0].item()
                else:
                    admin_user = None
                if admin_user <= admin_count:
                    return JsonResponse({MESSAGE_KEY: "MAXIMUM NUMBER OF ADMIN USER COUNT EXCEEDED"})

            self._psql_session.execute(CHECK_LOGIN_ID)
            loginids = self._psql_session.fetchall()
            login_ids = pd.DataFrame(loginids)
            log_in_id = list(login_ids["login_id"])
            email_id = list(login_ids["email_id"])
            if self._request_payload[LOGIN_ID] not in log_in_id and self._request_payload[
                USEREMAIL_KEY] not in email_id:
                try:
                    with transaction.atomic():
                        obj = HashingSalting()
                        hash_value, salt_value = obj.get_hashed_password(self._request_payload[USERPASSWORD_KEY])
                        self._psql_session.execute(ADD_USER.format(
                            self._request_payload[NAME],
                            self._request_payload[LOGIN_ID],
                            self._request_payload[USEREMAIL_KEY],
                            self._request_payload[DESIGNATION],
                            self._request_payload[USER_TYPE],
                            self._request_payload[EAMIL_NOTIFICATION],
                            hash_value, salt_value
                        ))

                        return JsonResponse({MESSAGE_KEY: CREATED_SUCCESSFULLY})

                except IntegrityError as e:
                    log_error("Exception due to : %s" + str(e))
                    return json_InternalServerError
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
                    return json_InternalServerError
            else:
                return JsonResponse({MESSAGE_KEY: "User is all ready registered"})
        else:
            return JsonResponse({MESSAGE_KEY: "User is not have permission to add"})

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def create_user(request):
    """
    This function will crete a new user
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == POST_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [
                        NAME,
                        LOGIN_ID,
                        USEREMAIL_KEY,
                        DESIGNATION,
                        USER_TYPE,
                        EAMIL_NOTIFICATION,
                        USERPASSWORD_KEY
                    ])

                    obj = CreateUser(loggedin_user_details, request_payload)
                    return obj.add_user()
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
        excMsg = "Add_user API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
