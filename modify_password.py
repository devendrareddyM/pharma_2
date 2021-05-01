"""
File                :   user_password_reset

Description         :   This file is having the programs for reset the user's password.

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   22/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import jwt
import pandas as pd
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Configuration import ENV, DEVELOPMENT
from ApplicationInterface.Database.Queries import CHECK_LOGIN_ID, CHECK_AUTHENTICATION_QUERY, \
    CHANGE_RESET_USER_PASSWORD_QUERY
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _RequestValidation, _TokenValidation
from utilities.Api_Response import json_MethodNotAllowed, json_InternalServerError, HTTP_400_BAD_REQUEST, \
    HTTP_500_INTERNAL_SERVER_ERROR, asert_res, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import MESSAGE_KEY, STATUS_KEY, DB_ERROR, USERNAME_NOT_REGISTERED, \
    USERPASSWORD_KEY, SALT_KEY, PUT_REQUEST, METHOD_NOT_ALLOWED, LOGIN_ID, \
    USER_TYPE, USERFUTUREPASSWORD_KEY, CHANGED_SUCCESSFULLY, PASSWORD_WRONG, RESET_ERROR, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import *


class UserPasswordChange(_PostGreSqlConnection):
    """
    This class will help to reset the user's password for the existing user
    """

    def __init__(self, loggedin_userid_details, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: logged in user id
        :param request_payload: request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self._request_payload = request_payload

    def change_user_password(self):
        """
        This function will reset the user password
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            return self.__change_user_password_query()

        except Exception as e:
            log_info()
            return asert_res(e)

    def __change_user_password_query(self):
        """
        This function will execute the query for resetting the user password
        :return: Json object
        """
        try:
            with transaction.atomic():

                if DEVELOPMENT:
                    self._psql_session.execute(
                        CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
                else:
                    self._psql_session.execute(
                        CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: USERNAME_NOT_REGISTERED}, status=HTTP_400_BAD_REQUEST)
                result_set = self._psql_session.fetchall()
                Permission = pd.DataFrame(result_set)
                self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self._request_payload[LOGIN_ID]))
                admin = pd.DataFrame(self._psql_session.fetchall())
                if not admin.empty:
                    check_admin = admin[USER_TYPE].iloc[0]
                    receiver = admin['email_id'].iloc[0]
                else:
                    return JsonResponse({MESSAGE_KEY: "USER IS NOT REGISTER WITH US"})

                if Permission[USER_TYPE].iloc[0] == 'Super Admin':
                    self._psql_session.execute(CHECK_LOGIN_ID)
                    loginids = self._psql_session.fetchall()
                    login_ids = pd.DataFrame(loginids)
                    log_in_id = list(login_ids["login_id"])
                    if self._request_payload[LOGIN_ID] in log_in_id:
                        obj = HashingSalting()
                        hash, salt = obj.get_hashed_password(self._request_payload[USERPASSWORD_KEY])

                        self._psql_session.execute(CHANGE_RESET_USER_PASSWORD_QUERY.format(SALT_KEY, salt,
                                                                                           USERPASSWORD_KEY, hash,
                                                                                           LOGIN_ID,
                                                                                           self._request_payload[
                                                                                               LOGIN_ID]))
                        port = 465  # For SSL
                        smtp_server = "smtpout.ingenero.com"

                        context = ssl.create_default_context()
                        with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
                            print(server.login('idmssupport@ingenero.com', 'Ap5sn%k*20'))
                            msg = MIMEMultipart('alternative')
                            msg['Subject'] = "Reset Password"
                            msg['From'] = 'idmssupport@ingenero.com'
                            msg['To'] = 'idmssupport@ingenero.com'

                            context = """Hi.\n Here is the you are Password .\n {}""".format(
                                self._request_payload[USERPASSWORD_KEY])
                            part1 = MIMEText(context, 'plain')
                            msg.attach(part1)
                            server.sendmail('idmssupport@ingenero.com', 'idmssupport@ingenero.com',
                                            msg.as_string())

                            server.quit()

                        if self._psql_session.rowcount:
                            return JsonResponse({MESSAGE_KEY: "PASSWORD UPDATED SUCCESSFULLY AND PASSWORD SENT TO "
                                                              "MAIL"})

                        return JsonResponse({MESSAGE_KEY: RESET_ERROR}, status=HTTP_400_BAD_REQUEST)

                    return JsonResponse({MESSAGE_KEY: "USER IS NOT REGISTER WITH US"})
                else:
                    return JsonResponse({MESSAGE_KEY: "NON ADMIN USER IS NOT ACCESS TO CHANGE PASSWORD"})

        except IntegrityError as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

        except Exception as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def change_user_password(request):
    """
    This function will reset the password for the existing user
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    request_payload = _RequestValidation().validate_request(request, [USERPASSWORD_KEY,
                                                                                      LOGIN_ID])
                    obj = UserPasswordChange(loggedin_user_details, request_payload)
                    return obj.change_user_password()
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
        excMsg = "modify_password API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
