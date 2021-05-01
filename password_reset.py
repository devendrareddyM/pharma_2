"""
File                :   user_password_reset

Description         :   This file is having the programs for reset the user's password.

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   22/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import ssl

import jwt
import pandas as pd
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Queries import CHECK_LOGIN_ID, CHECK_AUTHENTICATION_QUERY, \
    CHANGE_RESET_USER_PASSWORD_QUERY
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _RequestValidation, _TokenValidation
from utilities.Api_Response import json_MethodNotAllowed, json_InternalServerError, HTTP_400_BAD_REQUEST, \
    HTTP_500_INTERNAL_SERVER_ERROR, asert_res, HTTP_403_FORBIDDEN
from utilities.Constants import MESSAGE_KEY, STATUS_KEY, DB_ERROR, USERNAME_KEY, USERNAME_NOT_REGISTERED, \
    USERPASSWORD_KEY, SALT_KEY, CHANGE_ERROR, RESET_SUCCESSFULLY, PUT_REQUEST, METHOD_NOT_ALLOWED, TOKEN, LOGIN_ID, \
    USER_TYPE
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import *
from utilities.TokenManagement import TokenManagement
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class UserPasswordReset(_PostGreSqlConnection):
    """
    This class will help to reset the user's password for the existing user
    """

    def __init__(self, request_payload=None):
        """
        This will call the parent class to validate the connection and request payload
        :param loggedin_userid_details: logged in user id
        :param request_payload: request payload
        """
        super().__init__()
        self._request_payload = request_payload

    def reset_user_password(self):
        """
        This function will reset the user password
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}
            return self.__reset_user_password_query()

        except Exception as e:
            log_info()
            return asert_res(e)

    def __reset_user_password_query(self):
        """
        This function will execute the query for resetting the user password
        :return: Json object
        """
        try:
            with transaction.atomic():
                try:
                    decode_jwt = TokenManagement.is_valid_jwt(self._request_payload[TOKEN])
                    login_id = decode_jwt['login_id']
                    self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(login_id))
                    admin = pd.DataFrame(self._psql_session.fetchall())
                    check_admin = None
                    if not admin.empty:
                        check_admin = admin[USER_TYPE].iloc[0]
                        receiver = admin['email_id'].iloc[0]
                    else:
                        return JsonResponse({MESSAGE_KEY: "USER IS NOT REGISTER WITH US"})

                    if check_admin == 'Admin' or check_admin == 'Super Admin':
                        self._psql_session.execute(CHECK_LOGIN_ID)
                        loginids = self._psql_session.fetchall()
                        login_ids = pd.DataFrame(loginids)
                        log_in_id = list(login_ids["login_id"])
                        if login_id in log_in_id:
                            obj = HashingSalting()
                            hash_value, salt = obj.get_hashed_password(self._request_payload[USERPASSWORD_KEY])

                            self._psql_session.execute(CHANGE_RESET_USER_PASSWORD_QUERY.format(SALT_KEY, salt,
                                                                                               USERPASSWORD_KEY,
                                                                                               hash_value,
                                                                                               LOGIN_ID, login_id
                                                                                               ))
                            # s = smtplib.SMTP('smtp.gmail.com', 587)
                            # s.starttls()
                            # sender = 'devendrareddy058@gmail.com'
                            # s.login(sender, 'Mdevendra@1996')
                            # receiver_mail = receiver
                            # msg = MIMEMultipart('alternative')
                            # msg['Subject'] = "Password"
                            # msg['From'] = sender
                            # msg['To'] = receiver_mail
                            # context = """Hi.\n Here is the you are Password .\n {}""".format(
                            #     self._request_payload[USERPASSWORD_KEY])
                            # part1 = MIMEText(context, 'plain')
                            # msg.attach(part1)
                            # s.sendmail(sender, 'anna.jesline@livnsense.com', msg.as_string())
                            # s.quit()
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

                except jwt.ExpiredSignatureError:
                    return JsonResponse({MESSAGE_KEY: "Token Expired"})

                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: CHANGE_ERROR}, status=HTTP_400_BAD_REQUEST)

                return JsonResponse({MESSAGE_KEY: RESET_SUCCESSFULLY})

        except IntegrityError as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

        except Exception as e:

            log_error("Exception due to : %s" + str(e))

            return json_InternalServerError

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def resetpassword_user(request):
    """
    This function will reset the password for the existing user
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:
            request_payload = _RequestValidation().validate_request(request, [TOKEN, USERPASSWORD_KEY])
            obj = UserPasswordReset(request_payload)
            return obj.reset_user_password()

        log_debug(METHOD_NOT_ALLOWED)
        return json_MethodNotAllowed

    except Exception as e:
        excMsg = "reset_password API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
