"""
File                :   user_password_reset

Description         :   This file is having the programs for reset the user's password.

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   22/2/19

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
import datetime
import ssl

import jwt
import pandas as pd
from django.db import IntegrityError, transaction
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from psycopg2.errorcodes import INTERNAL_ERROR

from ApplicationInterface.Database.Configuration import env_settings
from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, CHECK_LOGIN_ID
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, _RequestValidation
from utilities.Api_Configuration import ENV
from utilities.Api_Response import json_MethodNotAllowed, json_InternalServerError, HTTP_400_BAD_REQUEST, asert_res, \
    HTTP_500_INTERNAL_SERVER_ERROR, HTTP_401_UNAUTHORIZED, HTTP_403_FORBIDDEN
from utilities.Constants import MESSAGE_KEY, STATUS_KEY, DB_ERROR, USERNAME_KEY, USERNAME_NOT_REGISTERED, \
    USERPASSWORD_KEY, SALT_KEY, CHANGE_ERROR, RESET_SUCCESSFULLY, PUT_REQUEST, METHOD_NOT_ALLOWED, USER_TYPE, LOGIN_ID, \
    GET_REQUEST
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_info, log_debug, log_error
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from utilities.TokenManagement import TokenManagement


class ForgotPassword(_PostGreSqlConnection):
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

    def forgot_password(self):
        """
        This function will reset the user password
        :return: Json payload
        """
        try:
            assert self._db_connection, {STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR, MESSAGE_KEY: DB_ERROR}

            return self.__forgot_user_password_query()

        except Exception as e:
            log_info()
            return asert_res(e)

    def __forgot_user_password_query(self):
        """
        This function will execute the query for resetting the user password
        :return: Json object
        """
        try:
            with transaction.atomic():

                self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self._request_payload[LOGIN_ID]))
                result_set = self._psql_session.fetchall()

                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)

                Permission = pd.DataFrame(result_set)
                self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self._request_payload[LOGIN_ID]))
                admin = pd.DataFrame(self._psql_session.fetchall())
                if not admin.empty:
                    check_admin = admin[USER_TYPE].iloc[0]
                    receiver = admin['email_id'].iloc[0]
                else:
                    return JsonResponse({MESSAGE_KEY: "USER IS NOT REGISTER WITH US"})

                if Permission[USER_TYPE].iloc[0] == 'Super Admin' or check_admin == 'Admin' or check_admin == 'Super ' \
                                                                                                              'Admin':
                    self._psql_session.execute(CHECK_LOGIN_ID)
                    loginids = self._psql_session.fetchall()
                    login_ids = pd.DataFrame(loginids)
                    log_in_id = list(login_ids["login_id"])
                    if self._request_payload[LOGIN_ID] in log_in_id:
                        jwt_token = TokenManagement().add_jwt(
                            {
                                LOGIN_ID: self._request_payload[LOGIN_ID],
                                'exp': datetime.datetime.utcnow() + datetime.timedelta(seconds=180)
                            }
                        )
                        # s = smtplib.SMTP('smtp.gmail.com', 587)
                        # s.starttls()
                        # sender = 'devendrareddy058@gmail.com'
                        # s.login(sender, 'Mdevendra@1996')
                        # receiver_mail = receiver
                        # msg = MIMEMultipart('alternative')
                        # msg['Subject'] = "Reset Password"
                        # msg['From'] = sender
                        # msg['To'] = receiver_mail
                        # if ENV:
                        #     context = """Hi
                        #     Here is the link for the reset the password
                        #     click on it
                        #     "https://p66dev.ingenero.ml/p66/users/password-reset/{}" """.format(jwt_token)
                        #
                        # else:
                        #     context = """Hi
                        #     Here is the link for the reset the password
                        #     click on it
                        #     "https://p66.ingenero.ml/p66/users/password-reset/{}" """.format(jwt_token)
                        #
                        # part1 = MIMEText(context, 'html')
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
                            if env_settings == 'DEV_':
                                context = ("Hi\n"
                                           "Here is the link for the reset password\n"
                                           "click on it\n"
                                           "https://p66dev.ingenero.ml/#/auth/reset-password/{}".format(jwt_token))

                            else:
                                context = ("Hi\n"
                                           "Here is the link for the reset the password\n"
                                           "click on it\n"
                                           "https://p66.ingenero.ml/#/auth/reset-password/{}".format(jwt_token))

                            part1 = MIMEText(context, 'plain')
                            msg.attach(part1)
                            server.sendmail('idmssupport@ingenero.com', 'idmssupport@ingenero.com', msg.as_string())
                            server.quit()

                else:
                    return JsonResponse({MESSAGE_KEY: "ACCESS DENIED"})

                if not self._psql_session.rowcount:
                    return JsonResponse({MESSAGE_KEY: CHANGE_ERROR}, status=HTTP_400_BAD_REQUEST)

                return JsonResponse({MESSAGE_KEY: "PASSWORD RESET LINK SENT YOUR MAIL! CHECK IN SPAM OR INBOX"})

        except IntegrityError as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        self._psql_session.close()


@csrf_exempt
def for_got_password(request):
    """
    This function will reset the password for the existing user
    :param userid:
    :param request: request django object
    :return: json object
    """

    obj = None

    try:
        if request.method == PUT_REQUEST:
            request_payload = _RequestValidation().validate_request(request, [LOGIN_ID])
            obj = ForgotPassword(request_payload)
            return obj.forgot_password()
        return json_MethodNotAllowed
    except Exception as e:
        excMsg = "forgot_users API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
