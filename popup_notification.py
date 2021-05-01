"""
File                :   Popup_Notification

Description         :   Sending data for License Expiry date details

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   04/09/20

Date Modified       :

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""

from datetime import datetime

import jwt
from django.http import JsonResponse

from ApplicationInterface.Database.Queries import LICENSE_DETAILS
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_403_FORBIDDEN, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from utilities.Constants import CONFIGURATION_DATE_1, CONFIGURATION_DATE_2, GET_REQUEST, MESSAGE_KEY, \
    METHOD_NOT_ALLOWED, HTTP_AUTHORIZATION_TOKEN
from utilities.Http_Request import error_instance
from utilities.LoggerFile import  log_debug, log_error



class Popup_Notification(_PostGreSqlConnection):
    """
    This class will help to get the license expiry details
    """

    def __init__(self):
        super().__init__()

    def license_expiry(self):
                try:
                    self._psql_session.execute(LICENSE_DETAILS)
                    license_expiry = self._psql_session.fetchone()
                    license_end_date = (license_expiry['end_date'] - datetime.now()).days
                    if CONFIGURATION_DATE_1 == license_end_date:
                        dict_data  = {
                                      "days_remaining": license_end_date,
                                      "notify_status":True,
                                      "message":"The license is expected to expire in the next {} days. Please "
                                                "contact your iSense4i application administrator for the renewal of "
                                                "the platform subscription.".format(license_end_date)

                                      }
                    elif license_end_date <= CONFIGURATION_DATE_2:
                        dict_data = {
                            "days_remaining": license_end_date,
                            "notify_status": True,
                            "message": "The license is expected to expire in the next {} days. Please "
                                                "contact your iSense4i application administrator for the renewal of "
                                                "the platform subscription.".format(license_end_date)

                        }
                    else:
                        dict_data = {
                                    "days_remaining": license_end_date,
                                    "notify_status":False,
                                    "message": "The license is expected to expire in the next {} days. Please "
                                                "contact your iSense4i application administrator for the renewal of "
                                                "the platform subscription.".format(license_end_date)
                                    }

                    return JsonResponse(dict_data,safe=False)
                except Exception as e:
                    log_error(e)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()



def get_license_expiry_details(request):
    obj = None
    try:
        if request.method == GET_REQUEST:
            jwt_value = _TokenValidation().validate_token(request)
            if jwt_value:
                obj = Popup_Notification()
                return obj.license_expiry()
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    except jwt.ExpiredSignatureError:
        token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
        return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj

