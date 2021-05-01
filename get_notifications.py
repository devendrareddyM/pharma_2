"""
File                :  get_notifications.py

Description         :   This file will return the notifications of all modules of p66

Author              :   LivNSense Technologies

Date Created        :   24-06-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, \
    GET_PERMISSION, DOWNLOAD_NOTIFICATION_PERIOD, NOTIFICATIONS_LIST, DEFAULT_NOTIFICATION_VIEW, \
    NOTIFICATION_ERROR_DETAILS, NOTIFICATION_ERROR_DETAILS_TMT, NOTIFICATIONS_LIST_TMT, BENCH_MARK_ERROR
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, GET_REQUEST, \
    METHOD_NOT_ALLOWED, RECORDS, LOGIN_ID, OVER_HEAD_PDI_TABLE, OVER_HEAD_MODULE, OUTAGE_MODULE, \
    OUTGAE_TABLE, HGI_TABLE, HGI_MODULE, TMT_RESULT_TABLE, TMT_FURNACE_A_MODULE, TMT_SPALL_RESULT, \
    TMT_FURNACE_A_SPALL_MODULE, ERROR_TMT_A, TMT_FURNACE_B_MODULE, TMT_FURNACE_B_SPALL_MODULE, ERROR_TMT_B, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class Notifications(_PostGreSqlConnection):
    """
    This class is responsible for reading data from the Database and perform operation according to LBT algo
    and return JSON
    """

    def __init__(self, loggedin_userid_details):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details

    def get_notifications(self):
        """
        This will get query from the Database for LBT algo
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            system_overview = []
            Benchmarking = []
            self._psql_session.execute(CHECK_AUTHENTICATION_QUERY.format(self.loggedin_userid_details[LOGIN_ID]))
            user = pd.DataFrame(self._psql_session.fetchall())
            if not self._psql_session.rowcount:
                return JsonResponse({MESSAGE_KEY: "LOGIN ID NOT REGISTER WITH US"}, status=HTTP_400_BAD_REQUEST)
            self._psql_session.execute(GET_PERMISSION.format(user['user_type'].iloc[0]))
            permission = pd.DataFrame(self._psql_session.fetchall())
            if not permission.empty:
                permissions = list(permission["feature"])
            else:
                permissions = []
            if 'Notification Menu' in permissions:
                self._psql_session.execute(DOWNLOAD_NOTIFICATION_PERIOD)
                download_period = pd.DataFrame(self._psql_session.fetchall())
                if not download_period.empty:
                    download_time_period = int(download_period['value'].iloc[0])
                else:
                    download_time_period = None
                self._psql_session.execute(DEFAULT_NOTIFICATION_VIEW)
                notification_view = pd.DataFrame(self._psql_session.fetchall())
                if not notification_view.empty:
                    notification_view_time = int(notification_view['value'].iloc[0])
                else:
                    notification_view_time = None

                self._psql_session.execute(NOTIFICATIONS_LIST.format(OVER_HEAD_PDI_TABLE, OVER_HEAD_MODULE,
                                                                     OVER_HEAD_PDI_TABLE, notification_view_time,
                                                                     self.loggedin_userid_details[LOGIN_ID]))
                overhead_notifications = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS.format(OVER_HEAD_MODULE, OVER_HEAD_PDI_TABLE,
                                                                             notification_view_time,
                                                                             self.loggedin_userid_details[LOGIN_ID]))
                overhead_alerts = pd.DataFrame(self._psql_session.fetchall())
                if not overhead_alerts.empty:
                    overhead_alerts['type'] = 'Alert'
                    overhead_notifications = overhead_notifications.append(overhead_alerts, ignore_index=True)
                pdi_overhead_notifications = overhead_notifications.to_dict(orient=RECORDS)
                self._psql_session.execute(NOTIFICATIONS_LIST.format(OUTGAE_TABLE, OUTAGE_MODULE,
                                                                     OUTGAE_TABLE, notification_view_time,
                                                                     self.loggedin_userid_details[LOGIN_ID]))
                outage_notifications = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS.format(OUTAGE_MODULE, OUTGAE_TABLE
                                                                             , notification_view_time,
                                                                             self.loggedin_userid_details[LOGIN_ID]))
                outage_alerts = pd.DataFrame(self._psql_session.fetchall())
                if not outage_alerts.empty:
                    outage_alerts['type'] = 'Alert'
                    outage_notifications = outage_notifications.append(outage_alerts, ignore_index=True)
                coke_drum_outage_notifications = outage_notifications.to_dict(orient=RECORDS)

                self._psql_session.execute(NOTIFICATIONS_LIST.format(HGI_TABLE, HGI_MODULE,
                                                                     HGI_TABLE, notification_view_time,
                                                                     self.loggedin_userid_details[LOGIN_ID]))
                hgi_notifications = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS.format(HGI_MODULE, HGI_TABLE
                                                                             , notification_view_time,
                                                                             self.loggedin_userid_details[LOGIN_ID]))
                hgi_alerts = pd.DataFrame(self._psql_session.fetchall())
                if not hgi_alerts.empty:
                    hgi_alerts['type'] = 'Alert'
                    hgi_notifications = hgi_notifications.append(hgi_alerts, ignore_index=True)
                coke_drum_hgi_notifications = hgi_notifications.to_dict(orient=RECORDS)

                """"""""""""""""""""""""""""""
                self._psql_session.execute(NOTIFICATIONS_LIST_TMT.format(TMT_RESULT_TABLE, TMT_FURNACE_A_MODULE,
                                                                         TMT_RESULT_TABLE, notification_view_time,
                                                                         self.loggedin_userid_details[LOGIN_ID]))
                tmt_furnace_A_notifications = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATIONS_LIST_TMT.format(TMT_SPALL_RESULT, TMT_FURNACE_A_SPALL_MODULE,
                                                                         TMT_SPALL_RESULT, notification_view_time,
                                                                         self.loggedin_userid_details[LOGIN_ID]))
                tmt_furnace_spall_A_notifications = pd.DataFrame(self._psql_session.fetchall())
                tmt_furnace_A_notifications = tmt_furnace_A_notifications.append(tmt_furnace_spall_A_notifications,
                                                                                 ignore_index=True)
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_A, TMT_RESULT_TABLE
                                                                                 , notification_view_time,
                                                                                 self.loggedin_userid_details[
                                                                                     LOGIN_ID]))
                tmt_furnace_A_alerts = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_A, TMT_SPALL_RESULT
                                                                                 , notification_view_time,
                                                                                 self.loggedin_userid_details[
                                                                                     LOGIN_ID]))
                tmt_spall_A_alerts = pd.DataFrame(self._psql_session.fetchall())
                tmt_furnace_A_alerts = tmt_furnace_A_alerts.append(tmt_spall_A_alerts, ignore_index=True)
                if not tmt_furnace_A_alerts.empty:
                    tmt_furnace_A_alerts['type'] = 'Alert'
                    tmt_furnace_A_notifications = tmt_furnace_A_notifications.append(tmt_furnace_A_alerts,
                                                                                     ignore_index=True)
                furnace_H3901A = tmt_furnace_A_notifications.to_dict(orient=RECORDS)

                """ ''''''''''''' """

                self._psql_session.execute(NOTIFICATIONS_LIST_TMT.format(TMT_RESULT_TABLE, TMT_FURNACE_B_MODULE,
                                                                         TMT_RESULT_TABLE, notification_view_time,
                                                                         self.loggedin_userid_details[LOGIN_ID]))
                tmt_furnace_B_notifications = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATIONS_LIST_TMT.format(TMT_SPALL_RESULT, TMT_FURNACE_B_SPALL_MODULE,
                                                                         TMT_SPALL_RESULT, notification_view_time,
                                                                         self.loggedin_userid_details[LOGIN_ID]))
                tmt_furnace_spall_B_notifications = pd.DataFrame(self._psql_session.fetchall())
                tmt_furnace_B_notifications = tmt_furnace_B_notifications.append(tmt_furnace_spall_B_notifications,
                                                                                 ignore_index=True)
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_B, TMT_RESULT_TABLE
                                                                                 , notification_view_time,
                                                                                 self.loggedin_userid_details[
                                                                                     LOGIN_ID]))
                tmt_furnace_B_alerts = pd.DataFrame(self._psql_session.fetchall())
                self._psql_session.execute(NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_B, TMT_SPALL_RESULT
                                                                                 , notification_view_time,
                                                                                 self.loggedin_userid_details[
                                                                                     LOGIN_ID]))
                tmt_spall_B_alerts = pd.DataFrame(self._psql_session.fetchall())
                tmt_furnace_B_alerts = tmt_furnace_B_alerts.append(tmt_spall_B_alerts, ignore_index=True)
                if not tmt_furnace_B_alerts.empty:
                    tmt_furnace_B_alerts['type'] = 'Alert'
                    tmt_furnace_B_notifications = tmt_furnace_B_notifications.append(tmt_furnace_B_alerts,
                                                                                     ignore_index=True)
                furnace_H3901B = tmt_furnace_B_notifications.to_dict(orient=RECORDS)

                """ """""""""""""""""""""""""""""""""""""""""""""""""""" "" """
                self._psql_session.execute(BENCH_MARK_ERROR.format(notification_view_time, self.loggedin_userid_details[
                    LOGIN_ID]))
                bench_mark_alerts = pd.DataFrame(self._psql_session.fetchall())
                if not bench_mark_alerts.empty:
                    bench_mark_alerts['type'] = 'Alert'
                    Benchmarking = bench_mark_alerts.to_dict(orient=RECORDS)

                old_dict = {"Coke Drum :Overhead PDI": pdi_overhead_notifications,
                            "Coke Drum :Outage": coke_drum_outage_notifications,
                            "Coke Drum :HGI": coke_drum_hgi_notifications,
                            "Furnace : H3901A Pass 3 & 4": furnace_H3901A,
                            "Furnace : H3901B Pass 1 & 2": furnace_H3901B,
                            "Benchmarking": Benchmarking,
                            "System Overview": system_overview,
                            "Maximum Values Notification Download Time Period": download_time_period,
                            }
                return JsonResponse(old_dict, safe=False)
            else:
                return JsonResponse({MESSAGE_KEY: "FORBIDDEN ERROR"}, status=HTTP_403_FORBIDDEN)
        except AssertionError as e:
            log_error("Exception occurred due to" + str(e))
            return asert_res(e)

        except Exception as e:
            log_error("Exception occurred due to" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_notifications_data(request):
    """
    This function will get the values for dynamic benchmarking
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = Notifications(loggedin_user_details)
                return obj.get_notifications()
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
        excMsg = "get_notifications API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
