"""
File                :  download_notifications.py

Description         :   This file will return the notifications in the pdf format

Author              :   LivNSense Technologies

Date Created        :   19-06-2020

Date Last modified :    19-06-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import os
import time as t

import jwt
import numpy as np
import pandas as pd
from django.http import HttpResponse
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from pandas import to_datetime
from ApplicationInterface.Database.Queries import CHECK_AUTHENTICATION_QUERY, \
    GET_PERMISSION, DOWNLOAD_NOTIFICATION_PERIOD, DOWNLOAD_NOTIFICATIONS_LIST, DOWNLOAD_NOTIFICATION_ERROR_DETAILS, \
    DOWNLOAD_NOTIFICATIONS_LIST_TMT, \
    DOWNLOAD_NOTIFICATION_ERROR_DETAILS_TMT, DOWNLOAD_BENCH_MARK_ERROR
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from ApplicationInterface.Notifications.utils import render_to_pdf
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, GET_REQUEST, \
    METHOD_NOT_ALLOWED, LOGIN_ID, OVER_HEAD_PDI_TABLE, OVER_HEAD_MODULE, OUTAGE_MODULE, \
    OUTGAE_TABLE, HGI_TABLE, HGI_MODULE, TMT_RESULT_TABLE, TMT_FURNACE_A_MODULE, TMT_SPALL_RESULT, \
    TMT_FURNACE_A_SPALL_MODULE, ERROR_TMT_A, TMT_FURNACE_B_MODULE, TMT_FURNACE_B_SPALL_MODULE, ERROR_TMT_B, \
    START_DATE_REQUEST, END_DATE_REQUEST, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class Notifications(_PostGreSqlConnection):
    """
    This class is responsible for downloading notifications from the Database and return as pdf format
    """

    def __init__(self, loggedin_userid_details, query_params=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details
        self.query_params = query_params

    def get_notifications(self, request):
        """
        This will download notifications from the database
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            dict_data = {}

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
            if 'Download Notifications for Selected Dates' in permissions:
                self._psql_session.execute(DOWNLOAD_NOTIFICATION_PERIOD)
                download_period = pd.DataFrame(self._psql_session.fetchall())
                if not download_period.empty:
                    download_time_period = int(download_period['value'].iloc[0])
                else:
                    download_time_period = None

                if self.query_params:
                    query_params = {
                        START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                        END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                    }
                    d0 = np.datetime64(query_params[START_DATE_REQUEST]).astype('int64')
                    d1 = np.datetime64(query_params[END_DATE_REQUEST]).astype('int64')
                    """
                    Calculating number of days between start date and end date
                    delta = (d1 - d0) / (24 * 3600000)
                    """
                    delta = (d1 - d0) / (24 * 3600000)

                    if delta <= download_time_period:
                        tm = t.time()
                        LAST_MODIFIED_DATE = pd.to_datetime(tm, unit='s').strftime('%d/%b/%Y %H:%M')
                        start_date = to_datetime(query_params[START_DATE_REQUEST], format='%Y-%m-%dT%H:%M:%S.%fZ')
                        converted_start_date = pd.to_datetime(start_date).strftime('%d-%b-%Y %H:%M:%S')
                        end_date = to_datetime(query_params[END_DATE_REQUEST], format='%Y-%m-%dT%H:%M:%S.%fZ')
                        converted_end_date = pd.to_datetime(end_date).strftime('%d-%b-%Y %H:%M:%S')
                        notifications_duration = str(converted_start_date) + " to " + str(converted_end_date)
                        dict_data["current_time"] = LAST_MODIFIED_DATE
                        dict_data["duration"] = notifications_duration
                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATIONS_LIST.format(OVER_HEAD_PDI_TABLE, OVER_HEAD_MODULE,
                                                               query_params[START_DATE_REQUEST],
                                                               query_params[END_DATE_REQUEST]))
                        overhead_notifications = pd.DataFrame(self._psql_session.fetchall())

                        if not overhead_notifications.empty:
                            overhead_notifications = overhead_notifications[['Date Time', 'Category', 'Notification']]

                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATION_ERROR_DETAILS.format(OVER_HEAD_MODULE,
                                                                       query_params[START_DATE_REQUEST],
                                                                       query_params[END_DATE_REQUEST]))
                        overhead_alerts = pd.DataFrame(self._psql_session.fetchall())

                        if not overhead_alerts.empty:
                            overhead_alerts['Date Time'] = overhead_alerts['Date Time'].dt.tz_convert(None)
                            overhead_alerts['Date Time'] = overhead_alerts['Date Time'].dt.strftime('%d/%b/%Y %H:%M')
                            alert = overhead_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            overhead_alerts = alert_group[['Date Time', 'Category', 'Notification']]

                        pdi_df = [overhead_notifications, overhead_alerts]
                        pdi_dataFrame = pd.concat(pdi_df)
                        pdi_dataFrame = pdi_dataFrame.style.set_properties(subset=['Notification'],
                                                                           **{'width': '400px'})

                        if not overhead_notifications.empty or not overhead_alerts.empty:
                            dict_data["overhead_pdi"] = pdi_dataFrame.render

                        self._psql_session.execute(DOWNLOAD_NOTIFICATIONS_LIST.format(OUTGAE_TABLE, OUTAGE_MODULE,
                                                                                      query_params[START_DATE_REQUEST],
                                                                                      query_params[END_DATE_REQUEST]))
                        outage_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not outage_notifications.empty:
                            outage_notifications = outage_notifications[['Date Time', 'Category', 'Notification']]

                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATION_ERROR_DETAILS.format(OUTAGE_MODULE,
                                                                       query_params[START_DATE_REQUEST],
                                                                       query_params[END_DATE_REQUEST]))
                        outage_alerts = pd.DataFrame(self._psql_session.fetchall())
                        if not outage_alerts.empty:
                            outage_alerts['Date Time'] = outage_alerts['Date Time'].dt.tz_convert(None)
                            outage_alerts['Date Time'] = outage_alerts['Date Time'].dt.strftime('%d/%b/%Y %H:%M')
                            alert = outage_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            outage_alerts = alert_group[['Date Time', 'Category', 'Notification']]

                        outage_df = [outage_notifications, outage_alerts]
                        outage_dataFrame = pd.concat(outage_df)
                        outage_dataFrame = outage_dataFrame.style.set_properties(subset=['Notification'],
                                                                                 **{'width': '400px'})

                        if not outage_notifications.empty or not outage_alerts.empty:
                            dict_data["outage"] = outage_dataFrame.render

                        self._psql_session.execute(DOWNLOAD_NOTIFICATIONS_LIST.format(HGI_TABLE, HGI_MODULE,
                                                                                      query_params[START_DATE_REQUEST],
                                                                                      query_params[END_DATE_REQUEST]))
                        hgi_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not hgi_notifications.empty:
                            hgi_notifications = hgi_notifications[['Date Time', 'Category', 'Notification']]
                        self._psql_session.execute(DOWNLOAD_NOTIFICATION_ERROR_DETAILS.format(HGI_MODULE,
                                                                                              query_params[
                                                                                                  START_DATE_REQUEST],
                                                                                              query_params[
                                                                                                  END_DATE_REQUEST]))
                        hgi_alerts = pd.DataFrame(self._psql_session.fetchall())
                        if not hgi_alerts.empty:
                            hgi_alerts['Date Time'] = hgi_alerts['Date Time'].dt.tz_convert(None)
                            hgi_alerts['Date Time'] = hgi_alerts['Date Time'].dt.strftime('%d/%b/%Y %H:%M')
                            alert = hgi_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            hgi_alerts = alert_group[['Date Time', 'Category', 'Notification']]

                        hgi_df = [hgi_notifications, hgi_alerts]
                        hgi_dataFrame = pd.concat(hgi_df)
                        hgi_dataFrame = hgi_dataFrame.style.set_properties(subset=['Notification'],
                                                                           **{'width': '400px'})

                        if not hgi_notifications.empty or not hgi_alerts.empty:
                            dict_data["hgi"] = hgi_dataFrame.render

                        """"""""""""""""""""""""""""""
                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATIONS_LIST_TMT.format(TMT_RESULT_TABLE, TMT_FURNACE_A_MODULE,
                                                                   query_params[START_DATE_REQUEST],
                                                                   query_params[END_DATE_REQUEST]))
                        tmt_furnace_A_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_A_notifications.empty:
                            tmt_furnace_A_notifications = tmt_furnace_A_notifications[
                                ['Date Time', 'Category', 'Notification']]
                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATIONS_LIST_TMT.format(TMT_SPALL_RESULT, TMT_FURNACE_A_SPALL_MODULE,
                                                                   query_params[START_DATE_REQUEST],
                                                                   query_params[END_DATE_REQUEST]))
                        tmt_furnace_spall_A_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_spall_A_notifications.empty:
                            tmt_furnace_spall_A_notifications = tmt_furnace_spall_A_notifications[
                                ['Date Time', 'Category', 'Notification']]

                        self._psql_session.execute(DOWNLOAD_NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_A,
                                                                                                  query_params[
                                                                                                      START_DATE_REQUEST],
                                                                                                  query_params[
                                                                                                      END_DATE_REQUEST]))
                        tmt_furnace_A_alerts = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_A_alerts.empty:
                            tmt_furnace_A_alerts['Date Time'] = tmt_furnace_A_alerts['Date Time'].dt.tz_convert(None)
                            tmt_furnace_A_alerts['Date Time'] = tmt_furnace_A_alerts['Date Time'].dt.strftime(
                                '%d/%b/%Y %H:%M')
                            alert = tmt_furnace_A_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            tmt_furnace_A_alerts = alert_group[['Date Time', 'Category', 'Notification']]

                        tmt_A_df = [tmt_furnace_A_notifications, tmt_furnace_spall_A_notifications,
                                    tmt_furnace_A_alerts]
                        tmt_A_dataFrame = pd.concat(tmt_A_df)
                        tmt_A_dataFrame = tmt_A_dataFrame.style.set_properties(subset=['Notification'],
                                                                               **{'width': '400px'})

                        if not tmt_furnace_A_notifications.empty or not tmt_furnace_spall_A_notifications.empty or not tmt_furnace_A_alerts.empty:
                            dict_data["furnace_tmt_A"] = tmt_A_dataFrame.render

                        """ ''''''''''''' """

                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATIONS_LIST_TMT.format(TMT_RESULT_TABLE, TMT_FURNACE_B_MODULE,
                                                                   query_params[START_DATE_REQUEST],
                                                                   query_params[END_DATE_REQUEST]))
                        tmt_furnace_B_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_B_notifications.empty:
                            tmt_furnace_B_notifications = tmt_furnace_B_notifications[
                                ['Date Time', 'Category', 'Notification']]
                        self._psql_session.execute(
                            DOWNLOAD_NOTIFICATIONS_LIST_TMT.format(TMT_SPALL_RESULT, TMT_FURNACE_B_SPALL_MODULE,
                                                                   query_params[START_DATE_REQUEST],
                                                                   query_params[END_DATE_REQUEST]))
                        tmt_furnace_spall_B_notifications = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_spall_B_notifications.empty:
                            tmt_furnace_spall_B_notifications = tmt_furnace_spall_B_notifications[
                                ['Date Time', 'Category', 'Notification']]

                        self._psql_session.execute(DOWNLOAD_NOTIFICATION_ERROR_DETAILS_TMT.format(ERROR_TMT_B,
                                                                                                  query_params[
                                                                                                      START_DATE_REQUEST],
                                                                                                  query_params[
                                                                                                      END_DATE_REQUEST]))
                        tmt_furnace_B_alerts = pd.DataFrame(self._psql_session.fetchall())
                        if not tmt_furnace_B_alerts.empty:
                            tmt_furnace_B_alerts['Date Time'] = tmt_furnace_B_alerts['Date Time'].dt.tz_convert(None)
                            tmt_furnace_B_alerts['Date Time'] = tmt_furnace_B_alerts['Date Time'].dt.strftime(
                                '%d/%b/%Y %H:%M')
                            alert = tmt_furnace_B_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            tmt_furnace_B_alerts = alert_group[['Date Time', 'Category', 'Notification']]

                        tmt_B_df = [tmt_furnace_B_notifications, tmt_furnace_spall_B_notifications,
                                    tmt_furnace_B_alerts]
                        tmt_B_dataFrame = pd.concat(tmt_B_df)
                        tmt_B_dataFrame = tmt_B_dataFrame.style.set_properties(subset=['Notification'],
                                                                               **{'width': '400px'})

                        if not tmt_furnace_B_notifications.empty or not tmt_furnace_spall_B_notifications.empty or not tmt_furnace_B_alerts.empty:
                            dict_data["furnace_tmt_B"] = tmt_B_dataFrame.render

                        """ """""""""""""""""""""""""""""""""""""""""""""""""""" "" """
                        self._psql_session.execute(
                            DOWNLOAD_BENCH_MARK_ERROR.format(query_params[START_DATE_REQUEST],
                                                             query_params[END_DATE_REQUEST]))
                        benchmark_alerts = pd.DataFrame(self._psql_session.fetchall())

                        if not benchmark_alerts.empty:
                            benchmark_alerts['Date Time'] = benchmark_alerts['Date Time'].dt.tz_convert(None)
                            benchmark_alerts['Date Time'] = benchmark_alerts['Date Time'].dt.strftime(
                                '%d/%b/%Y %H:%M')
                            alert = benchmark_alerts[['tag_name', 'Date Time', 'Notification']]
                            alert_group = alert.groupby(['Date Time', 'Notification'])['tag_name'].apply(
                                ', '.join).reset_index()
                            alert_group['Notification'] = alert_group['Notification'].str.cat(alert_group['tag_name'],
                                                                                              sep=" - ")
                            alert_group['Category'] = 'Alert'
                            benchmark_alerts = alert_group[['Date Time', 'Category', 'Notification']]
                            benchmark_dataFrame = benchmark_alerts
                            benchmark_dataFrame = benchmark_dataFrame.style.set_properties(subset=['Notification'],
                                                                                           **{'width': '400px'})

                            dict_data["benchmarking"] = benchmark_dataFrame.render
                        SITE_ROOT = os.path.dirname(os.path.realpath(__file__))
                        # image_1 = "\..\..\\templates\\p66logo.png"
                        image_1 = "/../..//templates//p66logo.png"
                        image_1_path = SITE_ROOT + image_1
                        # image_2 = "\..\..\\templates\\ingenero_logo.png"
                        image_2 = "/../..//templates//ingenero_logo.png"
                        image_2_path = SITE_ROOT + image_2
                        dict_data["image_1"] = image_1_path
                        dict_data["image_2"] = image_2_path
                        pdf = render_to_pdf('invoice.html', dict_data)
                        if pdf:
                            response = HttpResponse(pdf, content_type='application/pdf')
                            filename = "Notifications.pdf"
                            content = "inline; filename=%s" % filename
                            download = request.GET.get("download")
                            if download:
                                content = "attachment; filename=%s" % filename
                            response['Content-Disposition'] = content
                            return response
                        return HttpResponse("Not found")
                    else:
                        return JsonResponse(
                            {MESSAGE_KEY: "The days to download exceeds the default download time period"}, safe=False)
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
def download_notifications_data(request):
    """
    This function will download the notifications from the database
    :param request: request django object
    :return: json response
    """
    query_params, obj = None, None

    try:
        query_params = request

    except:
        pass

    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = Notifications(loggedin_user_details, query_params)
                return obj.get_notifications(request)
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
