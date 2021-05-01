"""
File                :  get_access_list.py

Description         :   This file will return the access_list of p66

Author              :   LivNSense Technologies

Date Created        :   19-06-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
import yaml

from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import GET_ACCESS_LIST, PERIOD_OF_TIME
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, json_MethodNotAllowed, \
    json_InternalServerError, asert_res, HTTP_400_BAD_REQUEST, HTTP_403_FORBIDDEN, HTTP_401_UNAUTHORIZED
from utilities.Constants import STATUS_KEY, MESSAGE_KEY, DB_ERROR, GET_REQUEST, \
    METHOD_NOT_ALLOWED, RECORDS, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class AccessList(_PostGreSqlConnection):
    """
    This class is responsible for reading data from the Database and perform operation according to LBT algo
    and return JSON
    """

    def __init__(self, query_params=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param query_params: request payload
        """
        super().__init__()
        self.query_params = query_params

    def get_access_list(self):
        """
        This will get query from the Database for LBT algo
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            benchmark = []
            furncae_A = []
            furncae_B = []
            hgi = []
            left_pane = []
            notifications = []
            outage = []
            pdi = []
            settings = []
            system_overview = []
            furnace_a_spall = []
            furnace_b_spall = []
            default_n_view = None
            maximum_n_d_period = None
            password_expiry = None
            self._psql_session.execute(GET_ACCESS_LIST)
            access_list = pd.DataFrame(self._psql_session.fetchall())
            if not access_list.empty:
                df = access_list.groupby('section')
                keys = []
                for name, group in df:
                    keys.append(name)
                if 'Settings' in keys:
                    settings = access_list[access_list['section'].str.contains('Settings')]
                    settings.drop(columns=['section'], inplace=True)
                    settings = yaml.safe_load(settings.to_json(orient=RECORDS))
                if 'HGI' in keys:
                    hgi = access_list[access_list['section'].str.contains('HGI')]
                    hgi.drop(columns=['section'], inplace=True)
                    hgi = yaml.safe_load(hgi.to_json(orient=RECORDS))

                if 'Furnace: H3901B' in keys:
                    furncae_B = access_list[access_list['section'].str.contains('Furnace: H3901B')]
                    furncae_B.drop(columns=['section'], inplace=True)
                    furncae_B = yaml.safe_load(furncae_B.to_json(orient=RECORDS))
                if 'Furnace: H3901A' in keys:
                    furncae_A = access_list[access_list['section'].str.contains('Furnace: H3901A')]
                    furncae_A.drop(columns=['section'], inplace=True)
                    furncae_A = yaml.safe_load(furncae_A.to_json(orient=RECORDS))
                if 'Benchmarking' in keys:
                    benchmark = access_list[access_list['section'].str.contains('Benchmarking')]
                    benchmark.drop(columns=['section'], inplace=True)
                    benchmark = yaml.safe_load(benchmark.to_json(orient=RECORDS))

                if 'Left Pane' in keys:
                    left_pane = access_list[access_list['section'].str.contains('Left Pane')]
                    left_pane.drop(columns=['section'], inplace=True)
                    left_pane = yaml.safe_load(left_pane.to_json(orient=RECORDS))

                if 'Notifications' in keys:
                    notifications = access_list[access_list['section'].str.contains('Notifications')]
                    notifications.drop(columns=['section'], inplace=True)
                    notifications = yaml.safe_load(notifications.to_json(orient=RECORDS))

                if 'Outage' in keys:
                    outage = access_list[access_list['section'].str.contains('Outage')]
                    outage.drop(columns=['section'], inplace=True)
                    outage = yaml.safe_load(outage.to_json(orient=RECORDS))

                if 'PDI' in keys:
                    pdi = access_list[access_list['section'].str.contains('PDI')]
                    pdi.drop(columns=['section'], inplace=True)
                    pdi = yaml.safe_load(pdi.to_json(orient=RECORDS))

                if 'System Overview' in keys:
                    system_overview = access_list[access_list['section'].str.contains('System Overview')]
                    system_overview.drop(columns=['section'], inplace=True)
                    system_overview = yaml.safe_load(system_overview.to_json(orient=RECORDS))
                if 'Furnace: H3901A (Spall)' in keys:
                    furnace_a_spall = access_list.loc[access_list['section'] == 'Furnace: H3901A (Spall)']
                    furnace_a_spall.drop(columns=['section'], inplace=True)
                    furnace_a_spall = yaml.safe_load(furnace_a_spall.to_json(orient=RECORDS))
                if 'Furnace: H3901B (Spall)' in keys:
                    furnace_b_spall = access_list.loc[access_list['section'] == 'Furnace: H3901B (Spall)']
                    furnace_b_spall.drop(columns=['section'], inplace=True)
                    furnace_b_spall = yaml.safe_load(furnace_b_spall.to_json(orient=RECORDS))
            self._psql_session.execute(PERIOD_OF_TIME)
            time_period = pd.DataFrame(self._psql_session.fetchall())
            if not time_period.empty:
                df = time_period.groupby('setting')
                keys = []
                for name, group in df:
                    keys.append(name)
                if 'Default Notification View Timer Period' in keys:
                    default_n_view = int(time_period[time_period['setting'].str.contains('Default Notification View '
                                                                                         'Timer Period')]['value'].iloc[
                                             0])
                if 'Maximum Values Notification Download Time Period' in keys:
                    maximum_n_d_period = int(
                        time_period[time_period['setting'].str.contains('Maximum Values Notification '
                                                                        'Download Time Period')][
                            'value'].iloc[
                            0])
                if 'Password Expiry Period' in keys:
                    password_expiry = int(time_period[time_period['setting'].str.contains('Password Expiry Period')][
                                              'value'].iloc[0])

            old_dict = {"Benchmarking": benchmark,
                        "Furnace: H3901A": furncae_A,
                        "Furnace: H3901B": furncae_B,
                        "Furnace: H3901A (Spall)": furnace_a_spall,
                        "Furnace: H3901B (Spall)": furnace_b_spall,
                        "HGI": hgi,
                        "Left Pane": left_pane,
                        "Notifications": notifications,
                        "Outage": outage,
                        "PDI": pdi,
                        "Settings": settings,
                        "System Overview": system_overview,
                        "Default Notification View Timer Period": default_n_view,
                        "Maximum Values Notification Download Time Period": maximum_n_d_period,
                        "Password Expiry Period": password_expiry
                        }
            return JsonResponse(old_dict, safe=False)
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
def get_access_list_data(request):
    """
    This function will get the values for dynamic benchmarking
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:
            loggedin_user_details = _TokenValidation.validate_token(request)
            r_name = loggedin_user_details['role']
            names = ['Super Admin']
            if r_name in names:
                if loggedin_user_details:
                    obj = AccessList(loggedin_user_details)
                    return obj.get_access_list()
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
        excMsg = "get_access_list API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
