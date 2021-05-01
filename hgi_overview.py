"""
File                :   pdi_overview.py

Description         :   This will return all the module level data for the overhead pdi module

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   07-01-2020

Date Modified       :   07-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import datetime
from datetime import date

import jwt
import pandas as pd
import yaml
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import ERROR_DATA, HGI_MODULE_LEVEL_DATA, DRUM_RUNTIME_HGI_QUERY
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all
from utilities.Api_Response import *
from utilities.Constants import *
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class HgiOverview(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the overhead pdi module
    """

    def __init__(self, equipment=None, module=None):
        """
        This will call the parent class to validate the connection
        :param equipment: equipment name will be provided
        :param module: module name will be provided
        """
        super().__init__()
        self.module = module
        self.equipment = equipment

    def get_hgi_data(self):
        """
        This will return the module level data for the overhead hgi module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            module_level_data = []
            error_details = []
            latest_timestamp = None
            dict_data = {
                TIMESTAMP_KEY: latest_timestamp,
                "data": module_level_data,
                "errors": error_details,
                "predicted_tag": None
            }

            if self.equipment == COKE_DRUM_VALUE and self.module == HGI_VALUE:
                try:
                    # self._psql_session.execute(HGI_MODULE_LEVEL_DATA.format(self.equipment, self.module))
                    module = django_search_query_all(HGI_MODULE_LEVEL_DATA.format(self.equipment, self.module))
                    df = pd.DataFrame(module)
                    # self._psql_session.execute(DRUM_RUNTIME_HGI_QUERY)
                    drum_run = django_search_query_all(DRUM_RUNTIME_HGI_QUERY)
                    df_run_time = pd.DataFrame(drum_run)
                    if not df_run_time.empty:
                        run_time_value = df_run_time[TAG_VALUE].iloc[ZERO]
                        drum_online = df_run_time[DRUM_ONLINE].iloc[ZERO]
                        if run_time_value < ONE and drum_online == NORTH_DRUM_ONLINE:
                            dict_data["predicted_tag"] = 0
                        elif run_time_value > ONE and drum_online == NORTH_DRUM_ONLINE:
                            dict_data["predicted_tag"] = 1
                        elif run_time_value < ONE and drum_online == SOUTH_DRUM_ONLINE:
                            dict_data["predicted_tag"] = 0
                        elif run_time_value > ONE and drum_online == SOUTH_DRUM_ONLINE:
                            dict_data["predicted_tag"] = 2
                        else:
                            dict_data["predicted_tag"] = None

                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        dict_data[TIMESTAMP_KEY] = df[TIMESTAMP_KEY].iloc[0]
                        df_data = df.drop(columns=TIMESTAMP_KEY)
                        dict_data["data"] = dict_data["data"] + yaml.safe_load(df_data.to_json(orient=RECORDS))

                        # self._psql_session.execute(
                        #     ERROR_DATA.format(self.equipment, self.module, df[TIMESTAMP_KEY].iloc[0]))
                        error = django_search_query_all(
                            ERROR_DATA.format(self.equipment, self.module, df[TIMESTAMP_KEY].iloc[0]))
                        df_error = pd.DataFrame(error)
                        if not df_error.empty:
                            df_error = df_error.where(pd.notnull(df_error) == True, None)
                            df_error = df_error.drop_duplicates()
                            dict_data["errors"] = dict_data["errors"] + yaml.safe_load(df_error.to_json(orient=RECORDS))
                        else:
                            if DEBUG == 1:
                                print("sorry error data is not there")
                    else:
                        return JsonResponse(dict_data, safe=False)
                    return JsonResponse(dict_data, safe=False)

                except Exception as e:
                    log_error("error due to:%s" + str(e))
        except AssertionError as e:
            log_error("Assertion error due to : %s" + str(e))
            return asert_res(e)

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_hgi_overview(request, equipment_name=None, module_name=None):
    """
    This function will return the hgi module level overview
    :param module_name: module name
    :param equipment_name: equipment name
    :param request: request django object
    :return: json response
    """
    obj = None
    try:

        if request.method == GET_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = HgiOverview(equipment_name, module_name)
                return obj.get_hgi_data()

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
        excMsg = "get_hgi_overview API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
