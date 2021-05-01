"""
File                :   pdi_overview.py

Description         :   This will return all the module level data for the overhead pdi module

Author              :   LivNSense Technologies Pvt Ltd

Date Created        :   07-01-2020

Date Modified       :   07-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    RECORDS, TIMESTAMP_KEY, PDI_VALUE, COKE_DRUM_VALUE, TAG_NAME_REQUEST, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import MODULE_LEVEL_DATA, ERROR_DATA
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class PdiOverview(_PostGreSqlConnection):
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

    def get_pdi_data(self):
        """
        This will return the module level data for the overhead pdi module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            module_level_data = []
            error_details = []
            drum_details = {}
            latest_timestamp = None
            dict_data = {
                TIMESTAMP_KEY: latest_timestamp,
                "data": module_level_data,
                "errors": error_details,
                "drums": drum_details
            }

            if self.equipment == COKE_DRUM_VALUE and self.module == PDI_VALUE:
                try:
                    self._psql_session.execute(MODULE_LEVEL_DATA.format(self.equipment, self.module))
                    df = pd.DataFrame(self._psql_session.fetchall())

                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        dict_data[TIMESTAMP_KEY] = df[TIMESTAMP_KEY].iloc[0]
                        df_data = df.drop(columns=TIMESTAMP_KEY)
                        dict_data["data"] = dict_data["data"] + yaml.safe_load(df_data.to_json(orient=RECORDS))

                        self._psql_session.execute(
                            ERROR_DATA.format(self.equipment, self.module, df[TIMESTAMP_KEY].iloc[0]))

                        df_error = pd.DataFrame(self._psql_session.fetchall())
                        if not df_error.empty:
                            df_error = df_error.where(pd.notnull(df_error) == True, None)
                            df_error = df_error.drop_duplicates()
                            dict_data["errors"] = dict_data["errors"] + yaml.safe_load(df_error.to_json(orient=RECORDS))
                        if self.module == PDI_VALUE:
                            df_temp = df.set_index(TAG_NAME_REQUEST)
                            north_status = df_temp.loc['BIL.39.D3908_Status.IDMS', 'tag_value']
                            south_status = df_temp.loc['BIL.39.D3909_Status.IDMS', 'tag_value']
                            print("no", north_status)
                            print("so", south_status)
                            print("er", df_error)
                            print("tm", df_temp)
                            print(df_temp)
                            if df_error.empty and df_temp.loc['39TC198', 'tag_value'] > 750:
                                """
                                Speedometer Status
                                """

                                dict_data["drums"]["BIL.39.D3908_Status.IDMS"] = {
                                    "status": int(north_status),
                                    "value": int(north_status)
                                }

                                dict_data["drums"]["BIL.39.D3909_Status.IDMS"] = {
                                    "status": int(south_status),
                                    "value": int(south_status)
                                }
                                try:
                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": int(north_status),
                                        "bar": float(df_temp.loc['BIL.39.D3908_Ovhd_FouledPDI.IDMS', 'tag_value']),
                                        "value": float(df_temp.loc['BIL.39.D3908_Ovhd_FouledPDI.IDMS', 'tag_value'])
                                    }
                                except Exception as e:
                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": int(north_status),
                                        "bar": 0,
                                        "value": None
                                    }
                                try:
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": int(south_status),
                                        "bar": float(df_temp.loc['BIL.39.D3909_Ovhd_FouledPDI.IDMS', 'tag_value']),
                                        "value": float(df_temp.loc['BIL.39.D3909_Ovhd_FouledPDI.IDMS', 'tag_value'])
                                    }
                                except Exception as E:
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": int(south_status),
                                        "bar": 0,
                                        "value": None
                                    }

                            elif not df_error.empty and df_temp.loc['39TC198', 'tag_value'] > 750:
                                """
                                Speedometer status based on error codes
                                """
                                dict_data["drums"]["BIL.39.D3908_Status.IDMS"] = {
                                    "status": north_status,
                                    "value": north_status
                                }

                                dict_data["drums"]["BIL.39.D3909_Status.IDMS"] = {
                                    "status": int(south_status),
                                    "value": int(south_status)
                                }
                                if df_error["error_code"].iloc[0] in [91, 97]:

                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": 1,
                                        "bar": 0,
                                        "value": None

                                    }
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": 1,
                                        "bar": 0,
                                        "value": None
                                    }
                                elif df_error["error_code"].iloc[0] in [92, 93, 94]:
                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": int(north_status),
                                        "bar": 0,
                                        "value": None

                                    }
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": int(south_status),
                                        "bar": 0,
                                        "value": None
                                    }
                                elif df_error["error_code"].iloc[0] == 96:
                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": int(south_status),
                                        "bar": 0,
                                        "value": None

                                    }
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": int(north_status),
                                        "bar": 0,
                                        "value": None
                                    }
                                else:
                                    tags = list(df_data['tag_name'])
                                    if 'BIL.39.D3908_Ovhd_CleanPDI.IDMS' in tags:
                                        value = df_data[
                                            df_data[TAG_NAME_REQUEST].str.contains('BIL.39.D3908_Ovhd_CleanPDI.IDMS')][
                                            'tag_value'].iloc[0]
                                    else:
                                        value = None

                                    # dict_data["drums"]["North_speedometer"] = {
                                    #     "status": 1,
                                    #     "bar": 0,
                                    #     "value": dict_data['data']['BIL.39.D3908_Ovhd_CleanPDI.IDMS']
                                    #
                                    # }
                                    dict_data["drums"]["North_speedometer"] = {
                                        "status": 1,
                                        "bar": 0,
                                        "value": value

                                    }
                                    dict_data["drums"]["South_speedometer"] = {
                                        "status": 1,
                                        "bar": 0,
                                        "value": None
                                    }
                            else:

                                dict_data["drums"]["BIL.39.D3908_Status.IDMS"] = {
                                    "status": int(north_status),
                                    "value": int(north_status)
                                }

                                dict_data["drums"]["BIL.39.D3909_Status.IDMS"] = {
                                    "status": int(south_status),
                                    "value": int(south_status)
                                }
                                dict_data["drums"]["North_speedometer"] = {
                                    "status": 0,
                                    "bar": 0,
                                    "value": None

                                }
                                dict_data["drums"]["South_speedometer"] = {
                                    "status": 0,
                                    "bar": 0,
                                    "value": None
                                }
                    else:
                        return dict_data
                    return dict_data

                except Exception as e:
                    log_error(e)
        except AssertionError as e:
            log_error("Assertion error due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
                                status=e.args[0][STATUS_KEY])

        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
                traceback.format_exc())},
                status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_pdi_overview(request, equipment_name=None, module_name=None):
    """
    This function will return the pdi module level overview
    :param module_name: module name
    :param equipment_name: equipment name
    :param request: request django object
    :return: json response
    """
    obj = None
    try:

        if request.method == GET_REQUEST:
            # loggedin_user_details = _TokenValidation.validate_token(request)
            # if loggedin_user_details:
            obj = PdiOverview(equipment_name, module_name)
            return obj.get_pdi_data()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    # except jwt.ExpiredSignatureError:
    #     token = request.META[HTTP_AUTHORIZATION_TOKEN].split(" ")[1]
    #     role = jwt.decode(token, verify=False)
    #     ob = HashingSalting()
    #     if role['role'] == 'Admin':
    #         ob.decreasing_admin_login_count()
    #     if role['role'] == 'Non Admin':
    #         ob.decreasing_Non_Admin_login_count()
    #     if role['role'] == 'Super Admin':
    #         ob.decreasing_super_Admin_login_count()
    #     return {MESSAGE_KEY: "Token Expired"}

    except Exception as e:
        return error_instance(e)

    finally:
        if obj:
            del obj
