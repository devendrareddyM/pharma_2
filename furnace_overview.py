"""
File                :   furnace_overview.py

Description         :   This will return all the module level data for the furnace module

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
    RECORDS, TIMESTAMP_KEY, FURNACE_VALUE, FURNACE_A_CHECK_STATUS_TAGS, \
    FURNACE_B_CHECK_STATUS_TAGS, PASS_A_VALUE, PASS_A_ONLINE_STATUS_TAG, PASS_A_SPALL_STATUS_TAG, \
    PASS_B_ONLINE_STATUS_TAG, PASS_B_SPALL_STATUS_TAG, ONE, ZERO, TAG_NAME_REQUEST, CREATE_TS, ALARM, DEBUG, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import ERROR_DATA, FURNACE_TMT_SPALL_CHECK, MODULE_LEVEL_SPALL_DATA, \
    MODULE_LEVEL_SPALL_TAGS_DATA, MODULE_LEVEL_TMT_DATA, MODULE_LEVEL_TMT_TAGS_DATA
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class FurnaceOverview(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the furnace module
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

    def get_furnace_data(self):
        """
        This will return the module level data for the furnace module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            module_level_data = []
            error_details = []
            furnace_details = []
            latest_timestamp = None
            dict_data = {
                TIMESTAMP_KEY: latest_timestamp,
                "data": module_level_data,
                "errors": error_details,
                "furnaces": furnace_details,
                "alarms": []
            }
            if self.equipment == FURNACE_VALUE:
                # if self.module == PASS_A_VALUE:
                #     tag_name = FURNACE_A_CHECK_STATUS_TAGS
                #     online_tag = PASS_A_ONLINE_STATUS_TAG
                #     spall_tag = PASS_A_SPALL_STATUS_TAG
                # else:
                #     tag_name = FURNACE_B_CHECK_STATUS_TAGS
                #     online_tag = PASS_B_ONLINE_STATUS_TAG
                #     spall_tag = PASS_B_SPALL_STATUS_TAG
                try:
                    # self._psql_session.execute(FURNACE_TMT_SPALL_CHECK.format(self.module, tuple(tag_name)))
                    # df_spall_check = pd.DataFrame(self._psql_session.fetchall())
                    # df_spall_check.set_index(TAG_NAME_REQUEST, inplace=True)
                    # df_spall_transposed = df_spall_check.T
                    # if df_spall_transposed[online_tag].iloc[0] == ZERO and df_spall_transposed[spall_tag].iloc[
                    #     0] == ONE:
                    #     """
                    #     This will return the module level data for the furnace module' spall effectiveness status
                    #     """
                    #     dict_data["is_spall"] = True
                    #     self._psql_session.execute(MODULE_LEVEL_SPALL_DATA.format(self.equipment, self.module))
                    #     df = pd.DataFrame(self._psql_session.fetchall())
                    #     self._psql_session.execute(MODULE_LEVEL_SPALL_TAGS_DATA.format(self.equipment, self.module))
                    #     df_tag = pd.DataFrame(self._psql_session.fetchall())
                    # else:
                    """
                    This will return the module level data for the furnace module
                    """
                    # dict_data["is_spall"] = False
                    self._psql_session.execute(MODULE_LEVEL_TMT_DATA.format(self.equipment, self.module))
                    df = pd.DataFrame(self._psql_session.fetchall())
                    self._psql_session.execute(MODULE_LEVEL_TMT_TAGS_DATA.format(self.equipment, self.module))
                    df_tag = pd.DataFrame(self._psql_session.fetchall())
                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        dict_data[TIMESTAMP_KEY] = df[CREATE_TS].iloc[0]
                        df_data = df.drop(columns=CREATE_TS)
                        dict_data["data"] = dict_data["data"] + yaml.safe_load(
                            df_data.to_json(orient=RECORDS))
                        alarm_tags = df_data[df_data[TAG_NAME_REQUEST].str.contains(ALARM)]
                        # if dict_data["is_spall"] == False:
                        reco_tags = df_data[df_data[TAG_NAME_REQUEST].str.contains('Reco')]
                        # reco_tags.loc[reco_tags['tag_value'].isnull(), 'condition'] = ' - '
                        reco_tags = (reco_tags[reco_tags.tag_value.notnull()])
                        if not reco_tags.empty:
                            alarm_tags = pd.concat([alarm_tags, reco_tags])
                        if len(alarm_tags) != 0:
                            # if dict_data["is_spall"] == True:
                            #     alarm_tags = alarm_tags[alarm_tags.condition != " - "]
                            if DEBUG == ZERO:
                                print("Sorry for spall Added new files")
                        else:
                            alarm_tags = df_data[df_data[TAG_NAME_REQUEST].str.contains(ALARM)]

                        dict_data["alarms"] = dict_data["alarms"] + yaml.safe_load(alarm_tags.to_json(orient=RECORDS))
                        self._psql_session.execute(
                            ERROR_DATA.format(self.equipment, self.module, df[CREATE_TS].iloc[0]))
                        df_error = pd.DataFrame(self._psql_session.fetchall())
                        if not df_error.empty:
                            df_error = df_error.where(pd.notnull(df_error) == True, None)
                            df_error = df_error.drop_duplicates()
                            dict_data["errors"] = dict_data["errors"] + yaml.safe_load(df_error.to_json(orient=RECORDS))
                        else:
                            if DEBUG == ONE:
                                print("Currently no error details!")
                        if not df_tag.empty:
                            df_tag = df_tag.where(pd.notnull(df_tag) == True, None)
                            df_tag = df_tag.drop(columns=CREATE_TS)
                            dict_data["furnaces"] = dict_data["furnaces"] + yaml.safe_load(
                                df_tag.to_json(orient=RECORDS))
                        else:
                            if DEBUG == ONE:
                                print("Currently no tag details for spall efficiency!")
                    else:
                        return dict_data
                    return dict_data
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
            else:
                return JsonResponse("This equipment is not registered with us!", safe=False)
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
def get_furnace_overview(request, equipment_name=None, module_name=None):
    """
    This function will return the furnace module level overview
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
            obj = FurnaceOverview(equipment_name, module_name)
            return obj.get_furnace_data()

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
