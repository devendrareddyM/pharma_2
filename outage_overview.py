"""
File                :   outage_overview.py

Description         :   This will return all the module level data for the outage module

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
    RECORDS, TIMESTAMP_KEY, OUTAGE_VALUE, COKE_DRUM_VALUE, OUTAGE_NORTH_TAGS, \
    OUTAGE_SOUTH_TAGS, TAG_VALUE, ZERO, DRUM_ONLINE, ONE, SOUTH_DRUM_ONLINE, NORTH_DRUM_ONLINE, \
    HTTP_AUTHORIZATION_TOKEN, TAG_NAME_REQUEST, OUTAGE_MODULE_LEVEL_ACTUAL_TAG, OUTAGE_MODULE_LEVEL_PREDICTED_TAG, \
    LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug, logger
from ApplicationInterface.Database.Queries import ERROR_DATA, MODULE_LEVEL_OUTAGE_DATA, DRUM_RUNTIME_QUERY, \
    MODULE_LEVEL_OUTAGE_ONLINE_DATA, OUTAGE_MODULE_LEVEL_PRED_ACTUAL_DATA
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all


class OutageOverview(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the outage module
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

    def get_outage_data(self):
        """
        This will return the module level data for the outage module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            module_level_data = []
            error_details = []
            latest_timestamp = None
            drum_online = None
            dict_data = {
                TIMESTAMP_KEY: latest_timestamp,
                "data": module_level_data,
                "alarms": [],
                "errors": error_details
            }

            if self.equipment == COKE_DRUM_VALUE and self.module == OUTAGE_VALUE:
                try:
                    outage_north_tags = tuple(OUTAGE_NORTH_TAGS)
                    outage_south_tags = tuple(OUTAGE_SOUTH_TAGS)
                    actual_tags = tuple(LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH)
                    # self._psql_session.execute(DRUM_RUNTIME_QUERY)
                    df_run = django_search_query_all(DRUM_RUNTIME_QUERY)

                    df_run_time = pd.DataFrame(df_run)
                    if not df_run_time.empty:
                        run_time_value = df_run_time[TAG_VALUE].iloc[ZERO]
                        drum_online = df_run_time[DRUM_ONLINE].iloc[ZERO]
                        if run_time_value > ONE and drum_online == SOUTH_DRUM_ONLINE:
                            # self._psql_session.execute(MODULE_LEVEL_OUTAGE_ONLINE_DATA.format(outage_north_tags,
                            #                                                                   self.equipment,
                            #                                                                   self.module))
                            module_data = django_search_query_all(
                                MODULE_LEVEL_OUTAGE_ONLINE_DATA.format(outage_north_tags,
                                                                       self.equipment,
                                                                       self.module))
                        elif run_time_value > ONE and drum_online == NORTH_DRUM_ONLINE:
                            # self._psql_session.execute(MODULE_LEVEL_OUTAGE_ONLINE_DATA.format(outage_south_tags,
                            #                                                                   self.equipment,
                            #                                                                   self.module))
                            module_data = django_search_query_all(
                                MODULE_LEVEL_OUTAGE_ONLINE_DATA.format(outage_south_tags,
                                                                       self.equipment,
                                                                       self.module))
                        else:
                            # self._psql_session.execute(MODULE_LEVEL_OUTAGE_DATA.format(self.equipment, self.module))
                            module_data = django_search_query_all(
                                MODULE_LEVEL_OUTAGE_DATA.format(self.equipment, self.module))

                    else:
                        # self._psql_session.execute(MODULE_LEVEL_OUTAGE_DATA.format(self.equipment, self.module))
                        module_data = django_search_query_all(
                            MODULE_LEVEL_OUTAGE_DATA.format(self.equipment, self.module))

                    df = pd.DataFrame(module_data)
                    if drum_online == NORTH_DRUM_ONLINE:
                        # self._psql_session.execute(OUTAGE_MODULE_LEVEL_PRED_ACTUAL_DATA.format(
                        #     self.equipment,
                        #     self.module,
                        #     actual_tags, SOUTH_DRUM_ONLINE))
                        act_pred = django_search_query_all(OUTAGE_MODULE_LEVEL_PRED_ACTUAL_DATA.format(
                            self.equipment,
                            self.module,
                            actual_tags, SOUTH_DRUM_ONLINE))
                        df_act_pred = pd.DataFrame(act_pred)
                    else:
                        # self._psql_session.execute(OUTAGE_MODULE_LEVEL_PRED_ACTUAL_DATA.format(
                        #     self.equipment,
                        #     self.module,
                        #     actual_tags, NORTH_DRUM_ONLINE))
                        act_pred = django_search_query_all(OUTAGE_MODULE_LEVEL_PRED_ACTUAL_DATA.format(
                            self.equipment,
                            self.module,
                            actual_tags, NORTH_DRUM_ONLINE))
                        df_act_pred = pd.DataFrame(act_pred)
                    if not df.empty:
                        df = df.where(pd.notnull(df) == True, None)
                        df = df[df.tag_name != 'BIL.39.CokeDrum_Outage_Pred.IDMS']
                        df = df[df.tag_name != 'BIL.39.CokeDrum_Actual_Outage.IDMS']
                        dict_data[TIMESTAMP_KEY] = df[TIMESTAMP_KEY].iloc[0]
                        df_data = df.drop(columns=TIMESTAMP_KEY)
                        dict_data["data"] = dict_data["data"] + yaml.safe_load(df_data.to_json(orient=RECORDS))
                        if not df_act_pred.empty:
                            df_act_pred = df_act_pred.where(pd.notnull(df_act_pred) == True, None)
                            dict_data["data"] = dict_data["data"] + yaml.safe_load(df_act_pred.to_json(orient=RECORDS))
                        alarm_tags = df_data[df_data[TAG_NAME_REQUEST].str.contains('Outage_OutFlag1')]
                        alarm_tags1 = df_data[df_data[TAG_NAME_REQUEST].str.contains('Outage_OutFlag2')]
                        result_alarm = pd.concat([alarm_tags, alarm_tags1])
                        if not result_alarm.empty:
                            dict_data["alarms"] = dict_data["alarms"] + yaml.safe_load(
                                result_alarm.to_json(orient=RECORDS))
                        else:
                            dict_data["alarms"] = []

                        # self._psql_session.execute(
                        #     ERROR_DATA.format(self.equipment, self.module, df[TIMESTAMP_KEY].iloc[0]))
                        error = django_search_query_all(
                            ERROR_DATA.format(self.equipment, self.module, df[TIMESTAMP_KEY].iloc[0]))
                        df_error = pd.DataFrame(error)
                        if not df_error.empty:
                            df_error = df_error.where(pd.notnull(df_error) == True, None)
                            df_error = df_error.drop_duplicates()
                            dict_data["errors"] = dict_data["errors"] + yaml.safe_load(
                                df_error.to_json(orient=RECORDS))
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
def get_outage_overview(request, equipment_name=None, module_name=None):
    """
    This function will return the outage module level overview
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
            obj = OutageOverview(equipment_name, module_name)
            return obj.get_outage_data()

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
        excMsg = "outage_overview API : " + str(error_instance(e))
        return excMsg

    finally:
        if obj:
            del obj
