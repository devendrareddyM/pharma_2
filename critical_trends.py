"""
File                :   critical_trends.py

Description         :   This will return the critical trends value for the selected module

Author              :   LivNSense Technologies

Date Created        :   07-01-2020

Date Last modified :    09-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import numpy as np
import pandas as pd
import yaml
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Configuration import SECONDARY_TAG_LIST_NORTH, DAYS, SECONDARY_TAG_LIST_SOUTH, \
    COKE_FOAM_CRITICAL_TREND_TAGS, OUTAGE_CRITICAL_TREND_TAGS, QUENCH_NORTH_TAGS, QUENCH_SOUTH_TAGS, \
    QUENCH_NORTH_THUMBNAIL_TAGS, QUENCH_SOUTH_THUMBNAIL_TAGS, QUENCH_TREND_THUMBNAIL_TAGS, QUENCH_TREND_POP_UP_TAGS, \
    NORTH_PRED_LAB_HGI_TAGS, SOUTH_PRED_LAB_HGI_TAGS, PRED_LAB_TAGS
# from Application.Database.Configuration import SOUTH_PRED_LAB_HGI_TAGS, NORTH_PRED_LAB_HGI_TAGS, PRED_LAB_TAGS
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, START_DATE_REQUEST, ONLINE_DRUM_REQUEST, MOVING_AVG_REQUEST, TAG_NAME_REQUEST, \
    NORTH, FURNACE_VALUE, LAST_10_TMT_UNIT, OUTAGE_VALUE, FEATURE_REQUEST, COKE_FOAM_DATA, \
    COKE_HEIGHT_TAG, FOAM_HEIGHT_TAG, PDI_VALUE, OUTAGE_DATA, QUENCH_WATER_DATA, OUTAGE_TREND_TAG, PASS, IS_LIMITING, \
    CURRENT_RUN_TAG, LAST_10_RUN_TAG, FALSE_VALUE, TRUE_VALUE, LAST_10_PASS_A, PASS_A_VALUE, PASS_B_VALUE, \
    LAST_10_PASS_B, QUENCH_NORTH_SOUTH_TAGS, UNIT, TAG_VALUE, TIMESTAMP_KEY, JSON_BUILD_OBJECT, RESULT_KEY, RUN_CYCLE, \
    DOL_KEY, MIN_VALUE, MAX_VALUE, RECORDS, X_AXIS, DRUM_ONLINE, TS_KEY, TAG_AVERAGE_KEY, HGI_VALUE, CRUDE_MIX, \
    DRUM_REQUEST, PREDICTED_LAB_HGI, SOUTH_DRUM_ONLINE, NORTH_DRUM_ONLINE, DEBUG, PRED, ACTUAL, CREATE_TS, \
    PREDICTED_HGI, TARGET_PRED_HGI, TARGET_UPPER, TARGET_PRED_UPPER, TARGET_LOWER, TARGET_PRED_LOWER, HGI_DISTRIBUTION, \
    LAB_HGI_NORTH_ACTUAL, LAB_HGI_SOUTH_ACTUAL, LAB_HGI_NORTH_PRED, LAB_HGI_SOUTH_PRED, ZERO, ONE, DESCRIPTION, \
    HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import DETAILED_PDI_TREND_ALL, DETAILED_PDI_DOL_TREND, \
    DETAILED_PDI_TREND_ALL_NULL_START_DATE, \
    DETAILED_PDI_DOL_TREND_NULL_START_DATE, DETAILED_PDI_DOL_TREND_NULL_START_DATE_INBOUND, \
    DETAILED_PDI_DOL_TREND_INBOUND, LAST_10_TMT_TABLE_DATA, \
    LAST_10_TMT_TAGS_DATA, LAST_10_LIMITING_TI, GRAPH_OF_LAST_10, GRAPH_OF_CURRENT_10, \
    DETAILED_COKE_FOAM_TREND_THUMBNAIL, DETAILED_QUENCH_TREND_THUMBNAIL, DETAILED_QUENCH_TREND, \
    MIN_MAX_PDI_CRITICAL, MIN_MAX_DATA, OUTAGE_MIN_MAX_DATA, OFFLINE_DRUM_QUERY, HGI_CRUDE_MIX, HGI_ONLINE_DRUM_QUERY, \
    PREDICTED_LAB_HGI_THUMBNAIL, PREDICTED_HGI_THUMBNAIL, HGI_DISTRIBUTION_THUMBNAIL, \
    HGI_DISTRIBUTION_DETAILED_GRAPH, DRUM_RUNTIME_HGI_QUERY, ERROR_MESSAGE, EOR_DATE
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all


class CriticalTrends(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the critical trends for the overhead pdi module
    """

    def __init__(self, query_params=None, equipment=None, module=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param module: module name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()

        self.equipment = equipment
        self.module = module
        self.query_params = query_params

    def get_pdi_critical_trends_data(self):
        """
                This will return the critical trends value for the PDI
                :return: Json Response
                """
        average_fouling = []
        moving_avg_time = []
        clean_pdi = []
        actual_pdi = []
        time_data = []
        X_axis = []
        Y_axis = []
        unit_value = None
        sending_response = {}

        if self.equipment == COKE_DRUM_VALUE and self.module == PDI_VALUE:
            """
            This will return the critical trends data for the selected overhead pdi module
            """
            query_params = {
                ONLINE_DRUM_REQUEST: self.query_params.GET[ONLINE_DRUM_REQUEST],
                START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
            }
            if MOVING_AVG_REQUEST in self.query_params.GET or TAG_NAME_REQUEST in self.query_params.GET:
                query_params[MOVING_AVG_REQUEST] = self.query_params.GET[MOVING_AVG_REQUEST]
                query_params[TAG_NAME_REQUEST] = self.query_params.GET[TAG_NAME_REQUEST]
            if self.query_params:
                tag_name_tuple_north = tuple(SECONDARY_TAG_LIST_NORTH)
                tag_name_tuple_south = tuple(SECONDARY_TAG_LIST_SOUTH)

                d0 = np.datetime64(query_params[START_DATE_REQUEST]).astype('int64')
                d1 = np.datetime64(query_params[END_DATE_REQUEST]).astype('int64')
                """
                Calculating number of days between start date and end date
                delta = (d1 - d0) / (24 * 3600000)
                """
                delta = (d1 - d0) / (24 * 3600000)
                if query_params[ONLINE_DRUM_REQUEST] == NORTH:
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] \
                            and MOVING_AVG_REQUEST not in query_params:
                        self._psql_session.execute(
                            DETAILED_PDI_TREND_ALL_NULL_START_DATE.format(
                                self.module,
                                tag_name_tuple_north,
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and MOVING_AVG_REQUEST not in query_params:
                        self._psql_session.execute(
                            DETAILED_PDI_TREND_ALL.format(
                                self.module,
                                tag_name_tuple_north,
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]))

                else:
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                            MOVING_AVG_REQUEST not in query_params:
                        self._psql_session.execute(
                            DETAILED_PDI_TREND_ALL_NULL_START_DATE.format(
                                self.module,
                                tag_name_tuple_south,
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and MOVING_AVG_REQUEST not in query_params:
                        self._psql_session.execute(
                            DETAILED_PDI_TREND_ALL.format(
                                self.module,
                                tag_name_tuple_south,
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]))

                df_data = pd.DataFrame(self._psql_session.fetchall())

                if not df_data.empty:
                    df_tag_data = df_data.groupby(TAG_NAME_REQUEST)
                    for name, group in df_tag_data:
                        sending_response[name] = {
                            "unit": group[UNIT].iloc[0],
                            "y-axis": list(group[TAG_VALUE]),
                            "x-axis": list(group[TIMESTAMP_KEY])
                        }
                    # df_data = self._psql_session.fetchone()
                    # if df_data:
                    #     pdi_dict = df_data['result']
                    #     # return JsonResponse(pdi_dict, safe=False)
                    #     if df_data['result']:
                    #         for i in pdi_dict:
                    #             sending_response.update(i)
                    ############################################################################
                    # IF TAG NAME IS MISSING
                    if query_params[ONLINE_DRUM_REQUEST] == NORTH:
                        tag_left = list(SECONDARY_TAG_LIST_NORTH - sending_response.keys())
                    else:
                        tag_left = list(SECONDARY_TAG_LIST_SOUTH - sending_response.keys())
                    for each in tag_left:
                        sending_response[each] = {
                            "unit": None,
                            "y-axis": [],
                            "x-axis": []
                        }
                    ################################################################################
                else:
                    if query_params[ONLINE_DRUM_REQUEST] == NORTH:
                        tag_list = SECONDARY_TAG_LIST_NORTH
                    else:
                        tag_list = SECONDARY_TAG_LIST_SOUTH
                    for tag in tag_list:
                        sending_response[tag] = {
                            "unit": unit_value,
                            "y-axis": Y_axis,
                            "x-axis": X_axis
                        }

                if delta >= DAYS:
                    """
                    This will return the critical trends data for the selected overhead pdi module when the 
                    filtration is greater than or equal to 7 days
                    """
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_PDI_DOL_TREND_NULL_START_DATE_INBOUND.format(
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST]
                            ))
                    else:
                        self._psql_session.execute(
                            DETAILED_PDI_DOL_TREND_INBOUND.format(
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]
                            ))
                else:
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_PDI_DOL_TREND_NULL_START_DATE.format(
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[END_DATE_REQUEST]
                            ))
                    else:
                        self._psql_session.execute(
                            DETAILED_PDI_DOL_TREND.format(
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST],
                                query_params[ONLINE_DRUM_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]
                            ))

                df_dol_data = self._psql_session.fetchone()
                dol_dict = df_dol_data[JSON_BUILD_OBJECT]
                sending_response.update(dol_dict)
                """
                Getting Min and Max Values
                """
                if query_params[ONLINE_DRUM_REQUEST] == NORTH:
                    self._psql_session.execute(
                        MIN_MAX_PDI_CRITICAL.format(self.module,
                                                    tag_name_tuple_north))
                else:
                    self._psql_session.execute(
                        MIN_MAX_PDI_CRITICAL.format(self.module,
                                                    tag_name_tuple_south))
                df_min_max_data = self._psql_session.fetchone()
                sending_response_new = {}
                if df_min_max_data:
                    min_max_dict = df_min_max_data[RESULT_KEY]
                    if df_min_max_data[RESULT_KEY]:
                        for result in min_max_dict:
                            sending_response_new.update(result)
                    for value in sending_response:
                        sending_response[value].update(sending_response_new.get(value, {}))
                else:
                    sending_response["min_data"] = None,
                    sending_response["max_data"] = None

            return sending_response

    def get_furnace_critical_trends_data(self):
        """
        This will return the critical trends value for the furnace
        :return: dictionary data
        """
        if self.equipment == FURNACE_VALUE:
            """
            This will return the critical trends data for the selected furnace module
            """
            query_params = {
                PASS: self.query_params.GET[PASS],
                IS_LIMITING: self.query_params.GET[IS_LIMITING]
            }
            if CURRENT_RUN_TAG in self.query_params.GET or LAST_10_RUN_TAG in self.query_params.GET:
                query_params[CURRENT_RUN_TAG] = self.query_params.GET[CURRENT_RUN_TAG]
                query_params[LAST_10_RUN_TAG] = self.query_params.GET[LAST_10_RUN_TAG]
            if self.query_params:
                if self.module == PASS_A_VALUE:
                    self._psql_session.execute(
                        MIN_MAX_DATA.format(
                            self.module,
                            LAST_10_PASS_A
                        ))
                elif self.module == PASS_B_VALUE:
                    self._psql_session.execute(
                        MIN_MAX_DATA.format(
                            self.module,
                            LAST_10_PASS_B
                        ))
                df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
                if CURRENT_RUN_TAG in query_params and LAST_10_RUN_TAG in query_params and \
                        query_params[IS_LIMITING] == FALSE_VALUE:
                    """
                    This will return the critical trends data for the custom select option
                    """
                    self._psql_session.execute(
                        GRAPH_OF_CURRENT_10.format(
                            query_params[CURRENT_RUN_TAG],
                            query_params[PASS]
                        ))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    df_data = df_data.dropna()

                    self._psql_session.execute(
                        GRAPH_OF_LAST_10.format(
                            query_params[LAST_10_RUN_TAG],
                            query_params[PASS]
                        ))

                    df_last_data = pd.DataFrame(self._psql_session.fetchall())
                    df_last_data = df_last_data.dropna()

                    self._psql_session.execute(
                        LAST_10_TMT_TABLE_DATA.format(
                            query_params[PASS]
                        ))
                    df_table_data = pd.DataFrame(self._psql_session.fetchall())
                    df_table_data = df_table_data.dropna()
                    self._psql_session.execute(
                        EOR_DATE.format(query_params[PASS]))
                    df_time = pd.DataFrame(self._psql_session.fetchall())
                    # # df_t = df_t.set_index('parameters') df_t['current'] = pd.to_datetime(df_t['current'],
                    # errors='coerce') # df_t['current'] = df_t['current'].tz_localize('utc') pd.to_datetime(df_t[
                    # 'current'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern') df_t = df_t.set_index(
                    # 'parameters').T  # set index to set thecolumns as parameters and EOR date df_t['EOR Date'] =
                    # pd.to_datetime(df_t['EOR Date'], format='%Y-%m-%d %H:%M')  # code didnt read the values as
                    # datetime so convertto timestamps df_t['EOR Date'] = df_t['EOR Date'].dt.strftime(
                    # '%Y-%m-%dT%H:%M:%SZ')
                    if not df_time.empty:
                        df_time['current'] = pd.to_datetime(df_time['current'],
                                                            format='%Y-%m-%d %H:%M')
                        df_time['current'] = df_time['current'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['last'] = pd.to_datetime(df_time['last'],
                                                         format='%Y-%m-%d %H:%M')
                        df_time['last'] = df_time['last'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['secondLast'] = pd.to_datetime(df_time['secondLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['secondLast'] = df_time['secondLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['thirdLast'] = pd.to_datetime(df_time['thirdLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['thirdLast'] = df_time['thirdLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                        df_time['fourthLast'] = pd.to_datetime(df_time['fourthLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['fourthLast'] = df_time['fourthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                        df_time['fifthLast'] = pd.to_datetime(df_time['fifthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['fifthLast'] = df_time['fifthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['sixthLast'] = pd.to_datetime(df_time['sixthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['sixthLast'] = df_time['sixthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['seventhLast'] = pd.to_datetime(df_time['seventhLast'],
                                                                format='%Y-%m-%d %H:%M')
                        df_time['seventhLast'] = df_time['seventhLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['eighthLast'] = pd.to_datetime(df_time['eighthLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['eighthLast'] = df_time['eighthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['ninthLast'] = pd.to_datetime(df_time['ninthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['ninthLast'] = df_time['ninthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['tenthLast'] = pd.to_datetime(df_time['tenthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['tenthLast'] = df_time['tenthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                    graph = []
                    table = []
                    dict_data = {
                        "graph_data": graph,
                        "table_data": table
                    }

                    if not df_data.empty or not df_last_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)

                        def Merge(dict1, dict2):
                            res = {**dict1, **dict2}
                            return res

                        if not df_data.empty:
                            temp_days = {
                                "Current": list(df_data[TAG_VALUE])
                            }
                        else:
                            temp_days = {
                                "Current": []
                            }

                        if not df_last_data.empty:
                            df_last_data = df_last_data.where(pd.notnull(df_last_data) == True, None)
                            last_run = df_last_data.groupby(RUN_CYCLE)
                            df_run = df_last_data[DOL_KEY].unique()
                            df_run_cycle = np.sort(df_run)

                            old_dict = {}

                            for name, group in last_run:
                                old_dict[name] = list(group[TAG_VALUE])

                            reversed_old_dict = {}

                            for key in sorted(old_dict.keys(), reverse=True):
                                reversed_old_dict[key] = old_dict[key]
                            new_dict = {
                                "Last": [],
                                "2nd Last": [],
                                "3rd Last": [],
                                "4th Last": [],
                                "5th Last": [],
                                "6th Last": [],
                                "7th Last": [],
                                "8th Last": [],
                                "9th Last": [],
                                "10th Last": []
                            }
                            final_dict = {k: j for k, j in zip(new_dict.keys(), reversed_old_dict.values())}
                            final_dict["unit"] = LAST_10_TMT_UNIT
                            final_dict["x-axis"] = df_run_cycle.tolist()
                            temp_final = Merge(temp_days, final_dict)
                            if not df_min_max_data.empty:
                                temp_final["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
                                temp_final["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
                            else:
                                temp_final["min_data"] = None
                                temp_final["max_data"] = None
                            graph.append(temp_final)
                        else:
                            new_dict = {
                                "Last": [],
                                "2nd Last": [],
                                "3rd Last": [],
                                "4th Last": [],
                                "5th Last": [],
                                "6th Last": [],
                                "7th Last": [],
                                "8th Last": [],
                                "9th Last": [],
                                "10th Last": [],
                                "Unit": LAST_10_TMT_UNIT,
                                "x-axis": []
                            }
                            temp_final = Merge(temp_days, new_dict)
                            if not df_min_max_data.empty:
                                temp_final["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
                                temp_final["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
                            else:
                                temp_final["min_data"] = None
                                temp_final["max_data"] = None
                            graph.append(temp_final)

                    if not df_table_data.empty:
                        if not df_time.empty:
                            df_table_data = df_time.append(df_table_data)
                        df_table_data = df_table_data.where(pd.notnull(df_table_data) == True, None)
                        df_table_data = yaml.safe_load(df_table_data.to_json(orient=RECORDS))
                        table.append(df_table_data)

                    return dict_data
                elif CURRENT_RUN_TAG not in query_params and LAST_10_RUN_TAG not in query_params and \
                        query_params[IS_LIMITING] == TRUE_VALUE:
                    """
                    This will return the critical trends data for the limiting TI option
                    """
                    self._psql_session.execute(
                        LAST_10_LIMITING_TI.format(
                            query_params[PASS]
                        ))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    df_data = df_data.dropna()

                    self._psql_session.execute(
                        LAST_10_TMT_TABLE_DATA.format(
                            query_params[PASS]
                        ))
                    df_table_data = pd.DataFrame(self._psql_session.fetchall())
                    df_table_data = df_table_data.dropna()
                    self._psql_session.execute(
                        LAST_10_TMT_TAGS_DATA.format(
                            query_params[PASS]
                        ))
                    df_tags_data = pd.DataFrame(self._psql_session.fetchall())
                    df_tags_data = df_tags_data.dropna()
                    self._psql_session.execute(
                        EOR_DATE.format(query_params[PASS]))
                    df_time = pd.DataFrame(self._psql_session.fetchall())
                    # # df_t = df_t.set_index('parameters') df_t['current'] = pd.to_datetime(df_t['current'],
                    # errors='coerce') # df_t['current'] = df_t['current'].tz_localize('utc') pd.to_datetime(df_t[
                    # 'current'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
                    # df_t = df_t.set_index('parameters').T  # set index to set thecolumns as parameters and EOR date df_t['EOR Date'] =
                    # pd.to_datetime(df_t['EOR Date'], format='%Y-%m-%d %H:%M')  # code didnt read the values as
                    # datetime so convertto timestamps
                    # df_t['EOR Date'] = df_t['EOR Date'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                    if not df_time.empty:
                        df_time['current'] = pd.to_datetime(df_time['current'],
                                                            format='%Y-%m-%d %H:%M')
                        df_time['current'] = df_time['current'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['last'] = pd.to_datetime(df_time['last'],
                                                         format='%Y-%m-%d %H:%M')
                        df_time['last'] = df_time['last'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['secondLast'] = pd.to_datetime(df_time['secondLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['secondLast'] = df_time['secondLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['thirdLast'] = pd.to_datetime(df_time['thirdLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['thirdLast'] = df_time['thirdLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                        df_time['fourthLast'] = pd.to_datetime(df_time['fourthLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['fourthLast'] = df_time['fourthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                        df_time['fifthLast'] = pd.to_datetime(df_time['fifthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['fifthLast'] = df_time['fifthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['sixthLast'] = pd.to_datetime(df_time['sixthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['sixthLast'] = df_time['sixthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['seventhLast'] = pd.to_datetime(df_time['seventhLast'],
                                                                format='%Y-%m-%d %H:%M')
                        df_time['seventhLast'] = df_time['seventhLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['eighthLast'] = pd.to_datetime(df_time['eighthLast'],
                                                               format='%Y-%m-%d %H:%M')
                        df_time['eighthLast'] = df_time['eighthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['ninthLast'] = pd.to_datetime(df_time['ninthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['ninthLast'] = df_time['ninthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')
                        df_time['tenthLast'] = pd.to_datetime(df_time['tenthLast'],
                                                              format='%Y-%m-%d %H:%M')
                        df_time['tenthLast'] = df_time['tenthLast'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

                    graph = []
                    table = []
                    tags = []
                    dict_data = {
                        "graph_data": graph,
                        "table_data": table,
                        "tag_list": tags
                    }

                    if not df_tags_data.empty:
                        dict_data["tag_list"] = list(df_tags_data[TAG_NAME_REQUEST])
                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_run = df_data[DOL_KEY].unique()
                        df_run_cycle = np.sort(df_run)
                        df = df_data.groupby(RUN_CYCLE)
                        old_dict = {}

                        for name, group in df:
                            old_dict[name] = list(group[TAG_VALUE])

                        reversed_old_dict = {}

                        for key in sorted(old_dict.keys(), reverse=True):
                            reversed_old_dict[key] = old_dict[key]

                        new_dict = {
                            "Current": [],
                            "Last": [],
                            "2nd Last": [],
                            "3rd Last": [],
                            "4th Last": [],
                            "5th Last": [],
                            "6th Last": [],
                            "7th Last": [],
                            "8th Last": [],
                            "9th Last": [],
                            "10th Last": []
                        }

                        final_dict = {k: j for k, j in zip(new_dict.keys(), reversed_old_dict.values())}
                        final_dict["unit"] = LAST_10_TMT_UNIT
                        final_dict["x-axis"] = df_run_cycle.tolist()
                        if not df_min_max_data.empty:
                            final_dict["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
                            final_dict["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
                        else:
                            final_dict["min_data"] = None
                            final_dict["max_data"] = None
                        graph.append(final_dict)

                    if not df_table_data.empty:
                        if not df_time.empty:
                            df_table_data = df_time.append(df_table_data)
                        df_table_data = df_table_data.where(pd.notnull(df_table_data) == True, None)
                        df_table_data = yaml.safe_load(df_table_data.to_json(orient=RECORDS))
                        table.append(df_table_data)
                    return dict_data

    def get_outage_critical_trends_data(self):
        if self.equipment == COKE_DRUM_VALUE and self.module == OUTAGE_VALUE:
            """
            This will return the critical trends data for the selected outage module
            """
            query_params = {
                FEATURE_REQUEST: self.query_params.GET[FEATURE_REQUEST]
            }
            if START_DATE_REQUEST in self.query_params.GET or END_DATE_REQUEST in self.query_params.GET:
                query_params[START_DATE_REQUEST] = self.query_params.GET[START_DATE_REQUEST]
                query_params[END_DATE_REQUEST] = self.query_params.GET[END_DATE_REQUEST]
            if self.query_params:
                coke_foam_tag_name = tuple(COKE_FOAM_CRITICAL_TREND_TAGS)
                outage_tag_name = OUTAGE_CRITICAL_TREND_TAGS
                quench_north_tag_name = tuple(QUENCH_NORTH_TAGS)
                quench_south_tag_name = tuple(QUENCH_SOUTH_TAGS)
                quench_water_north_tag_name = tuple(QUENCH_NORTH_THUMBNAIL_TAGS)
                quench_water_south_tag_name = tuple(QUENCH_SOUTH_THUMBNAIL_TAGS)
                quench_north_south_tags = tuple(QUENCH_NORTH_SOUTH_TAGS)
                quench_trend_thumbnail_tags = tuple(QUENCH_TREND_THUMBNAIL_TAGS)
                quench_trend_pop_up_tags = tuple(QUENCH_TREND_POP_UP_TAGS)

                if query_params[FEATURE_REQUEST] == COKE_FOAM_DATA:
                    """
                     This will return the critical trends thumbnail and pop up graph data for the selected coke foam feature
                    """
                    graph = []
                    dict_data = {
                        "coke": [],
                        "foam": [],
                        "x-axis": [],
                        "unit": None
                    }
                    coke_foam_data = []
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params:
                        coke_foam_data = django_search_query_all(
                            DETAILED_COKE_FOAM_TREND_THUMBNAIL.format(
                                self.module,
                                coke_foam_tag_name))
                    else:
                        pass
                    df_data = pd.DataFrame(coke_foam_data)
                    if not df_data.empty:
                        # df_data = df_data.dropna()
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        data_now = df_data.groupby(TAG_NAME_REQUEST)
                        df_cycle = df_data[X_AXIS].unique()
                        # df_unit = df_data["unit"].unique()
                        df_unit_d = df_data[UNIT].iloc[0]
                        # df_unit_d = df_data["unit"].iloc[0] if df_data["unit"].iloc[0] is not None else None
                        # df_unit_d = df_unit_d[0]
                        old_dict = {}
                        for name, group in data_now:
                            old_dict[name] = list(group[TAG_VALUE])

                        keys = []
                        for key in old_dict.keys():
                            keys.append(key)
                        if COKE_HEIGHT_TAG in keys:
                            dict_data["coke"] = old_dict[COKE_HEIGHT_TAG]
                        else:
                            dict_data["coke"] = []
                        if FOAM_HEIGHT_TAG in keys:
                            dict_data["foam"] = old_dict[FOAM_HEIGHT_TAG]
                        else:
                            dict_data["foam"] = []

                        dict_data["x-axis"] = df_cycle.tolist()
                        dict_data["unit"] = df_unit_d
                    graph.append(dict_data)
                    return graph
                elif query_params[FEATURE_REQUEST] == OUTAGE_DATA:
                    """
                    This will return the critical trends thumbnail and pop up graph data for the selected outage feature
                    """
                    graph = []
                    dict_data = {
                        "outage": [],
                        "x-axis": [],
                        "unit": None
                    }
                    outage_foam_data = []
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params:
                        outage_foam_data = django_search_query_all(
                            DETAILED_COKE_FOAM_TREND_THUMBNAIL.format(
                                self.module,
                                "('" + str(tuple(outage_tag_name)[0] + "')")))
                    else:
                        pass
                    df_data = pd.DataFrame(outage_foam_data)
                    if not df_data.empty:
                        # df_data = df_data.dropna()
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        data_now = df_data.groupby(TAG_NAME_REQUEST)
                        df_cycle = df_data[X_AXIS].unique()
                        df_unit_d = df_data[UNIT].iloc[0]
                        # df_unit = df_data["unit"].unique()
                        # df_unit_d = df_unit
                        # df_unit_d = df_data["unit"].iloc[0] if df_data["unit"].iloc[0] is not None else None
                        # df_unit_d = df_unit_d[0]
                        old_dict = {}
                        for name, group in data_now:
                            old_dict[name] = list(group[TAG_VALUE])
                        keys = []
                        for key in old_dict.keys():
                            keys.append(key)
                        if OUTAGE_TREND_TAG in keys:
                            dict_data["outage"] = old_dict[OUTAGE_TREND_TAG]
                        else:
                            dict_data["outage"] = []

                        dict_data["x-axis"] = df_cycle.tolist()
                        dict_data["unit"] = df_unit_d
                    graph.append(dict_data)
                    return graph
                elif query_params[FEATURE_REQUEST] == QUENCH_WATER_DATA:
                    """                           
                    This will return the critical trends thumbnail and pop up graph data for the selected quench water feature 
                    """
                    graph = []
                    dict_data = {"x-axis": {"data": []}}
                    offline_drum = django_search_query_all(
                        OFFLINE_DRUM_QUERY)
                    df_online_drum = pd.DataFrame(offline_drum)
                    df_online_drum = df_online_drum.dropna()
                    if not df_online_drum.empty:
                        online_drum = df_online_drum[DRUM_ONLINE].iloc[0]
                    else:
                        online_drum = None
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params:
                        quench_thumb_data = django_search_query_all(
                            DETAILED_QUENCH_TREND_THUMBNAIL.format(
                                self.module,
                                quench_trend_thumbnail_tags))
                        df_data = pd.DataFrame(quench_thumb_data)
                        min_max = django_search_query_all(
                            OUTAGE_MIN_MAX_DATA.format(
                                self.module,
                                quench_trend_thumbnail_tags
                            ))
                        df_min_max_data = pd.DataFrame(min_max)
                        if not df_data.empty:
                            # df_data = df_data.dropna()
                            df_data = df_data.where(pd.notnull(df_data) == True, None)
                            data_now = df_data.groupby(TAG_NAME_REQUEST)
                            df_cycle = df_data[TS_KEY].apply(lambda x: str(x)).unique()
                            del dict_data["x-axis"]
                            if not df_min_max_data.empty:
                                for name, group in data_now:
                                    if name in list(df_min_max_data[TAG_NAME_REQUEST]):
                                        dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                           "unit":
                                                               df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                           "min_data": df_min_max_data[
                                                               df_min_max_data[TAG_NAME_REQUEST] == name][
                                                               MIN_VALUE].iloc[0],
                                                           "max_data": df_min_max_data[
                                                               df_min_max_data[TAG_NAME_REQUEST] == name][
                                                               MAX_VALUE].iloc[0]}
                                    else:
                                        dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                           "unit":
                                                               df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                           "min_data": None,
                                                           "max_data": None}

                            else:
                                for name, group in data_now:
                                    dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                       "unit": df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                       "min_data": None,
                                                       "max_data": None}

                            dict_data["x-axis"] = {"data": df_cycle.tolist()}
                            graph.append(dict_data)
                        return graph

                    else:
                        dict_data["disabled_tags"] = quench_trend_pop_up_tags
                        quench_thumb_data = django_search_query_all(
                            DETAILED_QUENCH_TREND.format(
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST],
                                quench_north_south_tags))
                        df_data = pd.DataFrame(quench_thumb_data)
                        min_max = django_search_query_all(
                            OUTAGE_MIN_MAX_DATA.format(
                                self.module,
                                quench_north_south_tags
                            ))
                        df_min_max_data = pd.DataFrame(min_max)

                        if not df_data.empty:
                            df_data = df_data.where(pd.notnull(df_data) == True, None)
                            df_data = df_data.dropna(thresh=2)
                            data_now = df_data.groupby(TAG_NAME_REQUEST)
                            # df_cycle = df_data["ts"].apply(lambda x: str(x)).unique()
                            df_cycle = list(dict.fromkeys(list(df_data['ts'])))
                            del dict_data["x-axis"]
                            for name, group in data_now:
                                if not df_min_max_data.empty:

                                    if name in list(df_min_max_data[TAG_NAME_REQUEST]):
                                        dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                           "unit":
                                                               df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                           "min_data":
                                                               df_min_max_data[
                                                                   df_min_max_data[TAG_NAME_REQUEST] == name][
                                                                   MIN_VALUE].iloc[0],
                                                           "max_data":
                                                               df_min_max_data[
                                                                   df_min_max_data[TAG_NAME_REQUEST] == name][
                                                                   MAX_VALUE].iloc[0]
                                                           }
                                    else:
                                        dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                           "unit":
                                                               df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                           "min_data": None,
                                                           "max_data": None}
                                else:
                                    for name, group in data_now:
                                        dict_data[name] = {"data": list(group[TAG_AVERAGE_KEY]),
                                                           "unit":
                                                               df_data[df_data[TAG_NAME_REQUEST] == name][UNIT].iloc[0],
                                                           "min_data": None,
                                                           "max_data": None}

                            # IF TAG NAME IS MISSING
                            tag_data = list(QUENCH_NORTH_SOUTH_TAGS - dict_data.keys())
                            for each in tag_data:
                                if not df_min_max_data.empty:
                                    if each in list(df_min_max_data[TAG_NAME_REQUEST]):
                                        dict_data[each] = {"data": [],
                                                           "unit": None,
                                                           "min_data": df_min_max_data[
                                                               df_min_max_data[TAG_NAME_REQUEST] == each][
                                                               MIN_VALUE].iloc[0],
                                                           "max_data": df_min_max_data[
                                                               df_min_max_data[TAG_NAME_REQUEST] == each][
                                                               MAX_VALUE].iloc[0]
                                                           }
                                    else:
                                        dict_data[each] = {"data": [],
                                                           "unit": None,
                                                           "min_data": None,
                                                           "max_data": None}
                                else:
                                    dict_data[each] = {"data": [],
                                                       "unit": None,
                                                       "min_data": None,
                                                       "max_data": None}

                            dict_data["x-axis"] = {"data": df_cycle}
                            graph.append(dict_data)

                        else:
                            tag_data = QUENCH_NORTH_SOUTH_TAGS
                            for each in tag_data:
                                if not df_min_max_data.empty:
                                    if each in list(df_min_max_data[TAG_NAME_REQUEST]):
                                        dict_data[each] = {"data": [],
                                                           "unit": None,
                                                           "min_data":
                                                               df_min_max_data[
                                                                   df_min_max_data[TAG_NAME_REQUEST] == each][
                                                                   MIN_VALUE].iloc[0],
                                                           "max_data":
                                                               df_min_max_data[
                                                                   df_min_max_data[TAG_NAME_REQUEST] == each][
                                                                   MAX_VALUE].iloc[0]
                                                           }
                                    else:
                                        dict_data[each] = {"data": [],
                                                           "unit": None,
                                                           "min_data": None,
                                                           "max_data": None
                                                           }
                                else:
                                    dict_data[each] = {"data": [],
                                                       "unit": None,
                                                       "min_data": None,
                                                       "max_data": None
                                                       }
                            dict_data["x-axis"] = {"data": []}
                            graph.append(dict_data)
                        return graph

    def get_hgi_critical_trends_data(self):
        if self.equipment == COKE_DRUM_VALUE and self.module == HGI_VALUE:
            """
            This will return the critical trends data for the selected Hgi module
            """
            query_params = {
                FEATURE_REQUEST: self.query_params.GET[FEATURE_REQUEST]
            }
            if START_DATE_REQUEST in self.query_params.GET or END_DATE_REQUEST in self.query_params.GET:
                query_params[START_DATE_REQUEST] = self.query_params.GET[START_DATE_REQUEST]
                query_params[END_DATE_REQUEST] = self.query_params.GET[END_DATE_REQUEST]
            if DRUM_REQUEST in self.query_params.GET:
                query_params[DRUM_REQUEST] = self.query_params.GET[DRUM_REQUEST]
            if self.query_params:
                if query_params[FEATURE_REQUEST] == CRUDE_MIX:
                    """ 
                    it will give the data for the crude mix thumbnail for the hgi module
                    """
                    crude = django_search_query_all(HGI_CRUDE_MIX)
                    df_crude = pd.DataFrame(crude)
                    result_df = [df_crude['json_build_object'][0], df_crude['json_build_object'][1],
                                 df_crude['json_build_object'][2]]
                    return result_df
                if query_params[FEATURE_REQUEST] == PREDICTED_LAB_HGI:

                    """ 
                    it will give the data for the Predicted lab hgi
                    """

                    SOUTH_PRED_LAB_TAGS = tuple(SOUTH_PRED_LAB_HGI_TAGS)
                    NORTH_PRED_LAB_TAGS = tuple(NORTH_PRED_LAB_HGI_TAGS)
                    result = []
                    actual_time = []
                    predicted_time = []
                    actual_tag = None
                    predicted_tag = None
                    dict_data = {}
                    pred_lab_data = None
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params and DRUM_REQUEST not in query_params:

                        """ 
                        it will give the data for Predicted lab hgi thumbnail for the 7 days for online drum
                        """
                        online_drum_data = django_search_query_all(HGI_ONLINE_DRUM_QUERY)
                        drum_online = pd.DataFrame(online_drum_data)

                        if not drum_online.empty:
                            online_drum = drum_online['drum_online'].iloc[0]
                            if drum_online[DRUM_ONLINE].iloc[0] == SOUTH_DRUM_ONLINE:
                                pred_lab_data = django_search_query_all(PREDICTED_LAB_HGI_THUMBNAIL.format(self.module,
                                                                                                           SOUTH_PRED_LAB_TAGS,
                                                                                                           online_drum))
                            if drum_online[DRUM_ONLINE].iloc[0] == NORTH_DRUM_ONLINE:
                                pred_lab_data = django_search_query_all(PREDICTED_LAB_HGI_THUMBNAIL.format(self.module,
                                                                                                           NORTH_PRED_LAB_TAGS,
                                                                                                           online_drum))
                        else:
                            if DEBUG == 1:
                                print("sorry")

                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params and DRUM_REQUEST in query_params:

                        """
                        it will give the for 7 days data  when the drum request is there in the query params 
                        """
                        if query_params[DRUM_REQUEST] == SOUTH_DRUM_ONLINE:
                            pred_lab_data = django_search_query_all(PREDICTED_LAB_HGI_THUMBNAIL.format(self.module,
                                                                                                       SOUTH_PRED_LAB_TAGS,
                                                                                                       SOUTH_DRUM_ONLINE))
                        if query_params[DRUM_REQUEST] == NORTH_DRUM_ONLINE:
                            pred_lab_data = django_search_query_all(PREDICTED_LAB_HGI_THUMBNAIL.format(self.module,
                                                                                                       NORTH_PRED_LAB_TAGS,
                                                                                                       NORTH_DRUM_ONLINE))
                    df_data = pd.DataFrame(pred_lab_data)
                    if not df_data.empty:
                        data_now = df_data.groupby(TAG_NAME_REQUEST)
                        old_dict = {}
                        for name, group in data_now:
                            old_dict[name] = list(group[TAG_VALUE])

                        keys = []
                        for key in old_dict.keys():
                            keys.append(key)
                        if LAB_HGI_NORTH_ACTUAL in keys or LAB_HGI_SOUTH_ACTUAL in keys:
                            actual_tag = df_data[df_data[TAG_NAME_REQUEST].str.contains(ACTUAL)]['tag_name'].iloc[0]
                            act_time = df_data[df_data[TAG_NAME_REQUEST].str.contains(ACTUAL)]['timestamp']
                            actual_time.append(act_time)
                        if LAB_HGI_SOUTH_PRED in keys or LAB_HGI_NORTH_PRED in keys:
                            predicted_tag = df_data[df_data[TAG_NAME_REQUEST].str.contains(PRED)]['tag_name'].iloc[0]
                            pred_time = df_data[df_data[TAG_NAME_REQUEST].str.contains(PRED)]['timestamp']
                            predicted_time.append(pred_time)
                        if len(predicted_time) != 0:
                            for k in predicted_time:
                                if len(predicted_time) != 1:
                                    if k not in actual_time:
                                        df_data = df_data.append(
                                            {'tag_name': actual_tag, 'tag_value': None, 'timestamp': k},
                                            ignore_index=True)
                        if len(actual_time) != 0:
                            for k in actual_time:
                                if len(actual_time) != 1:
                                    if k not in predicted_time:
                                        df_data = df_data.append(
                                            {'tag_name': predicted_tag, 'tag_value': None, 'timestamp': k},
                                            ignore_index=True)
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values('timestamp', ascending=True, inplace=True)
                        if LAB_HGI_NORTH_ACTUAL in keys or LAB_HGI_SOUTH_ACTUAL in keys:
                            actual_values = df_data[df_data[TAG_NAME_REQUEST].str.contains(ACTUAL)][TAG_VALUE].values
                            key = df_data[df_data[TAG_NAME_REQUEST].str.contains(ACTUAL)][TAG_NAME_REQUEST].iloc[0]
                            dict_data[key] = actual_values.tolist()

                        if LAB_HGI_SOUTH_PRED in keys or LAB_HGI_NORTH_PRED in keys:
                            predicted_value = df_data[df_data[TAG_NAME_REQUEST].str.contains(PRED)][TAG_VALUE].values
                            key = df_data[df_data[TAG_NAME_REQUEST].str.contains(PRED)][TAG_NAME_REQUEST].iloc[0]
                            dict_data[key] = predicted_value.tolist()

                        dict_data["x-axis"] = list(dict.fromkeys(list(df_data['timestamp'])))
                        result.append(dict_data)
                    return result

                if query_params[FEATURE_REQUEST] == PREDICTED_HGI:

                    """ 
                    it will give the data for the predicted hgi for online drum based on the hourly
                    """
                    pred_lab_tags = tuple(PRED_LAB_TAGS)
                    alert = None
                    error_response = None
                    df_target = None
                    df_upper = None
                    df_lower = None
                    graph = []
                    dict_data = {TARGET_PRED_HGI: [],
                                 TARGET_UPPER: [],
                                 TARGET_LOWER: [],
                                 TARGET_PRED_UPPER: [],
                                 TARGET_PRED_LOWER: [],
                                 "x-axis": [],
                                 "unit": None,
                                 "alert": alert
                                 }
                    online_drum_data = django_search_query_all(DRUM_RUNTIME_HGI_QUERY)
                    df_run_time = pd.DataFrame(online_drum_data)
                    if not df_run_time.empty:
                        run_time_value = df_run_time[TAG_VALUE].iloc[ZERO]
                        if run_time_value < ONE:
                            error_response_data = django_search_query_all(ERROR_MESSAGE)
                            error_message = pd.DataFrame(error_response_data)
                            if not error_message.empty:
                                error_response = error_message[DESCRIPTION].iloc[0]
                            return {MESSAGE_KEY: error_response}
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params:

                        """ 
                        if start date and end date not in query params want to show data 
                        """
                        pred_hgi_data = django_search_query_all(
                            PREDICTED_HGI_THUMBNAIL.format(self.module, pred_lab_tags))
                        df_pred = pd.DataFrame(pred_hgi_data)
                        if not df_pred.empty:
                            df_pred = df_pred.where(pd.notnull(df_pred) == True, None)
                            data_now = df_pred.groupby(TAG_NAME_REQUEST)
                            df_cycle = df_pred[X_AXIS].unique()
                            df_unit_d = df_pred[UNIT].iloc[0]
                            old_dict = {}
                            for name, group in data_now:
                                old_dict[name] = list(group[TAG_VALUE])

                            keys = []
                            for key in old_dict.keys():
                                keys.append(key)

                            if TARGET_PRED_HGI in keys:
                                df_target = \
                                    df_pred.loc[(df_pred[TAG_NAME_REQUEST] == TARGET_PRED_HGI)][
                                        'tag_value'].iloc[-1]
                                values = old_dict[TARGET_PRED_HGI]
                                predicted_data = []
                                for i in range(len(values) - 1):
                                    predicted_data.append(None)
                                predicted_data.append(df_target)

                                dict_data[TARGET_PRED_HGI] = predicted_data

                            else:
                                dict_data[TARGET_PRED_HGI] = []
                            if TARGET_UPPER in keys:
                                df_upper = df_pred.loc[(df_pred[TAG_NAME_REQUEST] == TARGET_UPPER)][
                                    'tag_value'].iloc[-1]
                                dict_data[TARGET_UPPER] = old_dict[TARGET_UPPER]
                            else:
                                dict_data[TARGET_UPPER] = []
                            if TARGET_LOWER in keys:
                                df_lower = df_pred.loc[(df_pred[TAG_NAME_REQUEST] == TARGET_LOWER)][
                                    'tag_value'].iloc[-1]
                                dict_data[TARGET_LOWER] = old_dict[TARGET_LOWER]
                            else:
                                dict_data[TARGET_LOWER] = []
                            if TARGET_PRED_UPPER in keys:
                                dict_data[TARGET_PRED_UPPER] = old_dict[TARGET_PRED_UPPER]
                            else:
                                dict_data[TARGET_PRED_UPPER] = []
                            if TARGET_PRED_LOWER in keys:
                                dict_data[TARGET_PRED_LOWER] = old_dict[TARGET_PRED_LOWER]
                            else:
                                dict_data[TARGET_PRED_LOWER] = []
                            dict_data["x-axis"] = df_cycle.tolist()
                            dict_data["unit"] = df_unit_d
                            """
                            alert signal calculations if pred_target value is between target upper and target 
                            lower values then alert is green other wise alert is red (Zero is green and One is Red) 
                            """
                            if df_target is None:
                                dict_data["alert"] = 2
                            elif df_lower <= df_target <= df_upper:
                                dict_data["alert"] = 0
                            elif df_lower <= df_target >= df_upper:
                                dict_data["alert"] = 1
                            elif df_lower >= df_target <= df_upper:
                                dict_data["alert"] = 1
                            else:
                                if DEBUG == 1:
                                    print("sorry")

                        graph.append(dict_data)
                        return graph

                if query_params[FEATURE_REQUEST] == HGI_DISTRIBUTION:

                    """ 
                    it wil gives the data for the HGI distribution thumbnail and graph 
                    
                    """
                    graph = []
                    dict_data = {"frequency": [],
                                 "x-axis": [],
                                 "unit": None
                                 }
                    if START_DATE_REQUEST not in query_params and END_DATE_REQUEST not in query_params and DRUM_REQUEST not in query_params:

                        """"
                        it will give the thumbnail data for the online drum 
                      
                        """
                        online_drum_data = django_search_query_all(HGI_ONLINE_DRUM_QUERY)
                        drum_online = pd.DataFrame(online_drum_data)
                        if not drum_online.empty:
                            online_drum = drum_online['drum_online'].iloc[0]
                            if drum_online[DRUM_ONLINE].iloc[0] == SOUTH_DRUM_ONLINE:
                                distribution_result_data = django_search_query_all(
                                    HGI_DISTRIBUTION_THUMBNAIL.format(online_drum))
                            if drum_online[DRUM_ONLINE].iloc[0] == NORTH_DRUM_ONLINE:
                                distribution_result_data = django_search_query_all(
                                    HGI_DISTRIBUTION_THUMBNAIL.format(online_drum))
                        else:
                            if DEBUG == 1:
                                print("sorry")
                    if START_DATE_REQUEST in query_params and END_DATE_REQUEST in query_params and DRUM_REQUEST not in query_params:

                        """
                         it will gives the data for the graph in between the start and end date for the online drum
                        
                        """
                        online_drum_data = django_search_query_all(HGI_ONLINE_DRUM_QUERY)
                        drum_online = pd.DataFrame(online_drum_data)

                        if not drum_online.empty:
                            online_drum = drum_online['drum_online'].iloc[0]
                            if drum_online[DRUM_ONLINE].iloc[0] == SOUTH_DRUM_ONLINE:
                                distribution_result_data = django_search_query_all(
                                    HGI_DISTRIBUTION_DETAILED_GRAPH.format(online_drum,
                                                                           query_params[
                                                                               START_DATE_REQUEST],
                                                                           query_params[
                                                                               END_DATE_REQUEST]))
                            if drum_online[DRUM_ONLINE].iloc[0] == NORTH_DRUM_ONLINE:
                                distribution_result_data = django_search_query_all(
                                    HGI_DISTRIBUTION_DETAILED_GRAPH.format(online_drum,
                                                                           query_params[
                                                                               START_DATE_REQUEST],
                                                                           query_params[
                                                                               END_DATE_REQUEST]))
                        else:
                            if DEBUG == 1:
                                print("sorry")
                    if START_DATE_REQUEST in query_params and END_DATE_REQUEST in query_params and DRUM_REQUEST in query_params:

                        """
                        it will give the data based up on the drum request in between the start date and end date 
                        """
                        if query_params[DRUM_REQUEST] == SOUTH_DRUM_ONLINE:
                            distribution_result_data = django_search_query_all(
                                HGI_DISTRIBUTION_DETAILED_GRAPH.format(SOUTH_DRUM_ONLINE,
                                                                       query_params[
                                                                           START_DATE_REQUEST],
                                                                       query_params[
                                                                           END_DATE_REQUEST]))
                        if query_params[DRUM_REQUEST] == NORTH_DRUM_ONLINE:
                            distribution_result_data = django_search_query_all(
                                HGI_DISTRIBUTION_DETAILED_GRAPH.format(NORTH_DRUM_ONLINE,
                                                                       query_params[
                                                                           START_DATE_REQUEST],
                                                                       query_params[
                                                                           END_DATE_REQUEST]))
                        else:
                            if DEBUG == 1:
                                print("sorry")

                    distribution_data = pd.DataFrame(distribution_result_data)
                    if not distribution_data.empty:
                        distribution_data = distribution_data.where(pd.notnull(distribution_data) == True, None)
                        distribution_data.dropna(inplace=True)
                        distribution_data.sort_values('actual_lab_hgi', ascending=True, inplace=True)
                        frequency = distribution_data["count"]
                        actual_result_values = distribution_data["actual_lab_hgi"]
                        dict_data["frequency"] = frequency.tolist()
                        dict_data["x-axis"] = actual_result_values.tolist()
                        dict_data["unit"] = 'HGI'
                    graph.append(dict_data)
                    return graph

    def get_values(self):
        """
        This will return the critical trends value for the selected module
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.equipment == COKE_DRUM_VALUE and self.module == PDI_VALUE:
                return JsonResponse(self.get_pdi_critical_trends_data(), safe=False)
            if self.equipment == FURNACE_VALUE:
                return JsonResponse(self.get_furnace_critical_trends_data(), safe=False)
            if self.equipment == COKE_DRUM_VALUE and self.module == OUTAGE_VALUE:
                return JsonResponse(self.get_outage_critical_trends_data(), safe=False)
            if self.equipment == COKE_DRUM_VALUE and self.module == HGI_VALUE:
                return JsonResponse(self.get_hgi_critical_trends_data(), safe=False)

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
def get_critical_trends(request, equipment_name=None, module_name=None):
    """
    This function will return the critical trends value for the selected module
    :param module_name: module name
    :param equipment_name: equipment name
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
                obj = CriticalTrends(query_params, equipment_name, module_name)
                return obj.get_values()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
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
        return error_instance(e)

    finally:
        if obj:
            del obj
