"""
File                :   pdi_graph_old.py

Description         :   This will return the graph data for the selected Overhead PDI module

Author              :   LivNSense Technologies

Date Created        :   07-01-2020

Date Last modified :    09-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    END_DATE_REQUEST, START_DATE_REQUEST, FURNACE_VALUE, DAILY_AVG_REQUEST, \
    FURNACE_A_CHECK_STATUS_TAGS, \
    FURNACE_B_CHECK_STATUS_TAGS, PASS_A_VALUE, PASS_A_ONLINE_STATUS_TAG, PASS_A_SPALL_STATUS_TAG, \
    PASS_B_ONLINE_STATUS_TAG, PASS_B_SPALL_STATUS_TAG, ONE, ZERO, TAG_NAME_REQUEST, CREATE_TS, FURNACE_A_TAGS, \
    FURNACE_B_TAGS, TAG_VALUE, CLEAN, FLAG_VALUE, UNIT, DESCRIPTION, MAX_VALUE, MIN_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import DETAILED_TMT_GRAPH_NULL_START_DATE, DETAILED_TMT_GRAPH, \
    DETAILED_TMT_DAILY_AVG_GRAPH, \
    MIN_MAX_DATA, FURNACE_TMT_SPALL_CHECK, MIN_MAX_SPALL_DATA, DETAILED_TMT_SPALL_GRAPH, \
    DETAILED_TMT_SPALL_GRAPH_NULL_START_DATE
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class FurnaceGraph(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the selected furnace module
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

    def get_TMT_data(self):
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            query_params = {
                TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
                START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
            }
            if DAILY_AVG_REQUEST in self.query_params.GET:
                """
                This will return the graph data for the selected furnace module with the daily average
                """
                query_params[DAILY_AVG_REQUEST] = self.query_params.GET[DAILY_AVG_REQUEST]

            if self.equipment == FURNACE_VALUE and query_params:
                """
                This will return the graph data for the selected furnace module
                """
                temp = {}
                graph = []
                try:
                    """
                        This will return the module level graph data for the furnace module
                        """
                    temp["is_spall"] = False
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                            DAILY_AVG_REQUEST not in query_params:
                        """
                            This will return the graph data for the selected furnace module without the daily average
                            """
                        self._psql_session.execute(
                            DETAILED_TMT_GRAPH_NULL_START_DATE.format(
                                query_params[TAG_NAME_REQUEST],
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and DAILY_AVG_REQUEST not in query_params:
                        self._psql_session.execute(
                            DETAILED_TMT_GRAPH.format(
                                query_params[TAG_NAME_REQUEST],
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and query_params[DAILY_AVG_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_TMT_DAILY_AVG_GRAPH.format(
                                query_params[TAG_NAME_REQUEST],
                                query_params[END_DATE_REQUEST],
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST])
                        )
                    else:
                        print("Ignore this!")
                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    self._psql_session.execute(
                        MIN_MAX_DATA.format(
                            self.module,
                            query_params[TAG_NAME_REQUEST]))
                    df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values(CREATE_TS, ascending=True, inplace=True)

                        if "Clean" not in query_params[TAG_NAME_REQUEST]:
                            unit_values = \
                                df_data[df_data[TAG_NAME_REQUEST] == query_params[TAG_NAME_REQUEST]]['unit'].iloc[0]
                            actual_values = df_data[df_data[TAG_NAME_REQUEST] == query_params[TAG_NAME_REQUEST]][
                                TAG_VALUE].values

                        else:
                            unit_values = df_data[
                                ~df_data[TAG_NAME_REQUEST].str.contains(CLEAN) & ~df_data[
                                    TAG_NAME_REQUEST].str.contains(
                                    FLAG_VALUE)][UNIT].iloc[0]
                            actual_values = df_data[
                                ~df_data[TAG_NAME_REQUEST].str.contains(CLEAN) & ~df_data[
                                    TAG_NAME_REQUEST].str.contains(
                                    FLAG_VALUE)][TAG_VALUE].values
                        predicted_values = df_data[df_data[TAG_NAME_REQUEST].str.contains(CLEAN)][TAG_VALUE].values
                        description = df_data[
                            ~df_data[TAG_NAME_REQUEST].str.contains(CLEAN) & ~df_data[TAG_NAME_REQUEST].str.contains(
                                FLAG_VALUE)][
                            DESCRIPTION].iloc[0]

                        temp["unit"] = unit_values
                        temp["description"] = description
                        temp["actual"] = actual_values.tolist()
                        temp["predicted"] = predicted_values.tolist()
                        temp["x_axis"] = list(dict.fromkeys(list(df_data[CREATE_TS])))
                        if not df_min_max_data.empty:
                            temp["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
                            temp["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
                        else:
                            temp["min_data"] = None
                            temp["max_data"] = None
                        graph.append(temp)
                    return graph
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))
        except Exception as e:
            log_error("Exception due to:%s" + str(e))

    # def get_spall_data(self):
    #     try:
    #         assert self._db_connection, {
    #             STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
    #             MESSAGE_KEY: DB_ERROR}
    #         query_params = {
    #             TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
    #             START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
    #             END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
    #         }
    #         if self.equipment == FURNACE_VALUE and query_params:
    #             """
    #             This will return the graph data for the selected furnace module
    #             """
    #             temp = {}
    #             graph = []
    #             try:
    #                 """
    #                     This will return the module level graph data for the furnace module' spall effectiveness status
    #                     """
    #                 if self.module == PASS_A_VALUE:
    #                     furnace_tags = FURNACE_A_TAGS
    #                 else:
    #                     furnace_tags = FURNACE_B_TAGS
    #                 temp["is_spall"] = True
    #                 if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST]:
    #                     """
    #                         This will return the graph data for the selected spall furnace module without the daily average
    #                         """
    #                     self._psql_session.execute(
    #                         DETAILED_TMT_SPALL_GRAPH_NULL_START_DATE.format(
    #                             self.module,
    #                             query_params[TAG_NAME_REQUEST],
    #                             query_params[END_DATE_REQUEST]))
    #                 elif query_params[START_DATE_REQUEST]:
    #                     self._psql_session.execute(
    #                         DETAILED_TMT_SPALL_GRAPH.format(
    #                             self.module,
    #                             query_params[TAG_NAME_REQUEST],
    #                             query_params[START_DATE_REQUEST],
    #                             query_params[END_DATE_REQUEST]))
    #                 else:
    #                     print("No daily average in spall!")
    #                 df_data = pd.DataFrame(self._psql_session.fetchall())
    #                 self._psql_session.execute(
    #                     MIN_MAX_SPALL_DATA.format(
    #                         self.module,
    #                         query_params[TAG_NAME_REQUEST]))
    #                 df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
    #                 if not df_data.empty:
    #                     df_data = df_data.where(pd.notnull(df_data) == True, None)
    #                     df_data.sort_values(CREATE_TS, ascending=True, inplace=True)
    #                     if query_params[TAG_NAME_REQUEST] in furnace_tags:
    #                         tags_data = df_data.groupby(TAG_NAME_REQUEST)
    #                         for name, group in tags_data:
    #                             temp[name] = list(group[TAG_VALUE])
    #                     else:
    #                         temp["actual"] = list(df_data[TAG_VALUE])
    #                         temp["predicted"] = []
    #                     temp["unit"] = df_data[df_data[TAG_NAME_REQUEST] == query_params[TAG_NAME_REQUEST]][UNIT].iloc[
    #                         0]
    #                     temp["x_axis"] = list(dict.fromkeys(list(df_data[CREATE_TS])))
    #                     temp["description"] = df_data[DESCRIPTION].iloc[0]
    #                     if not df_min_max_data.empty:
    #                         temp["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
    #                         temp["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
    #                     else:
    #                         temp["min_data"] = None
    #                         temp["max_data"] = None
    #                     graph.append(temp)
    #                     return graph
    #                 else:
    #                     return graph
    #             except Exception as e:
    #                 log_error("Exception due to : %s" + str(e))
    #     except AssertionError as e:
    #         log_error("Exception due to : %s" + str(e))
    #         return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
    #                             status=e.args[0][STATUS_KEY])
    #
    #     except Exception as e:
    #         log_error(traceback.format_exc())
    #         return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
    #             traceback.format_exc())},
    #             status=HTTP_500_INTERNAL_SERVER_ERROR)
    #
    # def get_furnace(self):
    #     """
    #     This will return the graph data for the Furnace module
    #     :return: Json response
    #     """
    #     try:
    #         assert self._db_connection, {
    #             STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
    #             MESSAGE_KEY: DB_ERROR}
    #         if self.equipment == FURNACE_VALUE:
    #             """
    #             This will return the graph data for the selected furnace module
    #             """
    #             if self.module == PASS_A_VALUE:
    #                 tag_name = FURNACE_A_CHECK_STATUS_TAGS
    #                 online_tag = PASS_A_ONLINE_STATUS_TAG
    #                 spall_tag = PASS_A_SPALL_STATUS_TAG
    #             else:
    #                 tag_name = FURNACE_B_CHECK_STATUS_TAGS
    #                 online_tag = PASS_B_ONLINE_STATUS_TAG
    #                 spall_tag = PASS_B_SPALL_STATUS_TAG
    #             try:
    #                 self._psql_session.execute(FURNACE_TMT_SPALL_CHECK.format(self.module, tuple(tag_name)))
    #                 df_spall_check = pd.DataFrame(self._psql_session.fetchall())
    #                 df_spall_check.set_index(TAG_NAME_REQUEST, inplace=True)
    #                 df_spall_transposed = df_spall_check.T
    #                 if df_spall_transposed[online_tag].iloc[0] == ONE and df_spall_transposed[spall_tag].iloc[
    #                     0] == ZERO:
    #                     return self.get_TMT_data()
    #                 if df_spall_transposed[online_tag].iloc[0] == ZERO and df_spall_transposed[spall_tag].iloc[
    #                     0] == ONE:
    #                     return self.get_spall_data()
    #                 else:
    #                     pass
    #             except Exception as e:
    #                 log_error("Exception occurred due to" + str(e))
    #     except AssertionError as e:
    #         log_error("Assertion error due to : %s" + str(e))
    #         return JsonResponse({MESSAGE_KEY: e.args[0][MESSAGE_KEY]},
    #                             status=e.args[0][STATUS_KEY])
    #
    #     except Exception as e:
    #         log_error("Exception due to : %s" + str(e))
    #         return JsonResponse({MESSAGE_KEY: EXCEPTION_CAUSE.format(
    #             traceback.format_exc())},
    #             status=HTTP_500_INTERNAL_SERVER_ERROR)

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_furnace_graph(request, equipment_name=None, module_name=None):
    """
    This function will return the graph data for the selected module
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
            #
            # loggedin_user_details = _TokenValidation.validate_token(request)
            # if loggedin_user_details:
            obj = FurnaceGraph(query_params, equipment_name, module_name)
            return obj.get_TMT_data()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
    #
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
    #     return JsonResponse({MESSAGE_KEY: "Token Expired"}, status=HTTP_401_UNAUTHORIZED)
    except Exception as e:
        excMsg = "get_furnace_graph_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
