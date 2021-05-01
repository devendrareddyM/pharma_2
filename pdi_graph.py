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

from ApplicationInterface.Database.Configuration import NORTH_TAGS, SOUTH_TAGS
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, TAG_NAME_REQUEST, START_DATE_REQUEST, MOVING_AVG_REQUEST, PDI_VALUE, \
    ONLINE_NORTH_TAG, ONLINE_SOUTH_TAG, NORTH, SOUTH, TIMESTAMP_KEY, TAG_VALUE, UNIT, \
    DESCRIPTION, MIN_VALUE, MAX_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import DETAILED_PDI_GRAPH, DETAILED_PDI_GRAPH_NULL_START_DATE, \
    PDI_MOVING_AVG_GRAPH, \
    DETAILED_PDI_NORTH_GRAPH_NULL_START_DATE, DETAILED_PDI_NORTH_GRAPH, DETAILED_PDI_SOUTH_GRAPH_NULL_START_DATE, \
    DETAILED_PDI_SOUTH_GRAPH, MIN_MAX_DATA
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class PdiGraph(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the selected overhead pdi module
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

    def get_overhead_pdi(self):
        """
        This will return the graph data for the over head pdi module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.equipment == COKE_DRUM_VALUE and self.module == PDI_VALUE:
                """
                This will return the graph data for the selected overhead pdi module
                """
                query_params = {
                    TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }
                if MOVING_AVG_REQUEST in self.query_params.GET:
                    """
                    This will return the graph data for the selected overhead pdi module with the moving average
                    """
                    query_params[MOVING_AVG_REQUEST] = self.query_params.GET[MOVING_AVG_REQUEST]
                north_tags_list = tuple(NORTH_TAGS)
                south_tags_list = tuple(SOUTH_TAGS)
                if self.query_params:
                    if query_params[TAG_NAME_REQUEST] in north_tags_list:
                        if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                                MOVING_AVG_REQUEST not in query_params:
                            self._psql_session.execute(
                                DETAILED_PDI_NORTH_GRAPH_NULL_START_DATE.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[END_DATE_REQUEST]))
                        elif query_params[START_DATE_REQUEST] and MOVING_AVG_REQUEST not in query_params:
                            self._psql_session.execute(
                                DETAILED_PDI_NORTH_GRAPH.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[START_DATE_REQUEST],
                                    query_params[END_DATE_REQUEST]))

                    elif query_params[TAG_NAME_REQUEST] in south_tags_list:
                        if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                                MOVING_AVG_REQUEST not in query_params:
                            self._psql_session.execute(
                                DETAILED_PDI_SOUTH_GRAPH_NULL_START_DATE.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[END_DATE_REQUEST]))
                        elif query_params[START_DATE_REQUEST] and MOVING_AVG_REQUEST not in query_params:
                            self._psql_session.execute(
                                DETAILED_PDI_SOUTH_GRAPH.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[START_DATE_REQUEST],
                                    query_params[END_DATE_REQUEST]))

                    else:
                        if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                                MOVING_AVG_REQUEST not in query_params:
                            """
                            This will return the graph data for the selected overhead pdi module without the moving average
                            """
                            self._psql_session.execute(
                                DETAILED_PDI_GRAPH_NULL_START_DATE.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[END_DATE_REQUEST]))
                        elif query_params[START_DATE_REQUEST] and MOVING_AVG_REQUEST not in query_params:
                            self._psql_session.execute(
                                DETAILED_PDI_GRAPH.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[START_DATE_REQUEST],
                                    query_params[END_DATE_REQUEST]))
                        elif query_params[START_DATE_REQUEST] and query_params[MOVING_AVG_REQUEST]:
                            """
                            This will return the graph data for the selected overhead pdi module with the moving average
                            """
                            if query_params[TAG_NAME_REQUEST] == ONLINE_NORTH_TAG:
                                self._psql_session.execute(
                                    PDI_MOVING_AVG_GRAPH.format(
                                        NORTH,
                                        query_params[START_DATE_REQUEST],
                                        query_params[END_DATE_REQUEST]))
                            elif query_params[TAG_NAME_REQUEST] == ONLINE_SOUTH_TAG:
                                self._psql_session.execute(
                                    PDI_MOVING_AVG_GRAPH.format(
                                        SOUTH,
                                        query_params[START_DATE_REQUEST],
                                        query_params[END_DATE_REQUEST]))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    """
                    Getting Min and Max Values
                    """
                    self._psql_session.execute(MIN_MAX_DATA.format(self.module,
                                                                   query_params[TAG_NAME_REQUEST]))
                    min_data = pd.DataFrame(self._psql_session.fetchall())
                    temp = {}
                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                        if MOVING_AVG_REQUEST in query_params:
                            temp["y_axis"] = list(df_data[TAG_VALUE])
                            temp["x_axis"] = list(df_data[TIMESTAMP_KEY])

                        else:
                            temp["unit"] = df_data[UNIT].iloc[0]
                            temp["description"] = df_data[DESCRIPTION].iloc[0]
                            temp["y_axis"] = list(df_data[TAG_VALUE])
                            temp["x_axis"] = list(df_data[TIMESTAMP_KEY])

                        if not min_data.empty:
                            temp["min_data"] = min_data[MIN_VALUE].iloc[0]
                            temp["max_data"] = min_data[MAX_VALUE].iloc[0]
                        else:
                            temp["min_data"] = None
                            temp["max_data"] = None
                        graph.append(temp)

                    return graph
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
def get_pdi_graph(request, equipment_name=None, module_name=None):
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
            obj = PdiGraph(query_params, equipment_name, module_name)
            return obj.get_overhead_pdi()

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
    #     return {MESSAGE_KEY: "Token Expired"}
    except Exception as e:
        excMsg = "get_pdi_graph_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj

