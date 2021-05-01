"""
File                :   pdi_graph_old.py

Description         :   This will return the graph data for the selected Overhead PDI module

Author              :   LivNSense Technologies

Date Created        :   07-01-2020

Date Last modified :    09-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import DETAILED_OUTAGE_GRAPH_NULL_START_DATE, DETAILED_OUTAGE_GRAPH, \
    DETAILED_OUTAGE_MODULE_MULTILINE_GRAPH, MIN_MAX_DATA
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, django_search_query_all
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, \
    json_InternalServerError, asert_res
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, TAG_NAME_REQUEST, START_DATE_REQUEST, OUTAGE_VALUE, MULTILINE_REQUEST, \
    OUTAGE_MODULE_LEVEL_PREDICTED_TAG, \
    OUTAGE_MODULE_LEVEL_ACTUAL_TAG, TIMESTAMP_KEY, UNIT, DESCRIPTION, \
    TAG_VALUE, MIN_VALUE, MAX_VALUE, LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class OutageGraph(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the selected outage module
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

    def get_outage(self):
        """
        This will return the graph data for the outage module
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.equipment == COKE_DRUM_VALUE and self.module == OUTAGE_VALUE:
                """
                This will return the graph data for the selected outage module
                """
                query_params = {
                    TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }
                MODULE_LEVEL_MULTILINE_TAG = tuple(LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH)
                if MULTILINE_REQUEST in self.query_params.GET:
                    """
                    This will return the graph data for the actual and predicted tags for the selected outage module 
                    """
                    query_params[MULTILINE_REQUEST] = self.query_params.GET[MULTILINE_REQUEST]

                if query_params:
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST] and \
                            MULTILINE_REQUEST not in query_params:
                        graph_data = django_search_query_all(
                            DETAILED_OUTAGE_GRAPH_NULL_START_DATE.format(
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and MULTILINE_REQUEST not in query_params:
                        graph_data = django_search_query_all(
                            DETAILED_OUTAGE_GRAPH.format(
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]))
                    elif query_params[START_DATE_REQUEST] and query_params[MULTILINE_REQUEST]:
                        if query_params[TAG_NAME_REQUEST] in LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH:
                            graph_data = django_search_query_all(
                                DETAILED_OUTAGE_MODULE_MULTILINE_GRAPH.format(
                                    self.module,
                                    MODULE_LEVEL_MULTILINE_TAG,
                                    query_params[START_DATE_REQUEST],
                                    query_params[END_DATE_REQUEST]))

                        else:
                            graph_data = django_search_query_all(
                                DETAILED_OUTAGE_GRAPH.format(
                                    self.module,
                                    query_params[TAG_NAME_REQUEST],
                                    query_params[START_DATE_REQUEST],
                                    query_params[END_DATE_REQUEST]))

                    df_data = pd.DataFrame(graph_data)
                    min_max = django_search_query_all(
                        MIN_MAX_DATA.format(
                            self.module,
                            query_params[TAG_NAME_REQUEST]
                        ))
                    df_min_max_data = pd.DataFrame(min_max)
                    graph = []

                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                        df_unit = df_data[UNIT].iloc[0]
                        df_description = df_data[DESCRIPTION].iloc[0]
                        df_timestamp = list(dict.fromkeys(list(df_data[TIMESTAMP_KEY])))

                        if query_params[TAG_NAME_REQUEST] in LIST_OF_OUTAGE_MODULE_LEVEL_MULTILINE_TAGS_GRAPH:
                            df_result = df_data.groupby(TAG_NAME_REQUEST)
                            actual_north_data = []
                            predicted_north_data = []
                            actual_south_data = []
                            predicted_south_data = []
                            if len(df_result) == 2:
                                df_description = \
                                    df_data[df_data[TAG_NAME_REQUEST] == query_params[TAG_NAME_REQUEST]][
                                        DESCRIPTION].iloc[0]
                                df_north_actual = df_result.get_group(OUTAGE_MODULE_LEVEL_ACTUAL_TAG)
                                actual_north_data = list(df_north_actual['north_drum_tag_value'])
                                df_north_predicted = df_result.get_group(OUTAGE_MODULE_LEVEL_PREDICTED_TAG)
                                predicted_north_data = list(df_north_predicted['north_drum_tag_value'])
                                df_south_actual = df_result.get_group(OUTAGE_MODULE_LEVEL_ACTUAL_TAG)
                                actual_south_data = list(df_south_actual['south_drum_tag_value'])
                                df_south_predicted = df_result.get_group(OUTAGE_MODULE_LEVEL_PREDICTED_TAG)
                                predicted_south_data = list(df_south_predicted['south_drum_tag_value'])
                            elif len(df_result) == 1:

                                if df_result[TAG_NAME_REQUEST] == OUTAGE_MODULE_LEVEL_ACTUAL_TAG:
                                    df_description = \
                                        df_data[df_data[TAG_NAME_REQUEST] == OUTAGE_MODULE_LEVEL_ACTUAL_TAG][
                                            DESCRIPTION].iloc[0]
                                    df_north_actual = df_result.get_group(OUTAGE_MODULE_LEVEL_ACTUAL_TAG)
                                    actual_north_data = list(df_north_actual['north_drum_tag_value'])
                                    df_south_actual = df_result.get_group(OUTAGE_MODULE_LEVEL_ACTUAL_TAG)
                                    actual_south_data = list(df_south_actual['south_drum_tag_value'])

                                elif df_result[TAG_NAME_REQUEST] != OUTAGE_MODULE_LEVEL_ACTUAL_TAG:
                                    df_description = \
                                        df_data[df_data[TAG_NAME_REQUEST] == OUTAGE_MODULE_LEVEL_PREDICTED_TAG][
                                            DESCRIPTION].iloc[0]
                                    df_north_predicted = df_result.get_group(OUTAGE_MODULE_LEVEL_PREDICTED_TAG)
                                    predicted_north_data = list(df_north_predicted['north_drum_tag_value'])
                                    df_south_predicted = df_result.get_group(OUTAGE_MODULE_LEVEL_PREDICTED_TAG)
                                    predicted_south_data = list(df_south_predicted['south_drum_tag_value'])

                            temp = {"north_actual": actual_north_data, "north_predicted": predicted_north_data,
                                    "south_actual": actual_south_data, "south_predicted": predicted_south_data,
                                    "x_axis": df_timestamp,
                                    "unit": df_unit,
                                    "description": df_description}

                        else:
                            temp = {"y_axis": list(df_data[TAG_VALUE]), "x_axis": df_timestamp,
                                    "unit": df_unit, "description": df_description}
                        if not df_min_max_data.empty:
                            temp["min_data"] = df_min_max_data[MIN_VALUE].iloc[0]
                            temp["max_data"] = df_min_max_data[MAX_VALUE].iloc[0]
                        else:
                            temp["min_data"] = None
                            temp["max_data"] = None
                        graph.append(temp)

                    return graph

        except AssertionError as e:
            log_error("Exception due to : %s" + str(e))
            return asert_res(e)
        except Exception as e:
            log_error("Exception due to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_outage_graph(request, equipment_name=None, module_name=None):
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
            obj = OutageGraph(query_params, equipment_name, module_name)
            return obj.get_outage()

        log_debug(METHOD_NOT_ALLOWED)
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)

    except Exception as e:

        excMsg = "get_outage_graph_data API : " + str(error_instance(e))

        return excMsg

    finally:

        if obj:
            del obj
