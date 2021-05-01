"""
File                :   furnace_multiline.py

Description         :   This will display the multi line graph for the hgi module

Author              :   LivNSense Technologies

Date Created        :   09-07-2020

Date Last modified :    09-07-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import numpy as np
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.Database.Configuration import FURNACE_A_MULTI_LINE_TAGS, FURNACE_B_MULTI_LINE_TAGS, \
    FURNACE_A_SECONDARY_TAGS, FURNACE_B_SECONDARY_TAGS, FURNACE_A_PRIMARY_MULTI_LINE_TAGS, \
    FURNACE_B_PRIMARY_MULTI_LINE_TAGS
from ApplicationInterface.Database.Queries import FURNACE_MULTI_LINE_GRAPH, FURNACE_MIN_MAX_DATA, \
    FURNACE_SECONDARY_MULTI_LINE_GRAPH, FURNACE_SECONDARY_MIN_MAX_DATA, ONE_YEAR_FURNACE_MULTI_LINE_GRAPH, \
    ONE_YEAR_FURNACE_MULTI_LINE_GRAPH_DATA_COUNT, ONE_YAER_FURNACE_SECONDARY_MULTI_LINE_GRAPH, \
    ONE_YAER_FURNACE_SECONDARY_MULTI_LINE_DATA_COUNT
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    END_DATE_REQUEST, START_DATE_REQUEST, TAG_NAME_REQUEST, MIN_VALUE, \
    MAX_VALUE, TAG_VALUE, FURNACE_VALUE, FURNACE_A_VALUE, FURNACE_B_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class FurnaceMultiLine(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the furnace multi line graph
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

    def get_values(self):
        """
        This will return the furnace multi line graph data
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            graph = []
            tag_left = None
            dict_data = {}
            offset = None
            limit = None
            if self.equipment == FURNACE_VALUE:
                query_params = {
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }

                if 'offset' in self.query_params.GET:
                    offset = self.query_params.GET['offset']
                if 'limit' in self.query_params.GET:
                    limit = self.query_params.GET['limit']
                multi_line_tags = None
                if self.module == FURNACE_A_VALUE:
                    multi_line_tags = tuple(FURNACE_A_MULTI_LINE_TAGS)
                    tags_list = FURNACE_A_SECONDARY_TAGS
                elif self.module == FURNACE_B_VALUE:
                    multi_line_tags = tuple(FURNACE_B_MULTI_LINE_TAGS)
                    tags_list = FURNACE_B_SECONDARY_TAGS
                if TAG_NAME_REQUEST not in self.query_params.GET and 'limit' not in self.query_params.GET and 'offset' not in self.query_params.GET:
                    """
                    This will return the furnace multi line graph data without the secondary tag
                    """
                    dict_data["tags_list"] = tags_list
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST]:
                        self._psql_session.execute(FURNACE_MULTI_LINE_GRAPH.format(
                            self.module,
                            multi_line_tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST]))
                        df_data = pd.DataFrame(self._psql_session.fetchall())

                        self._psql_session.execute(FURNACE_MIN_MAX_DATA.format(
                            self.module,
                            multi_line_tags))
                        df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
                elif TAG_NAME_REQUEST in self.query_params.GET and 'limit' not in self.query_params.GET and 'offset' not in self.query_params.GET:
                    query_params[TAG_NAME_REQUEST] = self.query_params.GET[TAG_NAME_REQUEST]
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST] and query_params[
                        TAG_NAME_REQUEST]:
                        """
                        This will return the furnace multi line graph data with the primary tag
                        """
                        tags = None

                        if self.module == FURNACE_A_VALUE:
                            if query_params[TAG_NAME_REQUEST] in FURNACE_A_SECONDARY_TAGS:
                                tags = FURNACE_A_PRIMARY_MULTI_LINE_TAGS
                            else:
                                return JsonResponse("Sorry tag is not there", safe=False)

                        elif self.module == FURNACE_B_VALUE:

                            if query_params[TAG_NAME_REQUEST] in FURNACE_B_SECONDARY_TAGS:
                                tags = FURNACE_B_PRIMARY_MULTI_LINE_TAGS
                            else:
                                return JsonResponse("Sorry tag is not there", safe=False)

                        self._psql_session.execute(FURNACE_SECONDARY_MULTI_LINE_GRAPH.format(
                            self.module,
                            tags,
                            query_params[TAG_NAME_REQUEST],
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST]))

                        df_data = pd.DataFrame(self._psql_session.fetchall())
                        self._psql_session.execute(FURNACE_SECONDARY_MIN_MAX_DATA.format(
                            self.module,
                            tags,
                            query_params[TAG_NAME_REQUEST]
                        ))
                        df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
                elif TAG_NAME_REQUEST not in self.query_params.GET and 'limit' in self.query_params.GET and 'offset' in self.query_params.GET:
                    """
                    This will return the furnace multi line graph data without the secondary tag
                    """
                    dict_data["tags_list"] = tags_list
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST]:
                        self._psql_session.execute(ONE_YEAR_FURNACE_MULTI_LINE_GRAPH.format(
                            self.module,
                            multi_line_tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST], offset, limit))
                        df_data = pd.DataFrame(self._psql_session.fetchall())
                        self._psql_session.execute(ONE_YEAR_FURNACE_MULTI_LINE_GRAPH_DATA_COUNT.format(
                            self.module,
                            multi_line_tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST], offset, limit))
                        df_row_count = pd.DataFrame(self._psql_session.fetchall())
                        self._psql_session.execute(FURNACE_MIN_MAX_DATA.format(
                            self.module,
                            multi_line_tags))
                        df_min_max_data = pd.DataFrame(self._psql_session.fetchall())
                elif TAG_NAME_REQUEST in self.query_params.GET and 'limit' in self.query_params.GET and 'offset' in self.query_params.GET:
                    query_params[TAG_NAME_REQUEST] = self.query_params.GET[TAG_NAME_REQUEST]
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST] and query_params[
                        TAG_NAME_REQUEST]:
                        """
                        This will return the furnace multi line graph data with the primary tag
                        """
                        tags = None

                        if self.module == FURNACE_A_VALUE:
                            if query_params[TAG_NAME_REQUEST] in FURNACE_A_SECONDARY_TAGS:
                                tags = FURNACE_A_PRIMARY_MULTI_LINE_TAGS
                            else:
                                return JsonResponse("Sorry tag is not there", safe=False)

                        elif self.module == FURNACE_B_VALUE:

                            if query_params[TAG_NAME_REQUEST] in FURNACE_B_SECONDARY_TAGS:
                                tags = FURNACE_B_PRIMARY_MULTI_LINE_TAGS
                            else:
                                return JsonResponse("Sorry tag is not there", safe=False)

                        self._psql_session.execute(ONE_YAER_FURNACE_SECONDARY_MULTI_LINE_GRAPH.format(
                            self.module,
                            tags,
                            query_params[TAG_NAME_REQUEST],
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST],offset, limit))

                        df_data = pd.DataFrame(self._psql_session.fetchall())
                        self._psql_session.execute(ONE_YAER_FURNACE_SECONDARY_MULTI_LINE_DATA_COUNT.format(
                            self.module,
                            tags,
                            query_params[TAG_NAME_REQUEST],
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST],
                            query_params[START_DATE_REQUEST], offset, limit))
                        df_row_count = pd.DataFrame(self._psql_session.fetchall())
                        self._psql_session.execute(FURNACE_SECONDARY_MIN_MAX_DATA.format(
                            self.module,
                            tags,
                            query_params[TAG_NAME_REQUEST]
                        ))
                        df_min_max_data = pd.DataFrame(self._psql_session.fetchall())

                if not df_data.empty:
                    df_data = df_data.where(pd.notnull(df_data) == True, None)
                    df_time = list(df_data['timestamp'].unique())
                    df = df_data.groupby(TAG_NAME_REQUEST)

                    if not df_min_max_data.empty:
                        for name, group in df:
                            if name in list(df_min_max_data[TAG_NAME_REQUEST]):
                                if name not in ['BIL.39.H3901A_PredRunlength.IDMS', 'BIL.39.H3901B_PredRunlength.IDMS']:
                                    dict_data[name] = {
                                        "data": group[TAG_VALUE].to_list(),
                                        "description": group['description'].iloc[0],
                                        "unit": group['unit'].iloc[0],
                                        "min_data": df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST] == name][MIN_VALUE].iloc[0],
                                        "max_data": df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST] == name][
                                            MAX_VALUE].iloc[0]
                                    }
                                else:
                                    group['tag_value'] = group['tag_value'].astype(float).round()
                                    group = group.where(pd.notnull(group) == True, None)
                                    dict_data[name] = {
                                        "data": group[TAG_VALUE].to_list(),
                                        "description": group['description'].iloc[0],
                                        "unit": group['unit'].iloc[0],
                                        "min_data": df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST] == name][MIN_VALUE].iloc[0],
                                        "max_data": df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST] == name][
                                            MAX_VALUE].iloc[0]
                                    }

                            else:
                                dict_data[name] = {
                                    "data": group[TAG_VALUE].to_list(),
                                    "description": group['description'].iloc[0],
                                    "unit": group['unit'].iloc[0],
                                    "min_data": df_min_max_data[
                                        df_min_max_data[TAG_NAME_REQUEST] == name][MIN_VALUE].iloc[0],
                                    "max_data": df_min_max_data[
                                        df_min_max_data[TAG_NAME_REQUEST] == name][
                                        MAX_VALUE].iloc[0]
                                }

                    dict_data["x-axis"] = {
                        "data": df_time
                    }
                    if 'limit' and 'offset' not in self.query_params.GET:
                        pass
                    else:
                        dict_data['row_count'] = int(df_row_count['row_count'].iloc[0])

                    if TAG_NAME_REQUEST not in self.query_params.GET:
                        if self.module == FURNACE_A_VALUE:
                            tag_left = list(FURNACE_A_MULTI_LINE_TAGS - dict_data.keys())
                        else:
                            tag_left = list(FURNACE_B_MULTI_LINE_TAGS - dict_data.keys())

                    elif TAG_NAME_REQUEST in self.query_params.GET:
                        if self.module == FURNACE_A_VALUE:
                            primary_A_tags = []
                            primary_A_tags.append(FURNACE_A_PRIMARY_MULTI_LINE_TAGS)
                            primary_A_tags.append(query_params[TAG_NAME_REQUEST])
                            tag_left = list(primary_A_tags - dict_data.keys())

                        else:
                            primary_B_tags = []
                            primary_B_tags.append(FURNACE_B_PRIMARY_MULTI_LINE_TAGS)
                            primary_B_tags.append(query_params[TAG_NAME_REQUEST])
                            tag_left = list(primary_B_tags - dict_data.keys())

                    for each in tag_left:
                        dict_data[each] = {
                            "data": [],
                            "description": None,
                            "unit": None,
                            "min_data": df_min_max_data[
                                df_min_max_data[TAG_NAME_REQUEST] == each][MIN_VALUE].iloc[0],
                            "max_data": df_min_max_data[
                                df_min_max_data[TAG_NAME_REQUEST] == each][
                                MAX_VALUE].iloc[0]
                        }

                else:
                    if TAG_NAME_REQUEST not in self.query_params.GET:
                        if self.module == FURNACE_A_VALUE:
                            tag_left = FURNACE_A_MULTI_LINE_TAGS
                        else:
                            tag_left = FURNACE_B_MULTI_LINE_TAGS

                    elif TAG_NAME_REQUEST in self.query_params.GET:
                        if self.module == FURNACE_A_VALUE:
                            primary_A_tags = []
                            primary_A_tags.append(FURNACE_A_PRIMARY_MULTI_LINE_TAGS)
                            primary_A_tags.append(query_params[TAG_NAME_REQUEST])
                            tag_left = primary_A_tags

                        else:
                            primary_B_tags = []
                            primary_B_tags.append(FURNACE_B_PRIMARY_MULTI_LINE_TAGS)
                            primary_B_tags.append(query_params[TAG_NAME_REQUEST])
                            tag_left = primary_B_tags

                    for each in tag_left:
                        dict_data[each] = {
                            "data": [],
                            "description": None,
                            "unit": None,
                            "min_data": df_min_max_data[
                                df_min_max_data[TAG_NAME_REQUEST] == each][MIN_VALUE].iloc[0],
                            "max_data": df_min_max_data[
                                df_min_max_data[TAG_NAME_REQUEST] == each][
                                MAX_VALUE].iloc[0]
                        }

                    dict_data["x-axis"] = {
                        "data": []
                    }
                graph.append(dict_data)
                return JsonResponse(graph, safe=False)

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
def get_furnace_multi_line(request, equipment_name=None, module_name=None):
    """
    This function will return the furnace multi line graph data
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
                obj = FurnaceMultiLine(query_params, equipment_name, module_name)
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
