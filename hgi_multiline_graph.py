"""
File                :   hgi_multiline_graph.py

Description         :   This will display the multi line graph for the hgi module

Author              :   LivNSense Technologies

Date Created        :   11-05-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Configuration import HGI_MULTI_LINE_TAGS, HGI_SECONDARY_TAGS
from ApplicationInterface.Database.Queries import HGI_MIN_MAX_DATA, \
    HGI_MULTI_LINE_GRAPH
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all
# from Database.Configuration import HGI_MULTI_LINE_TAGS, HGI_SECONDARY_TAGS
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, START_DATE_REQUEST, TIMESTAMP_KEY, TAG_NAME_REQUEST, UNIT, DESCRIPTION, \
    MIN_VALUE, \
    MAX_VALUE, HGI_VALUE, HGI_PRED, NORTH_HGI_PRED, NORTH_HGI_ACTUAL, \
    SOUTH_HGI_PRED, SOUTH_HGI_ACTUAL, DEBUG, TAG_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class HgiMultiLine(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the hgi multi line graph
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
        This will return the hgi multi line graph data
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            graph = []
            max_data = None
            min_data = None
            empty_dict = {"data": [],
                          "description": None,
                          "unit": None,
                          "min_data": None,
                          "max_data": None
                          }
            dict1 = {}
            dict2 = {}
            dict3 = {}
            dict4 = {}
            dict5 = {}
            dict6 = {}
            dict7 = {"data": []}
            if self.equipment == COKE_DRUM_VALUE and self.module == HGI_VALUE:
                query_params = {
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }

                if TAG_NAME_REQUEST not in self.query_params.GET:
                    """
                    This will return the hgi multi line graph data without the secondary tag
                    """
                    multi_line_tags = tuple(HGI_MULTI_LINE_TAGS)
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST]:
                        multiline_data = django_search_query_all(HGI_MULTI_LINE_GRAPH.format(
                            self.module,
                            multi_line_tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST]))
                        df_data = pd.DataFrame(multiline_data)

                        min_max = django_search_query_all(HGI_MIN_MAX_DATA.format(
                            self.module,
                            multi_line_tags))

                elif TAG_NAME_REQUEST in self.query_params.GET:
                    query_params[TAG_NAME_REQUEST] = self.query_params.GET[TAG_NAME_REQUEST]
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST] and query_params[
                        TAG_NAME_REQUEST]:
                        """
                        This will return the hgi multi line graph data with the primary tag
                        """
                        if query_params[TAG_NAME_REQUEST] in HGI_SECONDARY_TAGS:
                            HGI_MULTI_LINE_TAGS.append(query_params["tag_name"])
                        else:
                            if DEBUG == 1:
                                print("Sorry tag is not there")
                        tags = tuple(HGI_MULTI_LINE_TAGS)
                        multiline_data = django_search_query_all(HGI_MULTI_LINE_GRAPH.format(
                            self.module,
                            tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST]))
                        df_data = pd.DataFrame(multiline_data)
                        min_max = django_search_query_all(HGI_MIN_MAX_DATA.format(
                            self.module,
                            tags))
                        HGI_MULTI_LINE_TAGS.pop()

                if TAG_NAME_REQUEST in self.query_params.GET:
                    if not df_data.empty:
                        query_params[TAG_NAME_REQUEST] = self.query_params.GET[TAG_NAME_REQUEST]
                        final_dict = {
                            HGI_PRED: dict1,
                            NORTH_HGI_PRED: dict2,
                            NORTH_HGI_ACTUAL: dict3,
                            SOUTH_HGI_PRED: dict4,
                            SOUTH_HGI_ACTUAL: dict5,
                            query_params[TAG_NAME_REQUEST]: dict6,
                            "x-axis": dict7,
                        }
                    else:
                        final_dict = {
                            HGI_PRED: empty_dict,
                            NORTH_HGI_PRED: empty_dict,
                            NORTH_HGI_ACTUAL: empty_dict,
                            SOUTH_HGI_PRED: empty_dict,
                            SOUTH_HGI_ACTUAL: empty_dict,
                            query_params[TAG_NAME_REQUEST]: empty_dict,
                            "x-axis": dict7,
                        }
                else:
                    if not df_data.empty:
                        final_dict = {
                            HGI_PRED: dict1,
                            NORTH_HGI_PRED: dict2,
                            NORTH_HGI_ACTUAL: dict3,
                            SOUTH_HGI_PRED: dict4,
                            SOUTH_HGI_ACTUAL: dict5,
                            "tags_list": HGI_SECONDARY_TAGS,
                            "x-axis": dict7,
                        }
                    else:
                        final_dict = {
                            HGI_PRED: empty_dict,
                            NORTH_HGI_PRED: empty_dict,
                            NORTH_HGI_ACTUAL: empty_dict,
                            SOUTH_HGI_PRED: empty_dict,
                            SOUTH_HGI_ACTUAL: empty_dict,
                            "tags_list": HGI_SECONDARY_TAGS,
                            "x-axis": dict7
                        }
                df_min_max_data = pd.DataFrame(min_max)
                if not df_data.empty:
                    df_data = df_data.where(pd.notnull(df_data) == True, None)
                    df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                    data_now = df_data.groupby(TAG_NAME_REQUEST)
                    df_time = df_data[TIMESTAMP_KEY].unique()
                    old_dict = {}
                    for name, group in data_now:
                        old_dict[name] = list(group[TAG_VALUE])
                    keys = []
                    for key in old_dict.keys():
                        keys.append(key)
                    if HGI_PRED in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(HGI_PRED)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(HGI_PRED)][DESCRIPTION].iloc[0]
                        if not df_min_max_data.empty:
                            min_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(HGI_PRED)][
                                    MIN_VALUE].iloc[
                                    0]
                            max_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(HGI_PRED)][
                                    MAX_VALUE].iloc[
                                    0]
                        dict1["data"] = old_dict[HGI_PRED]
                        dict1["unit"] = unit
                        dict1["description"] = description
                        dict1["min_data"] = min_data
                        dict1["max_data"] = max_data
                    elif HGI_PRED not in keys:
                        dict1["data"] = []
                        dict1["unit"] = None
                        dict1["description"] = None
                        dict1["min_data"] = None
                        dict1["max_data"] = None
                    if NORTH_HGI_PRED in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_PRED)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_PRED)][DESCRIPTION].iloc[
                            0]
                        if not df_min_max_data.empty:
                            min_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_PRED)][
                                    MIN_VALUE].iloc[0]
                            max_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_PRED)][
                                    MAX_VALUE].iloc[0]
                        dict2["data"] = old_dict[NORTH_HGI_PRED]
                        dict2["unit"] = unit
                        dict2["description"] = description
                        dict2["min_data"] = min_data
                        dict2["max_data"] = max_data
                    elif NORTH_HGI_PRED not in keys:
                        dict2["data"] = []
                        dict2["unit"] = None
                        dict2["description"] = None
                        dict2["min_data"] = None
                        dict2["max_data"] = None
                    if NORTH_HGI_ACTUAL in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_ACTUAL)][UNIT].iloc[0]
                        description = \
                            df_data[df_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_ACTUAL)][DESCRIPTION].iloc[
                                0]
                        if not df_min_max_data.empty:
                            min_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_ACTUAL)][
                                    MIN_VALUE].iloc[
                                    0]
                            max_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(NORTH_HGI_ACTUAL)][
                                    MAX_VALUE].iloc[
                                    0]
                        dict3["data"] = old_dict[NORTH_HGI_ACTUAL]
                        dict3["unit"] = unit
                        dict3["description"] = description
                        dict3["min_data"] = min_data
                        dict3["max_data"] = max_data
                    elif NORTH_HGI_ACTUAL not in keys:
                        dict3["data"] = []
                        dict3["unit"] = None
                        dict3["description"] = None
                        dict3["min_data"] = None
                        dict3["max_data"] = None
                    if SOUTH_HGI_PRED in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_PRED)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_PRED)][DESCRIPTION].iloc[
                            0]
                        if not df_min_max_data.empty:
                            min_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_PRED)][
                                    MIN_VALUE].iloc[
                                    0]
                            max_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_PRED)][
                                    MAX_VALUE].iloc[
                                    0]
                        dict4["data"] = old_dict[SOUTH_HGI_PRED]
                        dict4["unit"] = unit
                        dict4["description"] = description
                        dict4["min_data"] = min_data
                        dict4["max_data"] = max_data
                    elif SOUTH_HGI_PRED not in keys:
                        dict4["data"] = []
                        dict4["unit"] = None
                        dict4["description"] = None
                        dict4["min_data"] = None
                        dict4["max_data"] = None
                    if SOUTH_HGI_ACTUAL in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_ACTUAL)][UNIT].iloc[0]
                        description = \
                            df_data[df_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_ACTUAL)][DESCRIPTION].iloc[
                                0]
                        if not df_min_max_data.empty:
                            min_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_ACTUAL)][
                                    MIN_VALUE].iloc[
                                    0]
                            max_data = \
                                df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(SOUTH_HGI_ACTUAL)][
                                    MAX_VALUE].iloc[
                                    0]
                        dict5["data"] = old_dict[SOUTH_HGI_ACTUAL]
                        dict5["unit"] = unit
                        dict5["description"] = description
                        dict5["min_data"] = min_data
                        dict5["max_data"] = max_data
                    elif SOUTH_HGI_ACTUAL not in keys:
                        dict5["data"] = []
                        dict5["unit"] = None
                        dict5["description"] = None
                        dict5["min_data"] = None
                        dict5["max_data"] = None
                    if TAG_NAME_REQUEST in self.query_params.GET:
                        if query_params[TAG_NAME_REQUEST] in keys:
                            unit = \
                                df_data[df_data[TAG_NAME_REQUEST].str.contains(query_params[TAG_NAME_REQUEST])][
                                    UNIT].iloc[0]
                            description = \
                                df_data[df_data[TAG_NAME_REQUEST].str.contains(query_params[TAG_NAME_REQUEST])][
                                    DESCRIPTION].iloc[0]
                            if not df_min_max_data.empty:
                                min_tags = list(df_min_max_data[TAG_NAME_REQUEST])
                                if query_params[TAG_NAME_REQUEST] in min_tags:
                                    min_data = \
                                        df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST].str.contains(
                                                query_params[TAG_NAME_REQUEST])][
                                            MIN_VALUE].iloc[0]
                                    max_data = \
                                        df_min_max_data[
                                            df_min_max_data[TAG_NAME_REQUEST].str.contains(
                                                query_params[TAG_NAME_REQUEST])][
                                            MAX_VALUE].iloc[0]
                                else:
                                    min_data = None
                                    max_data = None

                            dict6["data"] = old_dict[query_params[TAG_NAME_REQUEST]]
                            dict6["unit"] = unit
                            dict6["description"] = description
                            dict6["min_data"] = min_data
                            dict6["max_data"] = max_data
                        else:
                            dict6["data"] = []
                            dict6["unit"] = None
                            dict6["description"] = None
                            dict6["min_data"] = None
                            dict6["max_data"] = None
                    else:
                        if DEBUG == 1:
                            print("sorry")
                    dict7["data"] = list(df_time)

                graph.append(final_dict)
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
def get_hgi_multi_line(request, equipment_name=None, module_name=None):
    """
    This function will return the hgi multi line graph data
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
                obj = HgiMultiLine(query_params, equipment_name, module_name)
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
