"""
File                :   outage_multiline.py

Description         :   This will display the multiline graph for the outage module

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
from ApplicationInterface.Database.Configuration import LIST_OF_OUTAGE_MULTILINE_TAGS, LIST_OF_OUTAGE_PRIMARY_TAGS
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    COKE_DRUM_VALUE, OUTAGE_VALUE, END_DATE_REQUEST, START_DATE_REQUEST, PRIMARY_TAG, COKE_HEIGHT_TAG, FOAM_HEIGHT_TAG, \
    OUTAGE_TREND_TAG, TIMESTAMP_KEY, TAG_NAME_REQUEST, DRUM_ONLINE, TAG_VALUE, UNIT, DESCRIPTION, MIN_VALUE, \
    MAX_VALUE, CURRENT_OUTAGE, FOAM_HEIGHT, COKE_HEIGHT, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import OUTAGE_MULTILINE, OUTAGE_MIN_MAX_DATA
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all


class OutageMultiLine(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the outage multiline graph
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
        This will return the outage multiline graph data
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            graph = []
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
            dict5 = {"data": []}
            if self.equipment == COKE_DRUM_VALUE and self.module == OUTAGE_VALUE:
                query_params = {
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }

                if PRIMARY_TAG not in self.query_params.GET:
                    """
                    This will return the outage multiline graph data without the primary tag
                    """
                    multiline_tags = tuple(LIST_OF_OUTAGE_MULTILINE_TAGS)
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST]:
                        multi_line = django_search_query_all(OUTAGE_MULTILINE.format(
                            self.module,
                            multiline_tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST]))
                        df_data = pd.DataFrame(multi_line)

                        min_max = django_search_query_all(OUTAGE_MIN_MAX_DATA.format(
                            self.module,
                            multiline_tags))

                elif PRIMARY_TAG in self.query_params.GET:
                    query_params[PRIMARY_TAG] = self.query_params.GET[PRIMARY_TAG]
                    if query_params[START_DATE_REQUEST] and query_params[END_DATE_REQUEST] and query_params[
                        PRIMARY_TAG]:
                        """
                        This will return the outage multiline graph data with the primary tag
                        """
                        LIST_OF_OUTAGE_MULTILINE_TAGS.append(query_params["primary_tag"])
                        tags = tuple(LIST_OF_OUTAGE_MULTILINE_TAGS)
                        multi_line = django_search_query_all(OUTAGE_MULTILINE.format(
                            self.module,
                            tags,
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST]))
                        df_data = pd.DataFrame(multi_line)
                        min_max = django_search_query_all(OUTAGE_MIN_MAX_DATA.format(
                            self.module,
                            tags))
                        LIST_OF_OUTAGE_MULTILINE_TAGS.pop()

                if PRIMARY_TAG in self.query_params.GET:
                    if not df_data.empty:
                        query_params[PRIMARY_TAG] = self.query_params.GET[PRIMARY_TAG]
                        final_dict = {
                            "Coke Height": dict1,
                            "Foam Height": dict2,
                            "Current Outage": dict3,
                            query_params[PRIMARY_TAG]: dict4,
                            "x-axis": dict5,
                            "online-drum": []
                        }
                    else:
                        final_dict = {
                            "Coke Height": empty_dict,
                            "Foam Height": empty_dict,
                            "Current Outage": empty_dict,
                            query_params[PRIMARY_TAG]: empty_dict,
                            "x-axis": dict5,
                            "online-drum": []
                        }
                else:
                    if not df_data.empty:
                        final_dict = {
                            "Coke Height": dict1,
                            "Foam Height": dict2,
                            "Current Outage": dict3,
                            "x-axis": dict5,
                            "tags_list": LIST_OF_OUTAGE_PRIMARY_TAGS,
                            "online-drum": []
                        }
                    else:
                        final_dict = {
                            "Coke Height": empty_dict,
                            "Foam Height": empty_dict,
                            "Current Outage": empty_dict,
                            "x-axis": dict5,
                            "tags_list": LIST_OF_OUTAGE_PRIMARY_TAGS,
                            "online-drum": []
                        }
                df_min_max_data = pd.DataFrame(min_max)

                if not df_data.empty:
                    df_data = df_data.where(pd.notnull(df_data) == True, None)
                    df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                    data_now = df_data.groupby(TAG_NAME_REQUEST)
                    df_time = df_data[TIMESTAMP_KEY].unique()
                    data_online_coke = data_now.get_group(COKE_HEIGHT_TAG)
                    data_online_foam = data_now.get_group(FOAM_HEIGHT_TAG)
                    data_online_current = data_now.get_group(FOAM_HEIGHT_TAG)
                    if not data_online_coke.empty:
                        final_dict["online-drum"] = list(data_online_coke[DRUM_ONLINE])
                    elif not data_online_foam.empty:
                        final_dict["online-drum"] = list(data_online_foam[DRUM_ONLINE])
                    elif not data_online_current.empty:
                        final_dict["online-drum"] = list(data_online_current[DRUM_ONLINE])
                    old_dict = {}
                    for name, group in data_now:
                        old_dict[name] = list(group[TAG_VALUE])
                    keys = []
                    for key in old_dict.keys():
                        keys.append(key)
                    if COKE_HEIGHT_TAG in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(COKE_HEIGHT)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(COKE_HEIGHT)][DESCRIPTION].iloc[0]
                        min_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(COKE_HEIGHT)][
                                MIN_VALUE].iloc[
                                0]
                        max_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(COKE_HEIGHT)][
                                MAX_VALUE].iloc[
                                0]
                        dict1["data"] = old_dict[COKE_HEIGHT_TAG]
                        dict1["unit"] = unit
                        dict1["description"] = description
                        dict1["min_data"] = min_data
                        dict1["max_data"] = max_data
                    elif COKE_HEIGHT_TAG not in keys:
                        dict1["data"] = None
                        dict1["unit"] = None
                        dict1["description"] = None
                        dict1["min_data"] = None
                        dict1["max_data"] = None
                    if FOAM_HEIGHT_TAG in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(FOAM_HEIGHT)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(FOAM_HEIGHT)][DESCRIPTION].iloc[0]
                        min_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(FOAM_HEIGHT)][
                                MIN_VALUE].iloc[0]
                        max_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(FOAM_HEIGHT)][
                                MAX_VALUE].iloc[0]
                        dict2["data"] = old_dict[FOAM_HEIGHT_TAG]
                        dict2["unit"] = unit
                        dict2["description"] = description
                        dict2["min_data"] = min_data
                        dict2["max_data"] = max_data
                    elif FOAM_HEIGHT_TAG not in keys:
                        dict2["data"] = None
                        dict2["unit"] = None
                        dict2["description"] = None
                        dict2["min_data"] = None
                        dict2["max_data"] = None
                    if OUTAGE_TREND_TAG in keys:
                        unit = df_data[df_data[TAG_NAME_REQUEST].str.contains(CURRENT_OUTAGE)][UNIT].iloc[0]
                        description = df_data[df_data[TAG_NAME_REQUEST].str.contains(CURRENT_OUTAGE)][DESCRIPTION].iloc[
                            0]
                        min_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(CURRENT_OUTAGE)][
                                MIN_VALUE].iloc[
                                0]
                        max_data = \
                            df_min_max_data[df_min_max_data[TAG_NAME_REQUEST].str.contains(CURRENT_OUTAGE)][
                                MAX_VALUE].iloc[
                                0]
                        dict3["data"] = old_dict[OUTAGE_TREND_TAG]
                        dict3["unit"] = unit
                        dict3["description"] = description
                        dict3["min_data"] = min_data
                        dict3["max_data"] = max_data
                    elif OUTAGE_TREND_TAG not in keys:
                        dict3["data"] = None
                        dict3["unit"] = None
                        dict3["description"] = None
                        dict3["min_data"] = None
                        dict3["max_data"] = None

                    if PRIMARY_TAG in self.query_params.GET:
                        if query_params[PRIMARY_TAG] in keys:
                            data_online_primary = data_now.get_group(query_params[PRIMARY_TAG])
                            if not data_online_primary.empty:
                                final_dict["online-drum"] = list(data_online_primary[DRUM_ONLINE])
                            else:
                                print("sorry")
                            unit = \
                                df_data[df_data[TAG_NAME_REQUEST].str.contains(query_params[PRIMARY_TAG])][UNIT].iloc[0]
                            description = \
                                df_data[df_data[TAG_NAME_REQUEST].str.contains(query_params[PRIMARY_TAG])][
                                    DESCRIPTION].iloc[0]
                            min_data = \
                                df_min_max_data[
                                    df_min_max_data[TAG_NAME_REQUEST].str.contains(query_params[PRIMARY_TAG])][
                                    MIN_VALUE].iloc[0]
                            max_data = \
                                df_min_max_data[
                                    df_min_max_data[TAG_NAME_REQUEST].str.contains(query_params[PRIMARY_TAG])][
                                    MAX_VALUE].iloc[0]
                            dict4["data"] = old_dict[query_params[PRIMARY_TAG]]
                            dict4["unit"] = unit
                            dict4["description"] = description
                            dict4["min_data"] = min_data
                            dict4["max_data"] = max_data
                        else:
                            dict4["data"] = None
                            dict4["unit"] = None
                            dict4["description"] = None
                            dict4["min_data"] = None
                            dict4["max_data"] = None
                    else:
                        print("sorry")
                    dict5["data"] = list(df_time)

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
def get_outage_multiline(request, equipment_name=None, module_name=None):
    """
    This function will return the outage multiline graph data
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
                obj = OutageMultiLine(query_params, equipment_name, module_name)
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
        excMsg = "get_outage_multi_line_graph_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
