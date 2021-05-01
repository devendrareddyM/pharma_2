"""
File                :   hgi_graph.py

Description         :   This will return the graph data for the selected Overhead PDI module

Author              :   LivNSense Technologies

Date Created        :   11-05-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.Database.Queries import MIN_MAX_DATA, DETAILED_HGI_NORTH_GRAPH_NULL_START_DATE, \
    DETAILED_HGI_GRAPH
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation, django_search_query_all
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, json
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, TAG_NAME_REQUEST, START_DATE_REQUEST, TIMESTAMP_KEY, TAG_VALUE, UNIT, \
    DESCRIPTION, MIN_VALUE, MAX_VALUE, HGI_VALUE, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug


class HgiGraph(_PostGreSqlConnection):
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

    def get_hgi_graph(self):
        """
        This will return the graph data for the hgi module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.equipment == COKE_DRUM_VALUE and self.module == HGI_VALUE:
                """
                This will return the graph data for the selected hgi module
                """
                query_params = {
                    TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
                    START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                    END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
                }
                graph_data = []
                if self.query_params:
                    if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST]:
                        graph_data = django_search_query_all(
                            DETAILED_HGI_NORTH_GRAPH_NULL_START_DATE.format(
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[END_DATE_REQUEST]))
                    else:
                        graph_data = django_search_query_all(
                            DETAILED_HGI_GRAPH.format(
                                self.module,
                                query_params[TAG_NAME_REQUEST],
                                query_params[START_DATE_REQUEST],
                                query_params[END_DATE_REQUEST]))

                df_data = pd.DataFrame(graph_data)
                """
                Getting Min and Max Values
                """
                min_max = django_search_query_all(MIN_MAX_DATA.format(self.module,
                                                                      query_params[TAG_NAME_REQUEST]))
                min_data = pd.DataFrame(min_max)
                temp = {}
                graph = []

                if not df_data.empty:
                    df_data = df_data.where(pd.notnull(df_data) == True, None)
                    df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                    temp["y_axis"] = list(df_data[TAG_VALUE])
                    temp["x_axis"] = list(df_data[TIMESTAMP_KEY])
                    temp["unit"] = df_data[UNIT].iloc[0]
                    temp["description"] = df_data[DESCRIPTION].iloc[0]
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
            return json

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_hgi_graph(request, equipment_name=None, module_name=None):
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
            obj = HgiGraph(query_params, equipment_name, module_name)
            return obj.get_hgi_graph()

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
