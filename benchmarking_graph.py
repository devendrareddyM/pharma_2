"""
File                :   benchmarking_graph.py

Description         :   This will return the graph data for the selected tag in the bench marking module

Author              :   LivNSense Technologies

Date Created        :   22-05-2020

Date Last modified :

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""

import pandas as pd
from django.views.decorators.csrf import *
from ApplicationInterface.Database.Queries import *
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation
from utilities.Api_Response import *
from utilities.Constants import *
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import *
from utilities.LoggerFile import *


class BenchMarkingGraph(_PostGreSqlConnection):
    """
    This class is responsible for getting the graph data and response for the selected bench marking module
    """

    def __init__(self, query_params=None):
        """
        This will call the parent class to validate the connection and initialize the values
        """
        super().__init__()
        self.query_params = query_params

    def get_benchmark_graph(self):
        """
        This will return the graph data for the bench marking module
        :return: Json response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            """
                This will return the graph data for the bench marking module
            """
            query_params = {
                TAG_NAME_REQUEST: self.query_params.GET[TAG_NAME_REQUEST],
                START_DATE_REQUEST: self.query_params.GET[START_DATE_REQUEST],
                END_DATE_REQUEST: self.query_params.GET[END_DATE_REQUEST]
            }
            if self.query_params:
                if START_DATE_REQUEST not in query_params or not query_params[START_DATE_REQUEST]:
                    self._psql_session.execute(
                        DETAILED_BENCHMARKING_GRAPH_NULL_START_DATE.format(
                            query_params[TAG_NAME_REQUEST],
                            query_params[END_DATE_REQUEST]))
                else:
                    self._psql_session.execute(
                        DETAILED_BENCHMARKING_GRAPH.format(
                            query_params[TAG_NAME_REQUEST],
                            query_params[START_DATE_REQUEST],
                            query_params[END_DATE_REQUEST]))
            df_data = pd.DataFrame(self._psql_session.fetchall())
            temp = {}
            graph = []

            if not df_data.empty:
                df_data = df_data.where(pd.notnull(df_data) == True, None)
                df_data.sort_values('input_ts', ascending=True, inplace=True)
                temp["y_axis"] = list(df_data[TAG_VALUE])
                temp["x_axis"] = list(df_data['input_ts'])
                temp["unit"] = df_data[UNIT].iloc[0]
                temp["description"] = df_data[DESCRIPTION].iloc[0]
                temp["min_data"] = None
                temp["max_data"] = None
                graph.append(temp)
            return JsonResponse(graph, safe=False)
        except AssertionError as e:
            log_error("Assertion error due to : %s" + str(e))
            return asert_res(e)

        except Exception as e:
            log_error("Exception occurred to : %s" + str(e))
            return json_InternalServerError

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_benchmarking_graph(request):
    """
    This function will return the graph data for the selected module
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
                obj = BenchMarkingGraph(query_params)
                return obj.get_benchmark_graph()

        log_debug(METHOD_NOT_ALLOWED)
        return json_MethodNotAllowed
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
        excMsg = "benchmarking_graph API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
