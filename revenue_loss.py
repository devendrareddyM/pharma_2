"""
File                :   revenue_loss.py

Description         :   This will return the revenue loss data for the overhead pdi module

Author              :   LivNSense Technologies

Date Created        :   07-01-2020

Date Last modified :    09-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
import yaml
import traceback
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    COKE_DRUM_VALUE, END_DATE_REQUEST, TAG_NAME_REQUEST, START_DATE_REQUEST, TIMESTAMP_KEY, MODULE_NAME, PDI_VALUE, \
    UNIT, TAG_VALUE, MODIFIED_TIME, RECORDS, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug
from ApplicationInterface.Database.Queries import DETAILED_REVENUE_GRAPH_NULL_START_DATE, DETAILED_REVENUE_GRAPH, \
    DETAILED_COST_GRAPH, \
    DETAILED_COST_GRAPH_NULL_START_DATE
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _CassandraConnection, _TokenValidation


class RevenueGraph(_PostGreSqlConnection, _CassandraConnection):
    """
    This class is responsible for getting the data and response for the revenue loss in the overhead pdi module
    """

    def __init__(self, query_params=None, equipment=None, module=None):
        """
        This will call the parent class to validate the connection and initialize the values
        :param module: module name will be provided
        :param equipment: equipment name will be provided
        """
        super().__init__()
        _CassandraConnection.__init__(self)

        self.equipment = equipment
        self.module = module
        self.query_params = query_params

    def get_values(self):
        """
        This will return the revenue loss data for the overhead pdi module value
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}

            if self.equipment == COKE_DRUM_VALUE:
                if self.query_params:
                    if START_DATE_REQUEST not in self.query_params or not self.query_params[START_DATE_REQUEST]:
                        self._psql_session.execute(
                            DETAILED_REVENUE_GRAPH_NULL_START_DATE.format(
                                self.module,
                                self.query_params[TAG_NAME_REQUEST],
                                self.query_params[END_DATE_REQUEST]))

                    else:
                        self._psql_session.execute(
                            DETAILED_REVENUE_GRAPH.format(
                                self.module,
                                self.query_params[TAG_NAME_REQUEST],
                                self.query_params[START_DATE_REQUEST],
                                self.query_params[END_DATE_REQUEST]))

                    df_data = pd.DataFrame(self._psql_session.fetchall())
                    graph = []
                    temp = {}
                    if not df_data.empty:
                        df_data = df_data.where(pd.notnull(df_data) == True, None)
                        df_data.sort_values(TIMESTAMP_KEY, ascending=True, inplace=True)
                        df_temp = df_data[df_data[MODULE_NAME] == PDI_VALUE]

                        if not df_temp.empty:
                            temp["unit"] = df_temp[UNIT].iloc[0]
                            temp["y_axis"] = list(df_temp[TAG_VALUE])
                            temp["x_axis"] = list(df_temp[TIMESTAMP_KEY])
                    else:
                        temp["unit"] = None
                        temp["y_axis"] = []
                        temp["x_axis"] = []

                    if START_DATE_REQUEST not in self.query_params or not self.query_params[START_DATE_REQUEST]:

                        self._psql_session.execute(
                            DETAILED_COST_GRAPH_NULL_START_DATE.format(
                                self.module,
                                self.query_params[END_DATE_REQUEST]))

                    else:
                        self._psql_session.execute(
                            DETAILED_COST_GRAPH.format(
                                self.module,
                                self.query_params[START_DATE_REQUEST],
                                self.query_params[END_DATE_REQUEST]))
                    df_cost_data = pd.DataFrame(self._psql_session.fetchall())
                    if not df_cost_data.empty:
                        df_cost_data = df_cost_data.where(pd.notnull(df_cost_data) == True, None)
                        df_cost_data.sort_values(MODIFIED_TIME, ascending=True, inplace=True)
                        # df_cost_temp = df_cost_data[df_cost_data[MODULE_NAME] == PDI_VALUE]
                        temp["updated_price"] = df_cost_data.to_dict(orient=RECORDS)
                    else:
                        temp["updated_price"] = []
                    graph.append(temp)

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
def get_revenue_graph(request, equipment_name=None, module_name=None):
    """
    This function will return the revenue loss data for the overhead pdi module
    :param module_name: module name
    :param equipment_name: equipment name
    :param request: request django object
    :return: json response
    """

    query_params, obj = None, None
    try:

        query_params = {
            TAG_NAME_REQUEST: request.GET[TAG_NAME_REQUEST],
            START_DATE_REQUEST: request.GET[START_DATE_REQUEST],
            END_DATE_REQUEST: request.GET[END_DATE_REQUEST]
        }

    except:
        pass

    try:
        if request.method == GET_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = RevenueGraph(query_params, equipment_name, module_name)
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
