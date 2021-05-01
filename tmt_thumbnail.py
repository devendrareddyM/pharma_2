import numpy as np
import pandas as pd
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import *
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import *
from utilities.LoggerFile import *
from ApplicationInterface.Database.Queries import *
from utilities.Api_Response import *
from ApplicationInterface.Database.Utility import *
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class TMTThumbnail(_PostGreSqlConnection):
    """
    This class is responsible for getting the data and response for the critical trends thumbnail of the tmt module
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
        This will return the critical trends thumbnail value for the tmt module
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            if self.equipment == FURNACE_VALUE:
                try:
                    assert self._db_connection, {
                        STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                        MESSAGE_KEY: DB_ERROR}
                    query_params = {
                        PASS: self.query_params.GET[PASS],
                        IS_TRENDS: self.query_params.GET[IS_TRENDS]
                    }
                    if self.query_params:
                        if IS_TRENDS in query_params and query_params[IS_TRENDS] == TRUE_VALUE:
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

                            self._psql_session.execute(
                                LAST_10_LIMITING_TI_THUMBNAIL.format(
                                    query_params[PASS]
                                ))
                            df_data = pd.DataFrame(self._psql_session.fetchall())

                            dict_data = []

                            if not df_data.empty:
                                df_data=df_data.dropna()
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
                                dict_data.append(final_dict)

                                return JsonResponse(dict_data, safe=False)
                            else:
                                return JsonResponse([], safe=False)
                        else:
                            return JsonResponse([], safe=False)
                except Exception as e:
                    log_error("Exception due to : %s" + str(e))

        except Exception as e:
            log_error("Exception due to : %s" + str(e))

    def __del__(self):
        if self._psql_session:
            self._psql_session.close()


@csrf_exempt
def get_tmt_thumbnail(request, equipment_name=None, module_name=None):
    """
    This function will return the critical trends thumbnail value for the tmt module
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
                obj = TMTThumbnail(query_params, equipment_name, module_name)
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
        excMsg = "get_tmt_thumbnail_graph_data API : " + str(error_instance(e))
        return excMsg
    finally:
        if obj:
            del obj
