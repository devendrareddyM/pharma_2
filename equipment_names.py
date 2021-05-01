"""
File                :   equipment_names.py

Description         :   This will return all the equipment names and module names

Author              :   LivNSense Technologies

Date Created        :   07-01-2020

Date Last modified :    07-01-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import traceback

import jwt
import pandas as pd
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from utilities.Constants import GET_REQUEST, METHOD_NOT_ALLOWED, MESSAGE_KEY, STATUS_KEY, EXCEPTION_CAUSE, DB_ERROR, \
    RECORDS, EQUIPMENT_NAME, MODULE_NAME, ID, EQUIPMENT, HTTP_AUTHORIZATION_TOKEN
from utilities.HashingManagement import HashingSalting
from utilities.Http_Request import error_instance
from utilities.LoggerFile import log_error, log_debug, log_info, logger
from ApplicationInterface.Database.Queries import MASTER_TABLE_QUERY
from utilities.Api_Response import HTTP_500_INTERNAL_SERVER_ERROR, HTTP_405_METHOD_NOT_ALLOWED, HTTP_401_UNAUTHORIZED
from ApplicationInterface.Database.Utility import _PostGreSqlConnection, _TokenValidation


class EquipmentNames(_PostGreSqlConnection):
    """
    This class is responsible for getting the names for all the equipment and module
    """

    def __init__(self, loggedin_userid_details):
        """
        This will call the parent class to validate the connection and request payload
        :param username: this will control the query a particular user
        :param loggedin_userid_details : loggedin userid details
        """
        super().__init__()
        self.loggedin_userid_details = loggedin_userid_details

    def get_names_values(self):
        """
        This will return the names for all the equipment and module
        :return: Json Response
        """
        try:
            assert self._db_connection, {
                STATUS_KEY: HTTP_500_INTERNAL_SERVER_ERROR,
                MESSAGE_KEY: DB_ERROR}
            self._psql_session.execute(MASTER_TABLE_QUERY)
            df = pd.DataFrame(self._psql_session.fetchall())
            df = pd.DataFrame(df, index=[0, 1, 2, 3, 5, 4, 6])
            # df.loc[df['module_name'] == 'H3901A: Pass 3 & 4(Spall)', 'id'] = 5
            # df.loc[df['module_name'] == 'H3901A: Pass 3 & 4 (Spall)', 'id'] = 5
            # df.loc[df['module_name'] == 'H3901B: Pass 1 & 2', 'id'] = 6
            # df.sort_values(by=['id'], inplace=True)
            hgi = self.loggedin_userid_details['permissions']['HGI']
            over_head_pdi = self.loggedin_userid_details['permissions']['PDI']
            outage = self.loggedin_userid_details['permissions']['Outage']
            furnace_a = self.loggedin_userid_details['permissions']['Furnace: H3901A']
            furnace_a_spall = self.loggedin_userid_details['permissions']['Furnace: H3901A (Spall)']
            furnace_b = self.loggedin_userid_details['permissions']['Furnace: H3901B']
            furnace_b_spall = self.loggedin_userid_details['permissions']['Furnace: H3901B (Spall)']
            left_equipment = self.loggedin_userid_details['permissions']['Left Pane']
            if 'Equipment' not in left_equipment:
                return JsonResponse([],
                                    safe=False)
            if 'Left Pane : Module Access' not in hgi:
                df.drop(df.loc[df['module_name'] == 'HGI'].index, inplace=True)
            if 'Left Pane : Module Access' not in furnace_a:
                df.drop(df.loc[df['module_name'] == 'H3901A: Pass 3 & 4'].index, inplace=True)
            if 'Left Pane : Module Access' not in furnace_a_spall:
                df.drop(df.loc[df['module_name'] == 'H3901A: Pass 3 & 4 (Spall)'].index, inplace=True)
            if 'Left Pane : Module Access' not in furnace_b_spall:
                df.drop(df.loc[df['module_name'] == 'H3901B: Pass 1 & 2 (Spall)'].index, inplace=True)
            if 'Left Pane : Module Access' not in furnace_b:
                df.drop(df.loc[df['module_name'] == 'H3901B: Pass 1 & 2'].index, inplace=True)
            if 'Left Pane : Module Access' not in outage:
                df.drop(df.loc[df['module_name'] == 'Outage'].index, inplace=True)
            if 'Left Pane : Module Access' not in over_head_pdi:
                df.drop(df.loc[df['module_name'] == 'Overhead PDI'].index, inplace=True)

            if df.shape[0]:
                unit_val = {}
                final_val = []

                equipment_name = df[EQUIPMENT_NAME].unique()
                equipment_val = []
                for equipment in equipment_name:
                    module_val = {"equipment_name": equipment, "modules": df[[MODULE_NAME, ID]][
                        (df[EQUIPMENT_NAME] == equipment)].to_dict(
                        orient=RECORDS)}
                    equipment_val.append(module_val)

                unit_val["unit_name"] = EQUIPMENT
                unit_val["equipments"] = equipment_val
                final_val.append(unit_val)
                return JsonResponse(final_val,
                                    safe=False)
            return JsonResponse([],
                                safe=False)

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
def get_equipment_names(request):
    """
    This function will return the names for all the equipment and module
    :param request: request django object
    :return: json response
    """
    obj = None

    try:
        if request.method == GET_REQUEST:

            loggedin_user_details = _TokenValidation.validate_token(request)
            if loggedin_user_details:
                obj = EquipmentNames(loggedin_user_details)
                return obj.get_names_values()
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
