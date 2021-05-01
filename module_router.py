"""
File                :   module_router.py

Description         :   This will return the module level data for all the modules

Author              :   LivNSense Technologies

Date Created        :   16-04-2020

Date Last modified  :   16-04-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
from django.http import JsonResponse

from ApplicationInterface.Database.Utility import _TokenValidation
from ApplicationInterface.ModuleOverview import pdi_overview, furnace_overview, outage_overview, hgi_overview, \
    furnace_spall
from utilities.Api_Response import HTTP_401_UNAUTHORIZED
from utilities.Constants import COKE_DRUM_VALUE, PDI_VALUE, FURNACE_VALUE, OUTAGE_VALUE, PASS_A_VALUE, PASS_B_VALUE, \
    HGI_VALUE, HTTP_AUTHORIZATION_TOKEN, MESSAGE_KEY, SPALL_PASS_A_VALUE, SPALL_PASS_B_VALUE
from utilities.HashingManagement import HashingSalting


def get_module_values(request, equipment_name, module_name):
    """
    This will return the graph data for the selected module
    :return: Json Response
    """
    try:
        loggedin_user_details = _TokenValidation.validate_token(request)
        if loggedin_user_details:
            if equipment_name == COKE_DRUM_VALUE and module_name == PDI_VALUE:
                return JsonResponse(pdi_overview.get_pdi_overview(request, equipment_name, module_name), safe=False)
            if equipment_name == FURNACE_VALUE and (module_name == PASS_A_VALUE or module_name == PASS_B_VALUE):
                return JsonResponse(furnace_overview.get_furnace_overview(request, equipment_name, module_name), safe=False)
            if equipment_name == FURNACE_VALUE and (module_name == SPALL_PASS_A_VALUE or module_name == SPALL_PASS_B_VALUE):
                return JsonResponse(furnace_spall.get_furnace_spall_overview(request, equipment_name, module_name), safe=False)
            if equipment_name == COKE_DRUM_VALUE and module_name == OUTAGE_VALUE:
                return JsonResponse(outage_overview.get_outage_overview(request, equipment_name, module_name), safe=False)
            if equipment_name == COKE_DRUM_VALUE and module_name == HGI_VALUE:
                return hgi_overview.get_hgi_overview(request, equipment_name, module_name)
            else:
                return JsonResponse("This equipment is not registered with us!", safe=False)
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
