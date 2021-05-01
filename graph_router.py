"""
File                :   graph_router.py

Description         :   This will return the module level graphs data for all the modules

Author              :   LivNSense Technologies

Date Created        :   16-04-2020

Date Last modified  :   16-04-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
import jwt
from django.http import JsonResponse

from ApplicationInterface.Database.Utility import _TokenValidation
from ApplicationInterface.graphs import pdi_graph, furnace_graph, outage_graph, hgi_graph, furnace_spall_graph
from utilities.Api_Response import HTTP_401_UNAUTHORIZED
from utilities.Constants import COKE_DRUM_VALUE, PDI_VALUE, FURNACE_VALUE, OUTAGE_VALUE, PASS_A_VALUE, PASS_B_VALUE, \
    HGI_VALUE, HTTP_AUTHORIZATION_TOKEN, MESSAGE_KEY, SPALL_PASS_A_VALUE, SPALL_PASS_B_VALUE
from utilities.HashingManagement import HashingSalting


def get_graph_values(request, equipment_name, module_name):
    """
    This will return the graph data for the selected module
    :return: Json Response
    """
    try:
        loggedin_user_details = _TokenValidation.validate_token(request)
        if loggedin_user_details:
            if equipment_name == COKE_DRUM_VALUE and module_name == PDI_VALUE:
                return JsonResponse(pdi_graph.get_pdi_graph(request, equipment_name, module_name), safe=False)
            if equipment_name == FURNACE_VALUE and (module_name == PASS_A_VALUE or module_name == PASS_B_VALUE):
                return JsonResponse(furnace_graph.get_furnace_graph(request, equipment_name, module_name), safe=False)
            if equipment_name == FURNACE_VALUE and (module_name == SPALL_PASS_A_VALUE or module_name == SPALL_PASS_B_VALUE):
                return JsonResponse(furnace_spall_graph.get_furnace_spall_graph(request, equipment_name, module_name), safe=False)
            if equipment_name == COKE_DRUM_VALUE and module_name == OUTAGE_VALUE:
                return JsonResponse(outage_graph.get_outage_graph(request, equipment_name, module_name), safe=False)
            if equipment_name == COKE_DRUM_VALUE and module_name == HGI_VALUE:
                return JsonResponse(hgi_graph.get_hgi_graph(request, equipment_name, module_name), safe=False)
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
