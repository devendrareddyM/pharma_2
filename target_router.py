"""
File                :   configuration_router.py

Description         :   This will return the algorithm list and configuration update

Author              :   LivNSense Technologies

Date Created        :   11-11-2019

Date Last modified  :   11-11-2019

Copyright (C) 2018 LivNSense Technologies - All Rights Reserved

"""
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from ApplicationInterface.HgiModule import hgi_target_list, hgi_target_update
from ApplicationInterface.OutageModule import target_list, target_update
from ApplicationInterface.TmtModule import target_eor_list, target_eor_update
from utilities.Constants import GET_REQUEST, OUTAGE_VALUE, HGI_VALUE, PUT_REQUEST, FURNACE_A_VALUE, FURNACE_B_VALUE


@csrf_exempt
def get_target_list_values(request, algorithm_name):
    """
        This will return the graph data for the selected module
        :return: Json Response
        """
    if request.method == GET_REQUEST and algorithm_name == OUTAGE_VALUE:
        return target_list.get_target_list(request, algorithm_name)
    if request.method == GET_REQUEST and algorithm_name == HGI_VALUE:
        return hgi_target_list.get_hgi_list(request, algorithm_name)
    if request.method == PUT_REQUEST and algorithm_name == OUTAGE_VALUE:
        return target_update.update_target_params(request, algorithm_name)
    if request.method == PUT_REQUEST and algorithm_name == HGI_VALUE:
        return hgi_target_update.update_hgi_target(request, algorithm_name)
    if request.method == GET_REQUEST and algorithm_name == FURNACE_A_VALUE:
        return target_eor_list.get_target_eor_list(request, algorithm_name)
    if request.method == GET_REQUEST and algorithm_name == FURNACE_B_VALUE:
        return target_eor_list.get_target_eor_list(request, algorithm_name)
    if request.method == PUT_REQUEST and algorithm_name == FURNACE_A_VALUE:
        return target_eor_update.update_target_eor_params(request, algorithm_name)
    if request.method == PUT_REQUEST and algorithm_name == FURNACE_B_VALUE:
        return target_eor_update.update_target_eor_params(request, algorithm_name)
    else:
        return JsonResponse("This equipment is not registered with us!", safe=False)
