"""
File                :   configuration_router.py

Description         :   This will return the algorithm details and configuration update

Author              :   LivNSense Technologies

Date Created        :   12-06-2020

Date Last modified  :   12-06-2020

Copyright (C) 2020 LivNSense Technologies - All Rights Reserved

"""
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from ApplicationInterface.configurations import algorithms_list, configuration_update
from utilities.Constants import GET_REQUEST, PUT_REQUEST, MESSAGE_KEY, METHOD_NOT_ALLOWED
from utilities.Api_Response import HTTP_405_METHOD_NOT_ALLOWED


@csrf_exempt
def configuration_with_params(request, algorithm_name):
    if request.method == GET_REQUEST:
        return algorithms_list.get_algorithm_list(request, algorithm_name)
    else:
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)


@csrf_exempt
def configuration_without_params(request):
    if request.method == GET_REQUEST:
        return algorithms_list.get_algorithm_list(request)
    if request.method == PUT_REQUEST:
        return configuration_update.update_algorithm_params(request)
    else:
        return JsonResponse({MESSAGE_KEY: METHOD_NOT_ALLOWED},
                            status=HTTP_405_METHOD_NOT_ALLOWED)
