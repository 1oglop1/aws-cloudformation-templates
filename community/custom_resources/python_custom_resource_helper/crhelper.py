# -*- coding: utf-8 -*-
#
# crhelper.py
#
# Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##################################################################################################
"""
This module handles communication with AWS Cloudformation during the
creation of lambda backed Custom Resource.

Adopted from
https://github.com/awslabs/aws-cloudformation-templates/blob/master/community/custom_resources/python_custom_resource_helper/crhelper.py
version: https://github.com/awslabs/aws-cloudformation-templates/commit/39432800827b93bda16dfb26c2e300d8a747f6d3

Usage:

In import this module lambda_handler.py, create functions with required output.
# lambda_handler.py

from crhelper import cfn_handler

def create(event, content):
   \"""Create resource.\"""
    physical_resource_id = context.log_stream_name
    return physical_resource_id, {"Some": "Data"}

def update(event, context)
    \"""Update resource.\"""
    physical_resource_id = event['PhysicalResourceId']
    return physical_resource_id, {}

def delete(event):
    \"""Delete resource.\"""
    return event['PhysicalResourceId']

def lambda_handler(event, context):
    cfn_handler(event, context, create, update, delete)

"""

import json
import logging
import threading

from botocore.vendored import requests

LOGGER = logging.getLogger(__name__)
LOGGER.addHandler(logging.NullHandler())

SUCCESS = "SUCCESS"
FAILED = "FAILED"


def send_cfn(event, context, response_status, response_data, reason=None, physical_resource_id=None):
    """
    Send a resource manipulation status response to CloudFormation

    Modified from:
    https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-lambda-function-code.html
    """

    default_reason = (
        f"See the details in CloudWatch Log group {context.log_group_name} "
        f"Stream: {context.log_stream_name}"
    )

    response_body = json.dumps(
        {
            "Status": response_status,
            "Reason": str(reason) + f".. {default_reason}" if reason else default_reason,
            "PhysicalResourceId": physical_resource_id or context.log_stream_name,
            "StackId": event["StackId"],
            "RequestId": event["RequestId"],
            "LogicalResourceId": event["LogicalResourceId"],
            "Data": response_data,
        }
    )

    LOGGER.info(f"ResponseURL: {event['ResponseURL']}", )
    LOGGER.info(f"ResponseBody: {response_body}")

    headers = {"Content-Type": "", "Content-Length": str(len(response_body))}

    response = requests.put(event["ResponseURL"], data=response_body, headers=headers)

    try:
        response.raise_for_status()
        LOGGER.info(f"Status code: {response.reason}")
    except requests.HTTPError:
        LOGGER.exception(f"Failed to send CFN response. {response.text}")
        raise


def lambda_timeout(event, context):
    """Send error to CFN if Lambda runs ouf of time."""
    msg = "Execution is about to time out, sending failure message"
    LOGGER.error(msg)
    send_cfn(event, context, FAILED, {}, reason=msg)
    raise Exception(msg)


def cfn_handler(event, context, create, update, delete):
    """
    Handle CFN events.

    This function executes methods for custom resource creation and send response to cloudformation API.

    Parameters
    ----------
    event
        AWS Lambda event (request from CFN)
    context
        AWS Lambda context
    create: function
        Create(request) custom resource function.
    update
        Update custom resource function.
    delete
        Delete custom resource function.

    """

    # Set timer to expire slightly sooner so we have time to notify CFN.
    timeout_timer = threading.Timer(
        (context.get_remaining_time_in_millis() / 1000.00) - 0.5,
        lambda_timeout,
        args=[event, context],
    )
    timeout_timer.start()

    physical_resource_id = None
    response_data = {}
    try:
        # Execute custom resource handlers
        LOGGER.info("Received a {} Request".format(event["RequestType"]))
        if event["RequestType"] == "Create":
            physical_resource_id, response_data = execute_handler(event, context, create)
        elif event["RequestType"] == "Update":
            physical_resource_id, response_data = execute_handler(event, context, update)
        elif event["RequestType"] == "Delete":
            physical_resource_id, response_data = execute_handler(event, context, delete, delete=True)
        else:
            send_cfn(event, context, FAILED, None, f"Unsupported RequestType: {event['RequestType']}")

        send_cfn(event, context, SUCCESS, response_data, physical_resource_id=physical_resource_id)

    # Safety switch - Catch any exceptions, log the stacktrace, send a failure back to
    # CloudFormation and then raise an exception
    except Exception as exc:
        LOGGER.error(exc, exc_info=True)
        send_cfn(
            event,
            context,
            FAILED,
            None,
            reason=f"{exc.__class__.__name__}: {exc}",
        )
        raise
    finally:
        # Stop the before next lambda invocation.
        timeout_timer.cancel()


def execute_handler(event, context, handler, delete=False):
    """
    Execute handlers: Create, Update and check their response.

    Parameters
    ----------
    event
        AWS Lambda event.
    context
        AWS Lambda context.
    handler: function
        Functions Create or Update
    delete: bool
        If handler is Delete - default False.
    Returns
    -------
    tuple
        Verified response.

    """
    response = handler(event, context)
    if delete:
        response = (response, {})
    if not isinstance(response, tuple) or len(response) != 2:
        raise TypeError(f"Error during {handler.__name__} does not return tuple(PhysicalResourceId, Data).")
    if not isinstance(response[0], str):
        raise ValueError(f"PhysicalResourceId is not string, but {type(response[0])}.")
    if not isinstance(response[1], dict):
        raise TypeError(f"Data is not dictionary, but {type(response[1])}.")
    return response
