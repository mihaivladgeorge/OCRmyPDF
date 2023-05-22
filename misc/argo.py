#!/usr/bin/env python3

import os
import requests

ARGO_URL = "http://argo-workflows-server.default.svc.cluster.local:2746/api/v1/workflows/?listOptions.labelSelector=workflows.argoproj.io/phase%20in%20(Running)"
ARGO_SUBMIT_URL = "http://argo-workflows-server.default.svc.cluster.local:2746/api/v1/workflows/default/submit"
MAX_CPU = int(os.getenv('MAX_CPU'))


def get_argo_workflow_count():
    res = requests.get(ARGO_URL)
    if res.status_code == 200:
        workflows = res.json().get("items", [])
        if workflows is not None:
            workflow_count = len(workflows)
            print(f"The number of workflows currently running is {workflow_count}")
            return workflow_count
        else:
            print("No workflows currently running.")
            return None
    else:
        print("Argo Workflow API Error...")
        return MAX_CPU


def start_workflow(input_id):
    data = {
        "namespace": "default",
        "resourceKind": "WorkflowTemplate",
        "resourceName": "ocr-worker",
        "submitOptions": {
            "entryPoint": "ocr",
            "generateName": "ocr-worker-",
            "parameters": [f"input={input_id}"]
        }
    }

    res = requests.post(ARGO_SUBMIT_URL, json=data)

    if res.status_code == 200:
        print(f"Workflow {input_id} started!")
    else:
        print(f"Error starting workflow: {res.text}")
