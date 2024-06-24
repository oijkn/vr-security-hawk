import base64
import json
import logging
import os

# Set up logging configuration
logging.basicConfig(force=True, level=os.getenv("LOG_LEVEL", "INFO"))


def flatten_json(y):
    """
    Flattens a nested JSON object.
    """
    out = {}

    def flatten(x, name=""):
        if isinstance(x, dict):
            for key in x:
                flatten(x[key], f"{name}{key}_")
        elif isinstance(x, list):
            for i, item in enumerate(x):
                flatten(item, f"{name}{i}_")
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    Processes input event records, flattens JSON data, and extracts specific fields.
    """
    logging.info("Loading function")
    logging.info(event)

    output = []

    wanted_keys = [
        "AwsAccountId", "CreatedAt", "Description", "Resources_0_Id", "Resources_0_Type",
        "Resources_0_Region", "FindingProviderFields_Severity_Label", "Title", "UpdatedAt",
        "Compliance_Status", "LastObservedAt", "Workflow_Status", "FirstObservedAt"
    ]

    for record in event.get("records", []):
        payload = base64.b64decode(record["data"]).decode("utf-8")
        data = json.loads(payload)

        finding = data["detail"]["findings"][0]
        finding_id = finding["Id"].split("/")[-1]

        flat_dict = flatten_json(finding)
        logging.info(flat_dict)

        trimmed_dict = {k: v for k, v in flat_dict.items() if k in wanted_keys}

        # Renaming keys
        key_mapping = {
            "Resources_0_Id": "Resource_Id",
            "Resources_0_Region": "Region",
            "Resources_0_Type": "Resource_Type",
            "FindingProviderFields_Severity_Label": "Severity_Label"
        }

        for old_key, new_key in key_mapping.items():
            if old_key in trimmed_dict:
                trimmed_dict[new_key] = trimmed_dict.pop(old_key)

        trimmed_dict["Id"] = finding_id

        trimmed_str = json.dumps(trimmed_dict) + "\n"  # Ensure newline for data parsing

        output_record = {
            "recordId": record["recordId"],
            "result": "Ok",
            "data": base64.b64encode(trimmed_str.encode("utf-8")).decode("utf-8")
        }
        output.append(output_record)

    logging.info(f"Successfully processed {len(event['records'])} records.")
    return {"records": output}
