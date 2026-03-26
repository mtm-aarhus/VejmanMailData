
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement
from datetime import datetime, timedelta
import pytz
import hashlib
import json
import requests
from azure.cosmos import CosmosClient
from azure.cosmos.exceptions import CosmosResourceNotFoundError
import os

orchestrator_connection = OrchestratorConnection("VejmanData", os.getenv('OpenOrchestratorSQL'), os.getenv('OpenOrchestratorKey'), None)

VEJMAN_TIMEZONE = pytz.timezone("Europe/Copenhagen")
ALLOWED_INITIALS = {"MAMASA", "LERV", "MABMO", "JKROG"}
DRY_RUN = False
vejman_token = orchestrator_connection.get_credential("VejmanToken").password
cosmos_credentials = orchestrator_connection.get_credential("AAKTilsynDB")
cosmos_url = cosmos_credentials.username
cosmos_key = cosmos_credentials.password
database_name = "aak-tilsyn"
container_name = "vejman-permissions"
client = CosmosClient(cosmos_url, credential=cosmos_key)
container = client.get_database_client(database_name).get_container_client(container_name)


# --- Time window for list endpoints ---
now = datetime.now(VEJMAN_TIMEZONE)
yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
today = now.strftime("%Y-%m-%d")


def build_permission_document(case: dict, case_details: dict) -> dict | None:
    case_number = safe_str(case.get("case_number"))
    case_id = safe_str(case.get("case_id"))

    if not case_number or not case_id:
        return None

    site = (case_details.get("sites") or [{}])[0] if case_details.get("sites") else {}
    building = site.get("building") or {}

    building_from = building.get("from")
    building_to = building.get("to")
    street_number_text = build_street_number_text(building_from, building_to)

    start_date_raw = case_details.get("start_date") or case.get("start_date")
    end_date_raw = case_details.get("end_date") or case.get("end_date")

    doc = {
        "id": case_number,
        "case_number": case_number,
        "case_id": case_id,
        "vejman_state": case.get("state"),

        "connected_case": nullable_str(case.get("connected_case")),
        "start_date": vejman_datetime_to_iso(start_date_raw),
        "end_date": vejman_datetime_to_iso(end_date_raw),

        "applicant": nullable_str(case.get("applicant")),
        "marker": nullable_str(case.get("marker")),
        "rovm_equipment_type": nullable_str(case.get("rovm_equipment_type")),
        "applicant_folder_number": nullable_str(case.get("applicant_folder_number")),
        "authority_reference_number": nullable_str(case.get("authority_reference_number")),

        "street_status": nullable_str(site.get("street_status")),
        "street_name": nullable_str(case.get("street_name")),
        "street_number_text": nullable_str(street_number_text),

        "initials": nullable_str(case.get("initials")),
    }

    doc["content_hash"] = compute_content_hash(doc)
    return doc


def build_street_number_text(building_from, building_to) -> str | None:
    if building_from not in (None, "") and building_to not in (None, ""):
        if str(building_from) == str(building_to):
            return str(building_from)
        return f"{building_from}-{building_to}"

    if building_from not in (None, ""):
        return str(building_from)

    if building_to not in (None, ""):
        return str(building_to)

    return None


def vejman_datetime_to_iso(value: str | None) -> str | None:
    """
    Converts '25-03-2026 05:00:00' to ISO with Europe/Copenhagen offset.
    """
    if not value:
        return None

    parsed = datetime.strptime(value, "%d-%m-%Y %H:%M:%S")
    localized = parsed.replace(tzinfo=VEJMAN_TIMEZONE)
    return localized.isoformat()


def compute_content_hash(doc: dict) -> str:
    """
    Hash only the business payload, not Cosmos/system fields.
    """
    payload = {
        "case_id": doc.get("case_id"),
        "vejman_state": doc.get("vejman_state"),
        "connected_case": doc.get("connected_case"),
        "start_date": doc.get("start_date"),
        "end_date": doc.get("end_date"),
        "applicant": doc.get("applicant"),
        "marker": doc.get("marker"),
        "rovm_equipment_type": doc.get("rovm_equipment_type"),
        "applicant_folder_number": doc.get("applicant_folder_number"),
        "authority_reference_number": doc.get("authority_reference_number"),
        "street_status": doc.get("street_status"),
        "street_name": doc.get("street_name"),
        "street_number_text": doc.get("street_number_text"),
        "initials": doc.get("initials"),
    }

    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def safe_str(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def nullable_str(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


# --- Requests session reuse ---
session = requests.Session()
session.headers.update({
    "accept": "application/json, text/plain, */*",
    "user-agent": "Aarhus Kommune Vejman Sync/1.0",
})

urls = [
    (
        "Udløbne tilladelser",
        f"https://vejman.vd.dk/permissions/getcases"
        f"?pmCaseStates=3"
        f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
        f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
        f"&pmCaseShowAttachments=false&dontincludemap=1"
        f"&endDateFrom={yesterday}&endDateTo={today}"
        f"&token={vejman_token}"
    ),
    (
        "Færdigmeldte tilladelser",
        f"https://vejman.vd.dk/permissions/getcases"
        f"?pmCaseStates=8"
        f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
        f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
        f"&pmCaseShowAttachments=false&dontincludemap=1"
        f"&endDateFrom={yesterday}&endDateTo={today}"
        f"&token={vejman_token}"
    ),
    (
        "Nye tilladelser",
        f"https://vejman.vd.dk/permissions/getcases"
        f"?pmCaseStates=3%2C6%2C8%2C12"
        f"&pmCaseFields=state%2Ctype%2Ccase_id%2Ccase_number%2Cauthority_reference_number%2Cmarker%2Cwebgtno%2Cstart_date%2Cend_date%2Capplicant_folder_number%2Cconnected_case%2Cstreet_name%2Capplicant%2Crovm_equipment_type%2Cinitials"
        f"&pmCaseWorker=all&pmCaseTypes=%27rovm%27%2C%27gt%27&pmCaseVariant=all&pmCaseTags=ignorerTags"
        f"&pmCaseShowAttachments=false&dontincludemap=1"
        f"&startDateFrom={today}&startDateTo={today}"
        f"&token={vejman_token}"
    ),
]

# We deduplicate by case_number so the same case from multiple lists is only processed once
base_cases_by_number: dict[str, dict] = {}

for header, url in urls:
    print(f"Fetching list: {header}")
    resp = session.get(url, timeout=60)
    resp.raise_for_status()

    cases = resp.json().get("cases", [])
    for case in cases:
        initials = case.get("initials")
        case_number = case.get("case_number")
        if initials not in ALLOWED_INITIALS:
            continue
        if not case_number:
            continue

        # Keep latest seen copy; details will be fetched below anyway
        base_cases_by_number[case_number] = case

print(f"Relevant unique cases found: {len(base_cases_by_number)}")

processed = 0
created = 0
updated = 0
unchanged = 0
skipped = 0

for case_number, case in base_cases_by_number.items():
    case_id = case.get("case_id")
    if not case_id:
        skipped += 1
        continue

    # Fetch details
    detail_url = f"https://vejman.vd.dk/permissions/getcase?caseid={case_id}&token={vejman_token}"
    detail_resp = session.get(detail_url, timeout=60)
    detail_resp.raise_for_status()
    case_details = detail_resp.json().get("data", {})

    doc = build_permission_document(case, case_details)
    if not doc:
        skipped += 1
        continue

    try:
        existing = container.read_item(item=doc["id"], partition_key=doc["case_number"])
    except CosmosResourceNotFoundError:
        existing = None

    if existing and existing.get("content_hash") == doc["content_hash"]:
        unchanged += 1
        processed += 1
        continue

    if DRY_RUN:
        print(
            f"DRY RUN - would upsert case_number={doc['case_number']}:\n"
            f"{json.dumps(doc, ensure_ascii=False, indent=2)}"
        )
    else:
        print(f"doc id = {repr(doc['id'])}")
        print(f"doc case_number = {repr(doc['case_number'])}")
        print(json.dumps(doc, ensure_ascii=False, indent=2))
        container.upsert_item(body=doc)

        if existing is None:
            created += 1
        else:
            updated += 1

    processed += 1

print(
    f"Done. Processed={processed}, Created={created}, Updated={updated}, "
    f"Unchanged={unchanged}, Skipped={skipped}"
)
