from __future__ import annotations

from typing import Any, Dict

import firebase_admin
from firebase_admin import credentials, db

FIREBASE_DB_URL = "https://snowy-hr-report-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_SA_PATH = r"C:\\Users\\yan\\Desktop\\snowy-hr-report-firebase-adminsdk-fbsvc-5dccd921e0.json"

ROOT = "c4cTickets_test_2"
STATUSES = {
    "Awaiting Parts",
    "Claim Assessment",
    "Evidence Requested",
    "New Claim",
    "Quote Validation",
    "Suspended Claim",
}


def firebase_init() -> None:
    if getattr(firebase_admin, "_apps", None) and firebase_admin._apps:
        return
    cred = credentials.Certificate(FIREBASE_SA_PATH)
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, list):
        return {str(idx): item for idx, item in enumerate(value) if item is not None}
    return {}


def main() -> None:
    firebase_init()
    tickets = _as_dict(db.reference(f"{ROOT}/tickets").get())

    counts = {status: 0 for status in STATUSES}
    for ticket in tickets.values():
        status = (ticket or {}).get("ticket", {}).get("TicketStatusText")
        if status in counts:
            counts[status] += 1

    total = sum(counts.values())
    for status in sorted(counts):
        print(f"{status}: {counts[status]}")
    print(f"Total: {total}")


if __name__ == "__main__":
    main()
