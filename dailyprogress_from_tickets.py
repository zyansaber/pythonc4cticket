from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Tuple

import firebase_admin
from firebase_admin import credentials, db

FIREBASE_DB_URL = "https://snowy-hr-report-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_SA_PATH = r"C:\Users\yan\Desktop\snowy-hr-report-firebase-adminsdk-fbsvc-5dccd921e0.json"

CURRENT_ROOT = "c4cTickets_test"
PREVIOUS_ROOT = "c4cTickets_test_1"
DAILY_PROGRESS_ROOT = "dailyprogress"
PREVIOUS_UPDATEAT = "2026-01-30T00:00:00"
DEBUG_TICKET_ID = "30105"


def firebase_init() -> None:
    if getattr(firebase_admin, "_apps", None) and firebase_admin._apps:
        return
    cred = credentials.Certificate(FIREBASE_SA_PATH)
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})


def _as_dict(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _normalize_ticket_map(raw: Dict[Any, Any]) -> Dict[str, Any]:
    return {str(k): v for k, v in raw.items()}


def _debug_ticket_status(ticket_id: str) -> None:
    curr_ticket = db.reference(f"{CURRENT_ROOT}/tickets/{ticket_id}/ticket").get() or {}
    prev_ticket = db.reference(f"{PREVIOUS_ROOT}/tickets/{ticket_id}/ticket").get() or {}
    curr_status = (curr_ticket or {}).get("TicketStatus")
    prev_status = (prev_ticket or {}).get("TicketStatus")
    curr_text = (curr_ticket or {}).get("TicketStatusText")
    prev_text = (prev_ticket or {}).get("TicketStatusText")

    print(
        "[DEBUG] TicketID {tid}: "
        "current TicketStatus={curr_status}, TicketStatusText={curr_text}; "
        "previous TicketStatus={prev_status}, TicketStatusText={prev_text}".format(
            tid=ticket_id,
            curr_status=curr_status,
            curr_text=curr_text,
            prev_status=prev_status,
            prev_text=prev_text,
        )
    )


def _summarize_diff(current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    created_on_count_current = sum(
        1 for ticket in current.values() if isinstance(ticket, dict) and ticket.get("ticket", {}).get("CreatedOn")
    )
    created_on_count_previous = sum(
        1 for ticket in previous.values() if isinstance(ticket, dict) and ticket.get("ticket", {}).get("CreatedOn")
    )

    status_changes: Dict[Tuple[Any, Any], int] = {}
    status_text_changes: Dict[Tuple[Any, Any], int] = {}
    changed_tickets = []

    for tid, curr_ticket in current.items():
        prev_ticket = previous.get(tid, {})
        curr_info = (curr_ticket or {}).get("ticket", {})
        prev_info = (prev_ticket or {}).get("ticket", {})

        curr_status = curr_info.get("TicketStatus")
        prev_status = prev_info.get("TicketStatus")
        if curr_status != prev_status:
            status_changes[(prev_status, curr_status)] = status_changes.get((prev_status, curr_status), 0) + 1

        curr_status_text = curr_info.get("TicketStatusText")
        prev_status_text = prev_info.get("TicketStatusText")
        if curr_status_text != prev_status_text:
            status_text_changes[(prev_status_text, curr_status_text)] = (
                status_text_changes.get((prev_status_text, curr_status_text), 0) + 1
            )

        if curr_status != prev_status or curr_status_text != prev_status_text:
            role_40_name = (curr_ticket or {}).get("roles", {}).get("40", {}).get("InvolvedPartyName")
            changed_tickets.append(
                {
                    "TicketID": tid,
                    "TicketStatus": curr_status,
                    "TicketStatusText": curr_status_text,
                    "Role40InvolvedPartyName": role_40_name,
                }
            )

    return {
        "createdOnCountCurrent": created_on_count_current,
        "createdOnCountPrevious": created_on_count_previous,
        "createdOnCountDelta": created_on_count_current - created_on_count_previous,
        "ticketStatusChanges": [
            {"from": k[0], "to": k[1], "count": v, "role40InvolvedPartyNames": _role_names_for_change(current, previous, k)}
            for k, v in sorted(status_changes.items(), key=lambda x: (-x[1], x[0]))
        ],
        "ticketStatusTextChanges": [
            {"from": k[0], "to": k[1], "count": v, "role40InvolvedPartyNames": _role_names_for_change(current, previous, k, field="TicketStatusText")}
            for k, v in sorted(status_text_changes.items(), key=lambda x: (-x[1], x[0]))
        ],
        "changedTickets": changed_tickets,
    }


def _role_names_for_change(
    current: Dict[str, Any],
    previous: Dict[str, Any],
    change_pair: Tuple[Any, Any],
    field: str = "TicketStatus",
) -> list:
    names = set()
    for tid, curr_ticket in current.items():
        prev_ticket = previous.get(tid, {})
        curr_info = (curr_ticket or {}).get("ticket", {})
        prev_info = (prev_ticket or {}).get("ticket", {})
        if (prev_info.get(field), curr_info.get(field)) == change_pair:
            role_40_name = (curr_ticket or {}).get("roles", {}).get("40", {}).get("InvolvedPartyName")
            if role_40_name:
                names.add(role_40_name)
    return sorted(names)


def main() -> None:
    firebase_init()

    current_updateat = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    db.reference(CURRENT_ROOT).update({"updateat": current_updateat})

    current_tickets = _normalize_ticket_map(_as_dict(db.reference(f"{CURRENT_ROOT}/tickets").get()))
    previous_tickets = _normalize_ticket_map(_as_dict(db.reference(f"{PREVIOUS_ROOT}/tickets").get()))

    _debug_ticket_status(DEBUG_TICKET_ID)

    summary = _summarize_diff(current_tickets, previous_tickets)
    summary["currentUpdateAt"] = current_updateat
    summary["previousUpdateAt"] = PREVIOUS_UPDATEAT

    db.reference(DAILY_PROGRESS_ROOT).set(summary)
    print(f"[FB] dailyprogress updated with {len(summary['changedTickets'])} changed tickets.")


if __name__ == "__main__":
    main()
