from __future__ import annotations

from datetime import datetime, timedelta

import firebase_admin
from firebase_admin import credentials, db


FIREBASE_DB_URL = "https://snowy-hr-report-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_SA_PATH = r"C:\Users\yan\Desktop\snowy-hr-report-firebase-adminsdk-fbsvc-5dccd921e0.json"
FIREBASE_ROOT = "c4cTickets_test"


def last_friday_midnight_local() -> datetime:
    today = datetime.now().date()
    days_since_friday = (today.weekday() - 4) % 7
    if days_since_friday == 0:
        days_since_friday = 7
    last_friday = today - timedelta(days=days_since_friday)
    return datetime.combine(last_friday, datetime.min.time())


def firebase_init() -> None:
    if getattr(firebase_admin, "_apps", None) and firebase_admin._apps:
        return
    cred = credentials.Certificate(FIREBASE_SA_PATH)
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})


def main() -> None:
    firebase_init()
    stamp = last_friday_midnight_local().isoformat()
    db.reference(FIREBASE_ROOT).update({"updateat": stamp})
    print(f"[FB] updated /{FIREBASE_ROOT}/updateat = {stamp}")


if __name__ == "__main__":
    main()
