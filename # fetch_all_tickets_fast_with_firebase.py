# fetch_full_upload_clean_structure_batched.py
# pip install requests pandas firebase-admin

from __future__ import annotations

import os
import time
from typing import Dict, Any, List, Set, Tuple
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import pandas as pd
import firebase_admin
from firebase_admin import credentials, db
from firebase_admin.exceptions import InvalidArgumentError

# =================== API 配置 ===================
BASE_URL = "https://longcui-automobile-cpi-tyrbc1k7.it-cpi010-rt.cpi.cn40.apps.platform.sapcloud.cn"
PATH = "/http/PC4C/Ticket/queryOdataBatch"

# ✅ 建议用环境变量：set USERNAME=... / set PASSWORD=...
USERNAME = "XIEYONGDONG@newgonow.cn" 
PASSWORD = "Max@sap2022"
ROLE_CODES = ["1001", "40", "43"]

API_TOP = 200
API_SKIP_START = 0
TIMEOUT = 60
VERIFY_SSL = True
# ===============================================

# ================= Firebase 配置 =================
FIREBASE_DB_URL = "https://snowy-hr-report-default-rtdb.asia-southeast1.firebasedatabase.app"
FIREBASE_SA_PATH = r"C:\Users\yan\Desktop\snowy-hr-report-firebase-adminsdk-fbsvc-5dccd921e0.json"

FIREBASE_ROOT = "c4cTickets_test"   # 写入根（不要带 /）
DELETE_BEFORE_UPLOAD = True

# 你原来按 ticket 数量 flush 没问题，但还需要加“路径数/字节数”保险
BATCH_TICKETS = 500
MAX_PATHS_PER_UPDATE = 8000           # ✅ 单次 update 最大路径数（保险）
MAX_BYTES_PER_UPDATE = 6_000_000      # ✅ 单次 update 估算最大字节（保险）
# ===============================================

SERVER_TIMESTAMP = {".sv": "timestamp"}

ROLE_VARYING_FIELDS = [
    "InvolvedPartyBusinessPartnerID",
    "InvolvedPartyID",
    "InvolvedPartyName",
    "InvolvedPartyRoleID",
    "RepairerBusinessNameID",
    "RepairerEmail",
    "RepairerPhoneNumber",
    "RepairerNamePointOfContact",
    "requested_skip",
]

REQUEST_META_FIELDS = {"requested_role_code", "requested_role_name", "requested_skip"}


def sanitize_fb_key(s: str) -> str:
    if s is None:
        return ""
    s = str(s).strip()
    for ch in [".", "$", "#", "[", "]", "/"]:
        s = s.replace(ch, "_")
    return s.strip()


def norm(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def build_url(role_code: str, top: int, skip: int) -> str:
    flt = f"(CCSRQ_DPY_ROLE_CD eq '{role_code}')"
    fval = quote(flt, safe="()'")
    return BASE_URL.rstrip("/") + PATH + f"?$top={top}&$skip={skip}&$filter={fval}"


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def fetch_role_page(session: requests.Session, role_code: str, top: int, skip: int) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    url = build_url(role_code, top, skip)
    r = session.get(
        url,
        auth=HTTPBasicAuth(USERNAME, PASSWORD),
        headers={"Accept": "application/json"},
        timeout=TIMEOUT,
        verify=VERIFY_SSL,
    )
    if r.status_code != 200:
        raise RuntimeError(f"[API] role={role_code} skip={skip} top={top} HTTP {r.status_code} BODY={r.text[:500]}")
    payload = r.json()
    rows = list(payload.get("data", []))

    for rr in rows:
        rr["requested_skip"] = skip

    meta = {
        "pageSize": payload.get("pageSize"),
        "pageNumber": payload.get("pageNumber"),
        "count": payload.get("count"),
        "totalCount": payload.get("totalCount"),
    }
    return rows, meta


def iter_role_all_rows(session: requests.Session, role_code: str):
    skip = API_SKIP_START
    page = 0
    while True:
        rows, meta = fetch_role_page(session, role_code, API_TOP, skip)
        page += 1
        print(f"[FETCH] role={role_code} page={page} skip={skip} rows={len(rows)} meta={meta}")

        if not rows:
            break

        for rr in rows:
            yield rr

        if len(rows) < API_TOP:
            break
        skip += API_TOP


# ---------- Firebase init ----------
def firebase_init():
    if getattr(firebase_admin, "_apps", None) and firebase_admin._apps:
        return
    if not os.path.exists(FIREBASE_SA_PATH):
        raise SystemExit("FIREBASE_SA_PATH 私钥文件路径无效")
    if not FIREBASE_DB_URL or "YOUR-PROJECT" in FIREBASE_DB_URL:
        raise SystemExit("请填写正确的 FIREBASE_DB_URL")
    cred = credentials.Certificate(FIREBASE_SA_PATH)
    firebase_admin.initialize_app(cred, {"databaseURL": FIREBASE_DB_URL})


# ---------- ✅ 关键：安全删除（递归+分批+二分定位大子树） ----------
def _root_update_delete(paths: List[str], tries: int = 5) -> bool:
    """
    用 multi-location update 把多个路径置 None（等价删除）
    paths 必须是相对根路径（不要带 leading /）
    """
    if not paths:
        return True

    root_ref = db.reference("/")  # update at root
    payload = {p.strip("/"): None for p in paths}

    last_err = None
    for i in range(tries):
        try:
            root_ref.update(payload)
            return True
        except Exception as e:
            last_err = e
            msg = str(e)
            # 如果是“单次修改太大”，直接返回 False 让上层二分
            if "exceeds the maximum size" in msg or isinstance(e, InvalidArgumentError):
                return False
            # 其他网络/限流错误：退避重试
            wait = 1.2 * (2 ** i)
            print(f"[FB] delete-batch failed (try {i+1}/{tries}), wait {wait:.1f}s, err={e}")
            time.sleep(wait)

    # 重试都失败：上抛
    raise RuntimeError(f"[FB] delete-batch failed after {tries} tries: {last_err}")


def _delete_paths_bisect(paths: List[str]):
    """
    尝试批量删 paths，如果失败就二分拆开。
    直到定位到单个“巨大子树”路径，再递归深入删除其子节点。
    """
    if not paths:
        return

    # 先尝试整批删除
    ok = _root_update_delete(paths)
    if ok:
        return

    # 失败：拆分
    if len(paths) == 1:
        # 单个路径仍然太大：递归深入
        fb_delete_tree(paths[0])
        return

    mid = len(paths) // 2
    _delete_paths_bisect(paths[:mid])
    _delete_paths_bisect(paths[mid:])


def fb_delete_tree(path: str, shallow_batch: int = 5000):
    """
    删除一个可能很大的节点：
    - 先尝试 delete()
    - 如果 size 超限：shallow 列出子 keys，然后批量删除子节点（失败就二分定位巨大子树，递归深入）
    """
    path = path.strip("/")
    if not path:
        raise ValueError("path cannot be empty")

    ref = db.reference(path)

    # 先直接删一次（小节点很快）
    try:
        ref.delete()
        print(f"[FB] deleted: /{path}")
        return
    except InvalidArgumentError as e:
        if "exceeds the maximum size" not in str(e):
            raise
        print(f"[FB] node too large to delete in one request: /{path} -> fallback to shallow delete")

    # shallow 列子 key（只拿 key，不拿大数据）
    children = ref.get(shallow=True)

    if not children:
        print(f"[FB] nothing under /{path}")
        return

    # 某些情况下 shallow 可能返回 True（叶子），再删一次
    if children is True:
        ref.delete()
        print(f"[FB] deleted leaf: /{path}")
        return

    keys = list(children.keys())
    print(f"[FB] /{path} has {len(keys)} children (shallow). deleting children...")

    # 将 child paths 组出来（相对 root）
    child_paths = [f"{path}/{k}" for k in keys]

    # 如果 children 极多，分段喂给二分删除（避免一次塞太大列表）
    for i in range(0, len(child_paths), shallow_batch):
        seg = child_paths[i:i + shallow_batch]
        _delete_paths_bisect(seg)

    # 最后再尝试删空壳（通常已经空了）
    try:
        ref.delete()
    except Exception:
        pass
    print(f"[FB] done delete tree: /{path}")


# ---------- ✅ 关键：安全 update（超限则二分拆包） ----------
def _ref_update_once(path: str, payload: Dict[str, Any]):
    db.reference(path).update(payload)


def _update_bisect(path: str, items: List[Tuple[str, Any]]):
    if not items:
        return
    # 小包直接写
    try:
        _ref_update_once(path, dict(items))
        return
    except Exception as e:
        msg = str(e)
        if "exceeds the maximum size" not in msg and not isinstance(e, InvalidArgumentError):
            raise

    if len(items) == 1:
        # 单条仍然超限（极少），只能直接报错告诉你哪个 key 太大
        k, _ = items[0]
        raise RuntimeError(f"[FB] single path too large to update: {k}")

    mid = len(items) // 2
    _update_bisect(path, items[:mid])
    _update_bisect(path, items[mid:])


def fb_update_with_retry(path: str, payload: dict, tries: int = 5):
    last_err = None
    for i in range(tries):
        try:
            db.reference(path).update(payload)
            return
        except Exception as e:
            last_err = e
            msg = str(e)

            # ✅ size 超限：直接二分拆包写入
            if "exceeds the maximum size" in msg or isinstance(e, InvalidArgumentError):
                items = list(payload.items())
                _update_bisect(path, items)
                return

            wait = 1.2 * (2 ** i)
            print(f"[FB] update failed (try {i+1}/{tries}), wait {wait:.1f}s, err={e}")
            time.sleep(wait)
    raise RuntimeError(f"[FB] update failed after {tries} tries: {last_err}")


def split_ticket_row(row: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    ticket_data: Dict[str, Any] = {}
    role_data: Dict[str, Any] = {}

    for k, v in row.items():
        v2 = norm(v)
        if k in ROLE_VARYING_FIELDS:
            role_data[k] = v2
        elif k in REQUEST_META_FIELDS:
            continue
        else:
            ticket_data[k] = v2

    return ticket_data, role_data


def _rough_bytes(payload: Dict[str, Any]) -> int:
    # 粗略估算 payload 大小（够用来做阈值）
    import json
    return len(json.dumps(payload, ensure_ascii=False).encode("utf-8"))


def flush_batch(updates_batch: Dict[str, Any], batch_no: int, batch_ticket_count: int):
    if not updates_batch:
        return
    print(f"[UPLOAD] batch={batch_no} tickets={batch_ticket_count} paths={len(updates_batch)} bytes~{_rough_bytes(updates_batch)} ...")
    fb_update_with_retry(FIREBASE_ROOT, updates_batch)
    print(f"[UPLOAD] batch={batch_no} ✅ done")


def main():
    if not USERNAME or not PASSWORD or "YOUR_" in USERNAME or "YOUR_" in PASSWORD:
        raise SystemExit("请先填写 USERNAME / PASSWORD（建议用环境变量 C4C_USERNAME / C4C_PASSWORD）")

    firebase_init()

    if DELETE_BEFORE_UPLOAD:
        print(f"[FB] deleting /{FIREBASE_ROOT} (safe batched) ...")
        fb_delete_tree(FIREBASE_ROOT)   # ✅ 替代 db.reference().delete()
        print(f"[FB] deleted /{FIREBASE_ROOT}")

    session = make_session()

    base_written: Set[str] = set()

    updates_batch: Dict[str, Any] = {}
    batch_ticket_set: Set[str] = set()
    batch_no = 0

    total_rows = 0
    total_unique_tickets_seen: Set[str] = set()

    for role_code in ROLE_CODES:
        print(f"\n===== START ROLE {role_code} =====")
        for row in iter_role_all_rows(session, role_code):
            total_rows += 1

            tid = str(row.get("TicketID") or "").strip()
            if not tid:
                continue

            tid_key = sanitize_fb_key(tid)
            total_unique_tickets_seen.add(tid_key)

            ticket_data, role_data = split_ticket_row(row)

            # roles 节点：每个 role_code 一份
            updates_batch[f"tickets/{tid_key}/roles/{role_code}"] = role_data
            updates_batch[f"tickets/{tid_key}/updatedAt"] = SERVER_TIMESTAMP

            # ticket：只写一次
            if tid_key not in base_written:
                updates_batch[f"tickets/{tid_key}/ticket"] = ticket_data
                base_written.add(tid_key)

            if tid_key not in batch_ticket_set:
                batch_ticket_set.add(tid_key)

            # ✅ flush 条件：ticket数 / 路径数 / 估算字节数 任一达到阈值就 flush
            if (
                len(batch_ticket_set) >= BATCH_TICKETS
                or len(updates_batch) >= MAX_PATHS_PER_UPDATE
                or _rough_bytes(updates_batch) >= MAX_BYTES_PER_UPDATE
            ):
                batch_no += 1
                flush_batch(updates_batch, batch_no, len(batch_ticket_set))
                updates_batch = {}
                batch_ticket_set = set()

        print(f"===== END ROLE {role_code} =====\n")

    if updates_batch:
        batch_no += 1
        flush_batch(updates_batch, batch_no, len(batch_ticket_set))

    print("\n================ SUMMARY ================")
    print(f"Total API rows processed: {total_rows}")
    print(f"Total unique TicketIDs seen: {len(total_unique_tickets_seen)}")
    print(f"Total batches uploaded: {batch_no}")
    print(f"✅ Uploaded to /{FIREBASE_ROOT}/tickets/<TicketID>/ticket + roles/<roleCode> + updatedAt (FULL LOAD)")


if __name__ == "__main__":
    main()
