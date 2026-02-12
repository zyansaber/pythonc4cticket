# -*- coding: utf-8 -*-

import logging
import pandas as pd
import pyodbc

# ================= CONFIG =================

HANA_SERVERNODE = "10.11.2.25:30241"
HANA_UID = "BAOJIANFENG"
HANA_PWD = "Xja@2025ABC"

DSN = (
    "DRIVER={HDBODBC};"
    f"SERVERNODE={HANA_SERVERNODE};"
    f"UID={HANA_UID};"
    f"PWD={HANA_PWD};"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger()

# ================= COUNT LIST =================

COUNT_LIST = [
"RRV220232","RRV21116","RRV21118","RRV220138","LRV240704",
"SRC253292","SRC253317","SRC253268","SRC254562","SRC254830",
"SRC254939","LRV233305","SRC253795","SRC254056","SRC254041",
"SRC254536","SRC254940","SRC254756","SRC255458","SRC243593",
"SRC253151","SRC253826","SRC243809","SRC254501","SRC254917",
"SRC255119","SRC254890","SRC254587","SRC254999","SRC243133",
"SRC254440","SRC254550","SRC254602","SRC254537","SRC253265",
"SRC254399","SRC255261","SRC255001","SRC253942","SRC254781",
"SRC254460","SRC254575","SRC254773","SRC254787","SRC254511",
"SRH253259","SRH243743","SRH254510","SRH253701","SRH243734",
"SRH253261","SRH254642","SRH254772","SRH250051","SRH250130",
"SRH250129","SRH254549","SRP253941","SRP254377","LRP230002",
"SRP253063","SRP253295","SRP253844","SRP254608","SRP255036",
"SRP255250","SRP254755","SRP255004","SRP255003","SRT253740",
"SRT254777","SRT254466","SRT254583","LRT240090","LRT240093",
"LRT240080","SRT253271","SRT244022","SRT253071","SRT253836",
"SRT253322","SRT254920","SRT255169","SRT254551","SRT254600",
"SRT254921","SRT254981","LRT240507","LRT230482","SRT253827",
"SRT254470","SRT254592","SRT255171","SRV250013","SRP255007"
]

# ================= DB =================

def hana_query(sql):
    with pyodbc.connect(DSN, autocommit=True) as conn:
        return pd.read_sql(sql, conn)

# ================= STEP 1: 真实库存 =================

def fetch_true_stock():

    sql_stock = """
    SELECT DISTINCT
        objk."SERNR" AS "Chassis",
        vbak."VBELN" AS "SalesOrder"

    FROM "SAPHANADB"."NSDM_V_MSKA" nsmka
    LEFT JOIN "SAPHANADB"."SER02" ser02
        ON nsmka."VBELN" = ser02."SDAUFNR"
       AND ser02."POSNR" = '000010'
    LEFT JOIN "SAPHANADB"."OBJK" objk
        ON ser02."OBKNR" = objk."OBKNR"
    LEFT JOIN "SAPHANADB"."VBAK" vbak
        ON ser02."SDAUFNR" = vbak."VBELN"

    WHERE nsmka."WERKS" = '3211'
      AND nsmka."LGORT" = '0002'
      AND nsmka."KALAB" > 0
      AND nsmka."MATNR" LIKE 'Z12%'
    """

    df_stock = hana_query(sql_stock)

    sql_move = """
    SELECT
        mseg."KDAUF" AS "SalesOrder",
        mseg."BWART",
        mseg."BUDAT_MKPF"
    FROM "SAPHANADB"."NSDM_V_MSEG" mseg
    WHERE mseg."BWART" IN ('601','602')
    """

    df_move = hana_query(sql_move)
    df_move["BUDAT_MKPF"] = pd.to_datetime(df_move["BUDAT_MKPF"])

    df_last = (
        df_move.sort_values("BUDAT_MKPF")
        .groupby("SalesOrder")
        .last()
        .reset_index()
    )

    df_stock = df_stock.merge(df_last, on="SalesOrder", how="left")

    # 删除最后是601的
    df_true = df_stock[df_stock["BWART"] != "601"]

    return set(df_true["Chassis"].dropna())


# ================= STEP 2: 对比 =================

def build_mismatch(sap_set):

    list_set = set(COUNT_LIST)
    all_serial = sorted(list_set.union(sap_set))

    rows = []

    for s in all_serial:
        if s in list_set and s in sap_set:
            continue
        elif s in list_set:
            rows.append([s, "Only in List"])
        else:
            rows.append([s, "Only in SAP"])

    return pd.DataFrame(rows, columns=["Chassis","Mismatch_Type"])


# ================= STEP 3A: 统计 (3120 only) =================

def fetch_statistics(serial_list):

    in_list = "(" + ",".join(f"'{c}'" for c in serial_list) + ")"

    sql = f"""
    SELECT
        obj."SERNR" AS "Chassis",
        COUNT(DISTINCT vbak."VBELN") AS "SalesOrder_Count",
        SUM(CASE WHEN mseg."BWART"='601' THEN 1 ELSE 0 END) AS "PGI_Count",
        SUM(CASE WHEN mseg."BWART"='602' THEN 1 ELSE 0 END) AS "Reverse_Count",
        MAX(mseg."BUDAT_MKPF") AS "Last_Movement_Date",
        MAX(mseg."BWART") AS "Last_Movement_Type"

    FROM "SAPHANADB"."OBJK" obj
    LEFT JOIN "SAPHANADB"."SER02" s
        ON obj."OBKNR" = s."OBKNR"
    LEFT JOIN "SAPHANADB"."VBAK" vbak
        ON s."SDAUFNR" = vbak."VBELN"
       AND vbak."VKORG" = '3120'
    LEFT JOIN "SAPHANADB"."NSDM_V_MSEG" mseg
        ON mseg."KDAUF" = vbak."VBELN"

    WHERE obj."SERNR" IN {in_list}

    GROUP BY obj."SERNR"
    """

    return hana_query(sql)


# ================= MAIN =================

def main():

    sap_set = fetch_true_stock()

    df_mismatch = build_mismatch(sap_set)

    mismatch_list = df_mismatch["Chassis"].tolist()

    df_stats = fetch_statistics(mismatch_list)

    df_stats = df_stats.merge(df_mismatch, on="Chassis", how="left")

    with pd.ExcelWriter("StJames_Audit_Final.xlsx") as writer:
        df_mismatch.to_excel(writer, sheet_name="Mismatch_List", index=False)
        df_stats.to_excel(writer, sheet_name="Mismatch_Statistics", index=False)

    log.info("Audit complete.")

if __name__ == "__main__":
    main()
