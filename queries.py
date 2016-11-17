from pymongo import DESCENDING
from datetime import date

def execute_query_1(db, date):
    result = db.get_collection('line_items').aggregate(
        [
            {"$match": {"l_shipdate": {"$lte": date}}},
            {"$group":
                {
                    "_id": {"returnflag": "$l_returnflag", "linestatus": "$l_linestatus"},
                    "sum_qty": {"$sum": "$l_quantity"},
                    "sum_base_price": {"$sum": "$l_extendedprice"},
                    "sum_disc_price": {"$sum": {"$multiply": ["$l_extendedprice", {"$subtract": [1, "$l_discount"]}]}},
                    "sum_charge": {"$sum": {"$multiply": ["$l_extendedprice", {
                        "$multiply": [{"$subtract": [1, "$l_discount"]}, {"$add": [1, "$l_tax"]}]}]}},
                    "avg_qty": {"$avg": "$l_quantity"},
                    "avg_price": {"$avg": "$l_extendedprice"},
                    "avg_disc": {"$avg": "$l_discount"},
                    "count_order": {"$sum": 1}
                }
            },
            {"$sort": {"l_returnflag": 1, "l_linestatus": 1}}
        ]
    )

    for document in result:
        print(document)


def execute_query_2(db, size, types, region):
    partial_result = db.get_collection('part_supplier').aggregate(
        [
            {"$match": {"r_name": {"$eq": region}}},
            {"$group":
                {
                    "_id": None,
                    "min_supplycost": {"$min": "$ps_supplycost"}
                }
            },
            {"$project": { "_id": 0, "min_supplycost": 1}}
        ]
    )

    min_supplycost = partial_result.next()['min_supplycost']

    result = db.get_collection('part_supplier').aggregate(
        [
            {"$match":
                {"$and":
                    [
                        {"p_size": {"$eq": size}},
                        {"$and":
                            [
                                {"p_type": {"$regex": types}},
                                {"ps_supplycost": {"$eq": min_supplycost}}
                            ]
                        }
                    ]
                }
            },
            {"$project":
                {
                    "_id": 0,
                    "s_acctbal": 1,
                    "s_name": 1,
                    "n_name": 1,
                    "p_partkey": 1,
                    "p_mfgr": 1,
                    "s_address": 1,
                    "s_phone": 1,
                    "s_comment": 1
                }
            }
        ]
    )

    for document in result:
        print(document)


def execute_query_3(db, segment, date1, date2):
    result = db.get_collection('orders').aggregate(
        [
            {"$match": {
                "c_mktsegment": {"$eq": segment},
                "o_order_date": {"$lt": date1}
            }},
            {"$unwind": "$li_values"},
            {"$match": {
                "li_values.l_shipdate": {"$gt": date2}
            }},
            {"$group":
                {
                    "_id": {"orderkey": "$_id", "orderdate": "$o_order_date", "shippriority": "$o_shippriotity"},
                    "revenue": {
                        "$sum": {"$multiply": [{"$subtract": [1, "$li_values.l_discount"]}, "$li_values.l_extendedprice"]}},
                }
            },
            {"$sort": {
                "revenue": DESCENDING, "orderdate": 1
            }}
        ]
    )

    for document in result:
        print(document)

def execute_query_4(db, name, date):
    secondDate = date.replace(date.year + 1)
    
    # result = db.get_collection('orders').aggregate(
    #     [
    #         {"$match": {
    #             "r_name": {"$eq": name},
    #             "o_order_date": {"$gte": date, "$lt": secondDate}
    #         }},
    #         {"$unwind": "$li_values"},
    #         {"$group":
    #             {
    #                 "_id": {"name": "$n_name"},
    #                 "revenue": {
    #                     "$sum": {
    #                         "$multiply": [{"$subtract": [1, db.get_collection('lineItem').findOne("$li_values.id").get("l_discount")]},  db.get_collection('lineItem').findOne("$li_values.id").get("l_extendedprice")]}},
    #             }
    #         },
    #         {"$sort": {
    #             "revenue": DESCENDING
    #         }}
    #     ]
    # )

    result = db.get_collection('orders').aggregate(
        [
            {"$match": {
                "r_name": {"$eq": name},
                "o_order_date": {"$gte": date, "$lt": secondDate}
            }},
            {"$unwind": "$li_values"},
            {"$group":
                {
                    "_id": {"name": "$n_name"},
                    "revenue": {
                        "$sum": {
                            "$multiply": [{"$subtract": [1, "$li_values.l_discount"]}, "$li_values.l_extendedprice"]}},
                }
            },
            {"$sort": {
                "revenue": DESCENDING
            }}
        ]
    )

    for document in result:
        print(document)
