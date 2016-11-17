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
        print document


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
        print document


def execute_query_3(db, segment, date1, date2):
    pass


def execute_query_4(db, date):
    pass
