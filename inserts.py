import datetime

from pymongo.errors import DuplicateKeyError, BulkWriteError


def insert_sample_data(db):
    insert_into_line_intems(db)
    insert_into_part_supplier(db)
    insert_into_orders(db)


def insert_into_orders(db):
    db_orders = db.get_collection('orders')
    if not db_orders:
        db_orders = db.create_collection('orders')
        db_orders.create_index("c_mktsegment")
    # doc1 = {
    #     "_id": "1",
    #     "o_order_date": datetime.datetime(2016, 11, 6, 20, 45),
    #     "c_mktsegment": "Computers",
    #     "li_values": [
    #         {"id": "1+1"},
    #         {"id": "1+2"}
    #     ],
    #     "o_shippriotity": 0,
    #     "r_name": "BCN",
    #     "n_name": "ESP"
    # }
    # doc2 = {
    #     "_id": "2",
    #     "o_order_date": datetime.datetime(2016, 11, 16, 20, 59),
    #     "c_mktsegment": "Computers",
    #     "li_values": [
    #         {"id": "2+1"}
    #     ],
    #     "o_shippriotity": 1,
    #     "r_name": "BCN",
    #     "n_name": "ESP"
    # }
    # doc3 = {
    #     "_id": "3",
    #     "o_order_date": datetime.datetime(2016, 11, 6, 20, 45),
    #     "c_mktsegment": "Chemistry",
    #     "li_values": [],
    #     "r_name": "BER",
    #     "n_name": "GER"
    # }
    doc1 = {
        "_id": "1",
        "o_order_date": datetime.datetime(2016, 11, 6, 20, 45),
        "c_mktsegment": "Computers",
        "li_values": [
            {
                "l_shipdate": datetime.datetime(2016, 11, 17, 9, 30),
                "l_extendedprice": 5.50,
                "l_discount": 0.50,
                "l_quantity": 1,
                "l_tax": 1.20,
                "l_returnflag": "F",
                "l_linestatus": "R"
            },
            {
                "_id": "1+2",
                "l_shipdate": datetime.datetime(2016, 11, 17, 9, 30),
                "l_extendedprice": 2.00,
                "l_discount": 0.00,
                "l_quantity": 3,
                "l_tax": 1.20,
                "l_returnflag": "F",
                "l_linestatus": "R"
            }
        ],
        "o_shippriotity": 0,
        "r_name": "BCN",
        "n_name": "ESP"
    }
    doc2 = {
        "_id": "2",
        "o_order_date": datetime.datetime(2016, 11, 16, 20, 59),
        "c_mktsegment": "Computers",
        "li_values": [
            {
                "_id": "2+1",
                "l_shipdate": datetime.datetime(2016, 11, 17, 10, 0),
                "l_extendedprice": 15.00,
                "l_discount": 0.3,
                "l_quantity": 1,
                "l_tax": 1.20,
                "l_returnflag": "F",
                "l_linestatus": "S"
            }
        ],
        "o_shippriotity": 1,
        "r_name": "BCN",
        "n_name": "ESP"
    }
    doc3 = {
        "_id": "3",
        "o_order_date": datetime.datetime(2016, 11, 6, 20, 45),
        "c_mktsegment": "Chemistry",
        "li_values": [],
        "r_name": "BER",
        "n_name": "GER"
    }
    try:
        db_orders.insert_many([doc1, doc2, doc3])
    except Exception:
        print ("Duplicates entries")


def insert_into_part_supplier(db):
    db_part_supplier = db.get_collection('part_supplier')
    if not db_part_supplier:
        db_part_supplier = db.create_collection('part_supplier')
        db_part_supplier.create_index("ps_supplycost")
        db_part_supplier.create_index("r_name")
    doc1 = {
        "_id": "1+1",
        "s_phone": "123456",
        "s_address": "Light Street 123, NY",
        "p_size": 1,
        "p_mfgr": "I don't know what is this",
        "p_type": "Basic",
        "s_name": "Light Lights",
        "r_name": "US-NY",
        "ps_supplycost": 1.99,
        "n_name": "USA",
        "s_acctbal": 0.0,
        "s_comment": "This is a comment"
    }
    doc2 = {
        "_id": "2+1",
        "p_type": "Basic",
        "n_name": "USA",
        "s_name": "Light Lights",
        "r_name": "US-NY",
        "ps_supplycost": 1.99,
        "p_size": 2,
        "s_acctbal": 0.0
    }
    doc3 = {
        "_id": "3+2",
        "p_type": "Luxury",
        "n_name": "USA",
        "s_name": "Golden Gold",
        "r_name": "US-WA",
        "ps_supplycost": 99.99,
        "p_size": 4,
        "s_acctbal": 1000.0
    }
    try:
        db_part_supplier.insert_many([doc1, doc2, doc3])
    except Exception:
        print ("Duplicates entries")


def insert_into_line_intems(db):
    db_line_items = db.get_collection('line_items')
    if not db_line_items:
        db_line_items = db.create_collection('line_items')
        db_line_items.create_index("l_shipdate")
    doc1 = {
        "_id": "1+1",
        "l_shipdate": datetime.datetime(2016, 11, 17, 9, 30),
        "l_extendedprice": 5.50,
        "l_discount": 0.50,
        "l_quantity": 1,
        "l_tax": 1.20,
        "l_returnflag": "F",
        "l_linestatus": "R"
    }
    doc2 = {
        "_id": "1+2",
        "l_shipdate": datetime.datetime(2016, 11, 17, 9, 30),
        "l_extendedprice": 2.00,
        "l_discount": 0.00,
        "l_quantity": 3,
        "l_tax": 1.20,
        "l_returnflag": "F",
        "l_linestatus": "R"
    }
    doc3 = {
        "_id": "2+1",
        "l_shipdate": datetime.datetime(2016, 11, 17, 10, 0),
        "l_extendedprice": 15.00,
        "l_discount": 0.3,
        "l_quantity": 1,
        "l_tax": 1.20,
        "l_returnflag": "F",
        "l_linestatus": "S"
    }
    try:
        db_line_items.insert_many([doc1, doc2, doc3])
    except Exception:
        print ("Duplicates entries")
