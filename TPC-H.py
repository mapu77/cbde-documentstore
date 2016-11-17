import datetime
from pymongo import MongoClient

from inserts import insert_sample_data
from queries import execute_query_1, execute_query_2, execute_query_3, execute_query_4


def show_options():
    print("Enter an option to do:")
    print ("\t 1 - Execute Query 1")
    print ("\t 2 - Execute Query 2")
    print ("\t 3 - Execute Query 3")
    print ("\t 4 - Execute Query 4")
    print ("\t 5 - Inserts (Warning: this can only be done once)")
    print ("\t 0 - Exit")


# Start of the program

# Get Mongo collections
client = MongoClient()
db = client['test']

show_options()
op = input()

while op != 0:
    if op == 1:
        date = str(input("Please enter a date: (yyyy-mm-dd) "))
        execute_query_1(db, datetime.datetime.strptime(date, "%Y-%m-%d"))
    elif op == 2:
        size = input("Please enter a size: ")
        types = input("Please enter a type: ")
        region = input("Please enter a region: ")
        execute_query_2(db, size, types, region)
    elif op == 3:
        segment = input("Please enter a segment: ")
        date1 = input("Please enter the first date: ")
        date2 = input("Please enter the second date: ")
        execute_query_3(db, segment, date1, date2)
    elif op == 4:
        date = input("Please enter a date: ")
        execute_query_4(db, date)
    elif op == 5:
        print ("Inserting sample data...")
        insert_sample_data(db)
        print ("Done!")
    else:
        print ("Error: This option is not valid")

    show_options()
    op = input()

print ("Bye!")
