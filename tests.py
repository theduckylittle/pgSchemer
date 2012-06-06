#!/usr/bin/env python

import psycopg2
from pgschemer.columns import VarcharColumn,SmallIntColumn,IntegerColumn,BigIntColumn,BooleanColumn,PrimaryKeyColumn
from pgschemer import Table

conn = psycopg2.connect("dbname=osm")

def execute_statements(conn, statements):
	curs = conn.cursor()
	for st in statements:
		print st
		curs.execute(st)
	

test_table = Table(conn, "test_table", columns=[
	PrimaryKeyColumn(conn, "pk"),
	VarcharColumn(conn, "a", 255),
	VarcharColumn(conn, "b", 128),
	IntegerColumn(conn, "c"),
#	VarcharColumn(conn, "c", 1),
])

if (test_table.tableExists()):
	execute_statements(conn, ["drop table test_table"])

execute_statements(conn, test_table.getSQL())

mod_test_table = Table(conn, "test_table", columns=[
	VarcharColumn(conn, "a", 255),
	VarcharColumn(conn, "b", 128),
	BooleanColumn(conn, "c",forceReplace=True),
	IntegerColumn(conn, "d")
])

execute_statements(conn, mod_test_table.getSQL())

# clean up
if (test_table.tableExists()):
	execute_statements(conn, ["drop table test_table"])
conn.commit()


