pgSchemer
=========

pgSchemer is a basic schema-management module for Python and PostgreSQL that works
to keep table definitions up to date.

Thus far this code is only tested on PostgreSQL 9.1.

Pros
----

* We are actively using this code to maintain live customer databases in the wild.  It sees use.
* The format is simply and pure python.

Cons
----

This code is *very* basic and is only being used for limited table-definition management.  It
is not anywhere near as complete as the services offered by Ruby's rack.


Requires
--------

* PostgreSQL.  Tested on 9.1 but may work on other versions fine.
* psycopg2.  


Pleas for Help
--------------

I don't believe we are the only folks with this problem.  If you have more intimate knowledge of how PostgreSQL's information schema works to define and relate tables please contibute but enhancing the tests in this code that detects changes.  If you just want to slog through it like we did then please fork and add some code for additional column types, tests, the lot.  This project is in its infancy and offers a lot of room to grow.

An Example?
-----------


Here is an example that defines a table then executes the SQL statements needed to create/modify it.::

	import psycopg2
	from pgschemer.columns import VarcharColumn,SmallIntColumn,IntegerColumn,BigIntColumn,BooleanColumn,PrimaryKeyColumn
	from pgschemer import Table

	conn = psycopg2.connect("dbname=test")

	test_table = Table(conn, "test_table", columns=[
		PrimaryKeyColumn(conn, "pk"),
		VarcharColumn(conn, "a", 255),
		VarcharColumn(conn, "b", 128),
		IntegerColumn(conn, "c")
	])

	cursor = conn.cursor()
	if (test_table.tableExists()):
		cursor.execute("drop table test_table")

	for statement in test_table.getSQL():
		cursor.execute(statement)
 
