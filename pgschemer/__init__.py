#
#
#

import re


#
# Function: convert_rows
# Converts the standard list of lists into a list of dicts.
#
# Parameters:
#  desc - cursor.description
#  rows - cursor.fetchall

def convert_rows(desc, rows):
	descriptor = dict( [d[0], x] for (x, d) in zip(range(len(desc)), desc) )
	new_rows = []
	for row in rows:
		r = {}
		for k in descriptor:
			r[k] = row[descriptor[k]]
		new_rows.append(r)
	return new_rows
	

def cleanStatement(statement):
	repeating_ws = re.compile('\s+')
	leading_ws = re.compile('^\s')
	trailing_ws = re.compile('\s$')
	statement = re.sub(repeating_ws, ' ', statement)
	statement = re.sub(leading_ws, '', statement)
	statement = re.sub(trailing_ws, '', statement)
	return statement


class Table:
	def __init__(self, connection, tableName, columns=[]):
		self.conn = connection
		self.table_name = tableName
		self.columns = columns

		self.oid = None
		if(self.tableExists()):
			self.oid = self.getOid()
	
	def addColumn(self, column):
		self.columns.append(column)

	def tableExists(self):
		sql = """
		select count(*) from pg_tables
		where tablename = %(table_name)s
		and schemaname = any(current_schemas(false))
		"""

		curs = self.conn.cursor()
		curs.execute(sql, {'table_name' : self.table_name})
		return (curs.fetchone()[0] > 0)
	
	def getOid(self):
		"""
			Takes in a table name and returns the OID.

			WARNING! This does not check for table existance.
		"""
		oid_sql = """
			select 
				case 
					when n.nspname = current_user then -1
					when n.nspname = 'public' then 1
				else 0 end as schema_order,
				c.oid as table_id
			from pg_class c
			left join pg_namespace n on n.oid = c.relnamespace
			left join pg_tablespace t on t.oid = c.reltablespace
			where c.relkind = 'r'::"char"
			and c.relname = %(table_name)s
			and n.nspname = any(current_schemas(false))
			order by schema_order
		"""
		curs = self.conn.cursor()
		curs.execute(oid_sql, {'table_name' : self.table_name})
		return curs.fetchone()[1]

	

	def getColumnNames(self):
		"""
		Return all the columns for the table.
		"""

		curs = self.conn.cursor()
		curs.execute("""
			select attname from pg_attribute 
			where attrelid = %(oid)s 
			and attnum >= 0
			and attisdropped = false
		""", {'oid' : self.oid})
		return [x[0] for x in curs.fetchall()]


	def createSQL(self):
		sql_template = """
			create table %(table_name)s (
				%(column_sqls)s
			)
		"""

		opts = {
			'table_name' : self.table_name,
			'column_sqls' : ',\n'.join([c.createSQL() for c in self.columns])
		}

		return [sql_template % opts,]

	def alterSQL(self):
		# get the extra columns, and drop 'em.
		column_names = self.getColumnNames()
		current_column_names = [c.name for c in self.columns]
		drop_em = []
		for col in column_names:
			if(col not in current_column_names):
				drop_em.append(col)
		# turn that list into SQL statements
		drop_template = 'alter table %(name)s drop column %(col)s'
		statements = [drop_template % {'name' : self.table_name, 'col' : c} for c in drop_em]

		for col in self.columns:
			cond = col.condition(self)
			if(cond == columns.MISMATCH):
				statements += col.alterSQL(self)
			elif(cond == columns.MISSING):
				statements.append(col.addSQL(self))

		return statements
	def cleanStatements(self, statements):
		return [
			cleanStatement(s) for s in statements
		]

	def getSQL(self):
		statements = []
		if(self.tableExists()):
			# iterate through the columns
			statements = self.alterSQL()
		else:
			statements = self.createSQL()
		return self.cleanStatements(statements)



