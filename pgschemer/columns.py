

from . import convert_rows,cleanStatement

# Returned when column definition mathes
OKAY = 0
# Returned when the column is missing altogether
MISSING = 1
# Returned when the column definition is mismatched
MISMATCH = 2

class Column:
	def __init__(self, conn, columnName, dataType, precision=None, default=None, notNull=False, primaryKey=False, forceReplace=False):
		self.conn = conn
		self.name = columnName 
		self.notNull = notNull 
		self.data_type = dataType
		self.precision = precision
		self.default = default 
		self.primaryKey = primaryKey
		self.force = forceReplace

	def _commonError(self):
		raise ValueError('Column class does not define a data type, use a child class to define columns')

	def getPrecisionDefinition(self):
		if(self.precision == None):
			return ''
		return '('+str(self.precision)+')'

	def getDefaultDefinition(self):
		raise Exception("Need to define default behavior")

	def _getColumnDefinition(self):
		defn = {
			'name' : self.name,
			'data_type' : self.data_type,
			'precision_def' : self.getPrecisionDefinition(),
			'not_null_def' : '',
			'default_def' : self.getDefaultDefinition(),
			'primary_key' : ''
		}

		if(self.notNull):
			defn['not_null_def'] = 'not null'
		if(self.primaryKey):
			defn['primary_key'] = 'primary key'
		return defn

	def dropSQL(self, table):
		defn = self._getColumnDefinition()
		defn['table_name'] = table.table_name
		return 'alter table %(table_name)s drop column if exists %(name)s' % defn 
			
	def alterSQL(self, table):
		"""
			Return the alter SQL to be used for updating the column definition.
		"""
		defn = self._getColumnDefinition()
		defn['table_name'] = table.table_name
		statements = []
		if(self.force):
			statements.append(self.dropSQL(table))
			statements.append(self.addSQL(table))
		else:
			statements.append('alter table %(table_name)s alter column %(name)s type %(data_type)s %(precision_def)s')
		return [st % defn for st in statements]

		#return 'alter table %(table_name)s alter column %(name)s %(data_type)s %(precision_def)s %(not_null_def)s %(default_def)s %(primary_key)s' % defn

	def addSQL(self, table):
		defn = self._getColumnDefinition()
		defn['table_name'] = table.table_name
		return 'alter table %(table_name)s add column %(name)s %(data_type)s %(precision_def)s %(not_null_def)s %(default_def)s %(primary_key)s' % defn


	def getType(self, typeCode):
		"""
			Returns the type definition for a given typeCode
		"""
		curs = self.conn.cursor()
		curs.execute("select typname from pg_type where typelem = %(code)s", {
			'code' : typeCode
		})

		return convert_rows(curs.description, curs.fetchall())

	def checkDataType(self, columnDesc):
		type_desc = self.getType(columnDesc['data_type'])
		return (type_desc['typname'] == self.data_type)

	def checkPrecision(self, columnDesc):
		if(self.precision == None):
			return True
		if(columnDesc['attlen'] == self.precision):
			return True

		return False 

	def condition(self, table):
		global MISSING, MISMATCH, OKAY
		# get the table oid
		table_oid = table.oid
		
		# Get the column profile.
		column_desc_sql = """
			select attrelid table_id, 
				attname column_name, 
				attlen,
				atttypmod,
				attnum,
				attndims,
				typname data_type, 
				attnotnull not_null,
				atthasdef has_default,
				atttypmod,
				attstorage,
				attoptions
			from pg_attribute, pg_type 
			where attname = %(column_name)s
			and attrelid = %(table_id)s
			and pg_type.typelem = atttypid
			order by attname
		"""

		# check to see if the column exists
		column_exists_sql = """
			select count(*) from pg_attribute
			where attname = %(column_name)s
			and attrelid = %(table_id)s 
		"""


		curs = self.conn.cursor()
		args = {'table_id' : table_oid, 'column_name' : self.name}
		curs.execute(column_exists_sql, args)
		if(curs.fetchone()[0] < 1):
			return MISSING 

		curs.execute(column_desc_sql, args)
		column_profile = convert_rows(curs.description, [curs.fetchone()])[0]

		if(not self.checkDataType(column_profile) or not self.checkPrecision(column_profile)):
			return MISMATCH

		return OKAY 

	def createSQL(self):
		"""
			Returns the SQL to be used during table creation.
		"""
		defn = self._getColumnDefinition()
		return cleanStatement('%(name)s %(data_type)s %(precision_def)s %(not_null_def)s %(default_def)s %(primary_key)s' % defn)


class VarcharColumn(Column):
	"""
	"""
	def __init__(self, conn, columnName, precision, default=None, notNull=False,forceReplace=False):
		Column.__init__(self, conn, columnName, 'varchar', precision=precision, default=default, notNull=notNull, forceReplace=forceReplace)
	
	def checkPrecision(self, columnProfile):
		#print 'precision: ',self.precision,columnProfile['atttypmod']
		return (columnProfile['atttypmod'] == (self.precision + 4))

	def checkDataType(self, columnProfile):
		return (columnProfile['data_type'] in ['varchar','_varchar'])

	def getDefaultDefinition(self):
		# half-assed escaping attempt.
		if(self.default == None):
			return ''
		default = self.default.replace("'", "''")
		return "default '%s'" % default

class IntegerColumn(Column):
	def __init__(self, conn, columnName, default=None, notNull=False,forceReplace=False):
		self.internal_type = '_int4'
		Column.__init__(self, conn, columnName, 'integer', precision=None, default=default, notNull=notNull,forceReplace=forceReplace)

	def checkPrecision(self, columnProfile):
		return (self.precision == None or self.precision == columnProfile['attlen'])
	
	def checkDataType(self, columnProfile):
		return (columnProfile['data_type'] == self.internal_type)
	
	def getDefaultDefinition(self):
		if(self.default == None):
			return ''
		return "default %d" % self.default

class BigIntColumn(IntegerColumn):
	def __init__(self, conn, columnName, default=None, notNull=False,forceReplace=False):
		self.internal_type = '_int8'
		Column.__init__(self, conn, columnName, 'bigint', precision=None, default=default, notNull=notNull,forceReplace=forceReplace)


class SmallIntColumn(IntegerColumn):
	def __init__(self, conn, columnName, default=None, notNull=False,forceReplace=False):
		self.internal_type = '_int2'
		Column.__init__(self, conn, columnName, 'smallint', precision=None, default=default, notNull=notNull,forceReplace=forceReplace)



class BooleanColumn(IntegerColumn):
	def __init__(self, conn, columnName, default=None, notNull=False,forceReplace=False):
		self.internal_type = '_bool'
		Column.__init__(self, conn, columnName, 'boolean', precision=None, default=default, notNull=notNull,forceReplace=forceReplace)

	def getDefaultDefinition(self):
		if(self.default == None):
			return ''
		if(self.default):
			return 'default true'
		else:
			return 'default false'

#class SerialPrimaryKeyColumn(Column):
	

