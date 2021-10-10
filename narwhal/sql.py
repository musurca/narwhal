import sys
import sqlite3
from datetime import datetime, date
from typing import get_args, get_origin


class Query:
	def Equals(var_name, val):
		return (
			f"{var_name} = ?", 
			( val, )
		)

	def NotEquals(var_name, val):
		return (
			f"{var_name} != ?", 
			( val, )
		)

	def LessThan(var_name, val):
		return (
			f"{var_name} < ?", 
			( val, )
		)

	def LessThanEquals(var_name, val):
		return (
			f"{var_name} <= ?", 
			( val, )
		)
	
	def GreaterThan(var_name, val):
		return (
			f"{var_name} > ?",
			( val, )
		)
	
	def GreaterThanEquals(var_name, val):
		return (
			f"{var_name} >= ?",
			( val, )
		)
	
	def Between(var_name, val1, val2):
		return (
			f"{var_name} between ? and ?",
			( val1, val2, )
		)

	def Like(var_name, pattern):
		return (
			f"{var_name} like ?",
			( pattern, )
		)

	def Not(expr):
		return (
			f"not ({expr[0]})",
			expr[1]
		)

	def ChainExprs(exprs, chain_str=", "):
		query_str = ""
		val_tup = ()
		expr_range = len(exprs) - 1
		for i in range(expr_range):
			expr = exprs[i]
			query_str += f"({expr[0]}){chain_str}"
			val_tup += expr[1]
		expr = exprs[expr_range]
		query_str += f"({expr[0]})"
		val_tup += expr[1]
		return ( query_str, val_tup )

	def And(*exprs):
		return Query.ChainExprs(exprs, chain_str=" and ")
	
	def Or(*exprs):
		return Query.ChainExprs(exprs, chain_str=" or ")

	OrderAscending 	= lambda v : f"{v} asc"

	OrderDescending = lambda v : f"{v} desc"

	def OrderChain(*exprs):
		return Query.ChainExprs(exprs)


class SQL:
	DEFAULT_DB = None
	TABLES = []

	use_cache: bool
	cache: dict
	use_sharedmemory: bool
	sharedmemory: dict

	TYPE_TABLE = {
		"str" 				: "text",
		"int"  				: "integer",
		"bool"				: "integer",
		"float"				: "real",
		"date"				: "date",
		"datetime" 			: "timestamp"
	}

	TYPE_DEFAULT = {
		"text" 		: "",
		"integer"	: -1,
		"real"		: 0.0,
		"date"		: date(1000, 1, 1),
		"timestamp"	: datetime(1000, 1, 1)
	}

	TYPE_ADAPTERS = {}

	def RegisterTypeConversion(data_class:type, adapter, converter, default):
		"""
		Register your custom data type for storage in the DB.

		Arguments:
		data_class	--	The type of the data
		adapter		--	The function used to convert the data into a sqlite value.
		converter	--  The function used to convert a sqlite value into the data.
		default		--  The default value
		"""
		type_name = data_class.__name__
		sql_type = type_name.lower()
		SQL.TYPE_TABLE[type_name] = sql_type
		SQL.TYPE_DEFAULT[sql_type] = default
		sqlite3.register_adapter(data_class, adapter)
		SQL.TYPE_ADAPTERS[type_name] = adapter
		sqlite3.register_converter(sql_type, converter)

	def ListIdentifier(parent_dc:type, varname:str):
		"""
		Constructs the base string for list identifier columns.
		"""
		return f"_{parent_dc.__name__.lower()}_{varname}_list"

	MakeListID 		= lambda a : f"{a}_id"

	MakeListOrder 	= lambda a : f"{a}_order"

	def RegisterList(parent_class: type, child_class:type, varname:str):
		"""
		Registers a list by producing the two new columns to maintain 
		its 1:M relations.
	
		The two new columns are both of the integer type. One stores
		the id of the parent, and the other stores the order in which
		the child is stored.
		"""
		list_id = SQL.ListIdentifier(parent_class, varname)

		id 		= SQL.MakeListID(list_id)
		order 	= SQL.MakeListOrder(list_id)

		# register it with the child if necessary
		if not hasattr(child_class, "__sql_columns__"):
			child_class.__sql_columns__ = []
		sql_columns = child_class.__sql_columns__

		list_id_column = ( id, "integer" )
		list_order_column = ( order, "integer" )
		if list_id_column not in sql_columns:
			sql_columns.append(list_id_column)
		if list_order_column not in sql_columns:
			sql_columns.append(list_order_column)

		parent_class.__list_defs__.append((varname, child_class.__name__))

	def Get():
		return SQL.DEFAULT_DB

	def __init__(self, db_path:str, use_cache=False, use_sharedmemory=False):
		self.connection = sqlite3.connect(
			db_path,
			detect_types=sqlite3.PARSE_DECLTYPES
		)
		self.connection.row_factory = sqlite3.Row
		self.use_cache = use_cache
		self.cache = {}
		self.use_sharedmemory = use_sharedmemory or use_cache
		self.sharedmemory = {}
		SQL.DEFAULT_DB = self

	def CommandHash(cmd:str, args:tuple) -> int:
		"""
		Calculates a integer hash for the SQL command after 
		substituting arguments into '?'. The hash is order-
		independent, meaning that any collection of identical
		characters will return the same hash, regardless of the
		order in which they're arranged.
		"""
		cmd_list = cmd.split("?")
		cmd_str = ""
		# substitute arguments into list
		for i in range( len(cmd_list) ):
			cmd_str += cmd_list[i]
			if i < len(args):
				arg = args[i]
				arg_typename = type(arg).__name__
				if arg_typename in SQL.TYPE_ADAPTERS:
					arg = SQL.TYPE_ADAPTERS[arg_typename](arg)
				elif hasattr(arg, "__sql_adapter__"):
					arg = arg.__sql_adapter__()
				cmd_str += str(arg)

		hash = 0
		for i in range( len(cmd_str) ):
			k = ord(cmd_str[i])
			k = k ^ (k >> 17)
			k *= 830770091
			k = k ^ (k >> 11)
			k *= -1404298415
			k = k ^ (k >> 15)
			k *= 830770091
			k = k ^ (k >> 14)
			hash += k
		return hash

	def CacheSize(self):
		"""
		Returns the size of the cache, which is just
		the size of the shared memory.
		"""
		return self.SharedMemorySize()

	def SharedMemorySize(self):
		"""
		Returns the size of shared memory.
		"""
		sz = 0
		for key, item in self.sharedmemory.items():
			sz += sys.getsizeof(key)
			for k, i in item.items():
				sz += sys.getsizeof(k)
				sz += sys.getsizeof(i)
		return sz

	def MakeColumns(data_class: type):
		"""
		Detects the columns from the underlying class,
		and stores them in a new list of tuples in the
		data class itself.

		Lists may add entries to those columns to add 
		1:M relations.
		"""
		data_class.__list_defs__ = []
		sql_columns = []
		annotations = data_class.__annotations__
		for var_name, var_type in annotations.items():
			classname = ""
			if hasattr(var_type, "__name__"):
				classname = var_type.__name__
			else:
				# Reference or List
				var_args = get_args(var_type)[0]
				var_type = get_origin(var_type)

			if classname in SQL.TYPE_TABLE.keys():
				sql_columns.append(
					( var_name, SQL.TYPE_TABLE[classname] )
				)
			elif hasattr(var_type, "__sql_type__"):
				sql_columns.append(
					( var_name, var_type.__sql_type__ )
				)
			elif var_type.__name__ == "List":
				SQL.RegisterList(data_class, var_args, var_name)
			else:
				print(f"WARNING: {var_name} in {data_class.__name__} not a recognized datatype!")
		return sql_columns

	def ColumnValuesInString(data_class:type) -> str:
		sql_columns = data_class.__sql_columns__

		column_cmd = "("
		values_cmd = "("
		last_column_index = len(sql_columns) - 1
		for i in range(last_column_index):
			column_name = sql_columns[i][0]
			column_cmd += f"{column_name},"
			values_cmd += "?,"
		column_cmd += f"{sql_columns[last_column_index][0]})"
		values_cmd += "?)"
		return (column_cmd, values_cmd)

	def RegisterTables(self, data_classes:list):
		"""
		Registers the classes that will be used as tables
		in the DB. Should be run only once.
		"""
		self.tables = data_classes
		for data_class in data_classes:
			dc_name = data_class.__name__

			# initialize shared memory
			if self.use_sharedmemory:
				self.sharedmemory[dc_name] = {}

			# initialize column names
			if not hasattr(data_class, "__sql_columns__"):
				data_class.__sql_columns__ = []
			data_class.__sql_columns__ += SQL.MakeColumns(data_class)

			# initialize table name
			if "__tablename__" not in data_class.__dict__.keys():
				data_class.__tablename__ = f"{dc_name.lower()}_table"
			
			# find foreign key relationships, if any
			foreign_keys = []
			for item_name, item_type in data_class.__annotations__.items():
				origin = get_origin(item_type)
				if hasattr(origin, "__name__"):
					if origin.__name__ == "Reference":
						child_dc = get_args(item_type)[0]
						foreign_keys.append(
							(item_name, child_dc.__tablename__)
						)
			data_class.__foreign_keys__ = foreign_keys
		
		# now that columns are finalized, cache column name + args
		for data_class in data_classes:
			data_class.__column_values__ = SQL.ColumnValuesInString(
				data_class=data_class
			)
			
	# TODO: don't create the tables if not needed
	def CreateTables(self):
		for data_class in self.tables:
			if len(data_class.__sql_columns__) > 0:
				self.CreateTable(data_class)

	def CreateTable(self, data_class:type):
		"""
		Creates a table in the DB from a class.
		"""

		sql_columns = data_class.__sql_columns__
		table_name = data_class.__tablename__

		# build table creation command via
		# tuples of ( column_name, column_type )
		column_name = lambda c : c[0]
		column_type = lambda c : c[1]
		cmd = f"create table {table_name} ( dbid integer primary key autoincrement, "
		col_range = len(sql_columns) - 1
		for i in range( col_range ):
			column = sql_columns[i]
			cmd = cmd + f"{column_name(column)} {column_type(column)}, "
		column = sql_columns[col_range]
		cmd = cmd + f"{column_name(column)} {column_type(column)}"

		# establish foreign keys
		foreign_keys = data_class.__foreign_keys__
		for foreign_key in foreign_keys:
			cmd += f", foreign key({foreign_key[0]}) references {foreign_key[1]}(dbid)"
		cmd += ")"

		# execute it
		self.connection.execute(cmd)
		self.connection.commit()

	def Clear(self, data_class:type):
		"""
		Drops the table for a corresponding class.
		"""
		table_name = data_class.__tablename__
		# clear table first
		self.connection.execute(f"delete from {table_name}")
		self.connection.commit()
		
		# Clear our shared memory
		if self.use_sharedmemory:
			self.sharedmemory[data_class.__name__] = {}

	def TableLength(self, data_class:type) -> int:
		"""
		Returns the number of rows in a table.
		"""
		table_name = data_class.__tablename__
		cmd = f"select count(1) from {table_name}"
		cursor = self.connection.cursor()
		cursor.execute(cmd)
		size = cursor.fetchone()[0]
		cursor.close()
		return size

	def Commit(self):
		"""
		Manually commit any changes made. Useful if one has
		deferred other changes.
		"""
		self.connection.commit()

	def Add(self, item, commit=True):
		"""
		Adds the item as a row to its corresponding table.

		If commit is False, then the change will not be
		immediately committed to the DB.
		"""
		data_class = item.__class__

		table_name = data_class.__tablename__
		sql_columns = data_class.__sql_columns__
		idict = item.__dict__
		valid_columns = idict.keys()

		if "dbid" in valid_columns:
			if item.dbid != -1:
				self.Update(item, commit=commit)
				return
		
		attr_list = []
		# all attributes other than the dbid
		for column in sql_columns:
			var_name = column[0]
			var_type = column[1]
			# save it if there's a value;
			# otherwise set it to a default value
			if var_name in idict.keys():
				var_value = idict[ var_name ]
				if hasattr(var_value, "__sql_adapter__"):
					var_value = var_value.__sql_adapter__()
				attr_list.append( var_value )
			else:
				attr_list.append( 
					SQL.TYPE_DEFAULT[var_type]
				)
	
		# save them to the table
		colvals = data_class.__column_values__
		cmd = f"insert into {table_name} {colvals[0]} values {colvals[1]}"
		cursor = self.connection.cursor()
		cursor.execute(cmd, attr_list)
		# Set the item dbid
		item.dbid = cursor.lastrowid

		# add the item to shared memory
		if self.use_sharedmemory:
			self.sharedmemory[data_class.__name__][item.dbid] = item

		cursor.close()

		# finally, add lists
		for list in item.__get_lists__():
			list.__add_to_db__()

		if commit:
			self.connection.commit()

	def AddList(self, i_list:list, commit=True):
		"""
		Adds the items in i_list as rows to their corresponding 
		tables.

		If commit is False, then the change will not be
		immediately committed to the DB.
		"""
		for item in i_list:
			self.Add(item, commit=False)

		if commit:
			self.connection.commit()

	def Delete(self, item, force_remove=False, commit=True):
		data_class = item.__class__
		dc_name = data_class.__name__
		if data_class.__immutable__ and not force_remove:
			print(f"WARNING: Can't delete immutable type {dc_name}!")
			return
		table_name = data_class.__tablename__
		cmd = f"delete from {table_name} where dbid = ?"
		self.connection.execute( cmd, (item.dbid,) )
		
		if self.use_cache:
			# clear cached results if they include this item
			to_remove = []
			for h, val in self.cache.items():
				if type(val) == list:
					if item in val:
						to_remove.append(h)
				else:
					if item == val:
						to_remove.append(h)
			for h in to_remove:
				self.cache.pop(h)

		if self.use_sharedmemory:
			# remove it from shared memory
			if item.dbid in self.sharedmemory[dc_name].keys():
				self.sharedmemory[dc_name].pop(item.dbid)
			
			# remove it from any References in shared memory
			for dc in self.tables:
				for fk in dc.__foreign_keys__:
					if fk[1] == dc_name:
						for v in self.sharedmemory[dc.__name__].values():
							ref = v.__get_reference__(fk[0])
							if ref.ref_id == item.dbid:
								ref.__set__(None)

			# remove it from any Lists in shared memory 
			for name, vals in self.sharedmemory.items():
				if name == dc_name:
					continue
				for ld in dc.__list_defs__:
					if ld[1] == dc_name:
						for val in vals.values():
							my_list = val.__dict__[ ld[0] ]
							if item in my_list:
								my_list.remove(item)
		
		if commit:
			self.connection.commit()

	def Update(self, item, force_update=False, commit=True):
		"""
		Updates the row of item in its corresponding table.

		If commit is False, then the change will not be
		immediately committed to the DB.
		"""
		data_class = item.__class__

		table_name = data_class.__tablename__
		sql_columns = data_class.__sql_columns__
		item_dict = item.__dict__
		valid_columns = item_dict.keys()

		if "dbid" in valid_columns:
			# construct the sql update command
			dbid = item.dbid

			if data_class.__immutable__ and not force_update:
				print(f"WARNING: Can't update immutable type {data_class.__name__}!")
				return

			update_list = []

			column_name = lambda c : c[0]
			column_type = lambda c : c[1]
			
			cmd = f"update {table_name} set "
			
			def add_to_column_list(column):
				name = column_name(column)
				if name in valid_columns:
					val = item_dict[name]
					if hasattr(val, "__sql_adapter__"):
						val = val.__sql_adapter__()
					update_list.append( val )
				else:
					update_list.append(
						SQL.TYPE_DEFAULT[ column_type(column) ]
					)

			col_range = len(sql_columns) - 1
			for i in range(col_range ):
				column = sql_columns[i]
				add_to_column_list(column)
				cmd = cmd + f"{column_name(column)} = ?, "
			column = sql_columns[col_range]
			add_to_column_list(column)
			cmd = cmd + f"{column_name(column)} = ? where dbid = ?"
			update_list.append( dbid )

			# update the db
			self.connection.execute(cmd, tuple(update_list) )

			# Finally, update lists
			for list in item.__get_lists__():
				list.__update_to_db__()

			if commit:
				self.connection.commit()
		else:
			self.Add(item, commit=commit)
	
	def UpdateList(self, item_list, force_update=False, commit=True):
		"""
		Updates the rows of the items in item_list in their 
		corresponding tables.

		If commit is False, then the change will not be
		immediately committed to the DB.
		"""
		if len( item_list ) == 0:
			return

		data_class 	= item_list[0].__class__
		table_name 	= data_class.__tablename__
		sql_columns = data_class.__sql_columns__

		if data_class.__immutable__ and not force_update:
			print(f"WARNING: Can't update immutable type {data_class.__name__}!")
			return

		arg_list = []
		i_list = []
		add_list = []
		
		column_name = lambda c : c[0]
		column_type = lambda c : c[1]

		# Build SQL command from first item
		cmd = f"update {table_name} set "
		item = item_list[0]
		col_range = len(sql_columns) - 1
		for i in range(col_range ):
			column = sql_columns[i]
			cmd = cmd + f"{column_name(column)} = ?, "
		column = sql_columns[col_range]
		cmd = cmd + f"{column_name(column)} = ? where dbid = ?"

		# Insert actual values to update
		for item in item_list:
			item_dict = item.__dict__
			valid_columns = item_dict.keys()

			if "dbid" in valid_columns:
				# construct the sql update command
				dbid = item.dbid

				update_list = []
				def add_to_column_list(column):
					name = column_name(column)
					if name in valid_columns:
						val = item_dict[name]
						if hasattr(val, "__sql_adapter__"):
							val = val.__sql_adapter__()
						update_list.append( val )
					else:
						update_list.append(
							SQL.TYPE_DEFAULT[ column_type(column) ]
						)

				col_range = len(sql_columns) - 1
				for i in range(col_range ):
					column = sql_columns[i]
					add_to_column_list(column)
				column = sql_columns[col_range]
				add_to_column_list(column)
				update_list.append( dbid )

				# Add this to the argument list
				arg_list.append(tuple(update_list))
				# Add item to list of possible list containers
				i_list.append(item)
			else:
				add_list.append(item)

			# add to db any items that weren't in it
			self.AddList(add_list, commit=commit)

			# update the db
			self.connection.executemany(cmd, arg_list)
		
			# Finally, update lists
			for item in i_list:
				for list in item.__get_lists__():
					list.__update_to_db__()

			if commit:
				self.connection.commit()

	def CopyRowToData(data_class:type, row:sqlite3.Row, existing_item=None):
		"""
		Internal method to transform data returned from the DB as sqlite3.Row
		into a corresponding object.

		If existing_item is None, then the method will allocate a new object.
		Otherwise, it will copy the data into the attributes of existing_item.
		"""
		if existing_item == None:
			item = data_class()
		else:
			item = existing_item

		annotations = data_class.__annotations__
		for column_name in row.keys():
			if column_name == "dbid":
				item.dbid = row["dbid"]
			else:
				# set the value
				if column_name in annotations:
					# if it's a reference, set the ref_id only
					ref = item.__get_reference__(column_name)
					if ref != None:
						ref.__sql_converter__(row[column_name])
						continue
							
				setattr(
					item,
					column_name, 
					row[column_name]
				)
		# Mark lists as preexisting from db
		item.__mark_lists_from_db__()
		return item

	def ProcessRow(self, data_class:str, row:sqlite3.Row):
		"""
		Process an individual Row returned from the DB.
		"""
		dbid = row["dbid"]
		dc_name = data_class.__name__
		item = None
		# If the item is in shared memory, we copy the result
		# into the same block of memory. Otherwise, we
		# make a new item, add it, then return
		if self.use_sharedmemory:
			if dbid in self.sharedmemory[dc_name].keys():
				# in shared memory
				item = self.sharedmemory[dc_name][dbid]
				SQL.CopyRowToData(data_class, row, item)
			else:
				# not in shared memory already
				item = SQL.CopyRowToData(data_class, row)
				self.sharedmemory[dc_name][dbid] = item
		else:
			item = SQL.CopyRowToData(data_class, row)
		return item

	def MakeSelectCommand(table_name:str, args:tuple, orderby="") -> str:
		"""
		Creates a SELECT command with WHERE query arguments, and 
		optional ORDER BY.
		"""
		cmd = f"select * from {table_name} where {args}"
		if orderby != "":
			cmd = cmd + f" order by {orderby}"
		return cmd

	def Select(self, data_class:type, args:tuple, orderby="") -> list:
		"""
		Selects data from the DB, and returns a list of objects.

		Arguments:

		args 	-- Produced by chaining Query functions.
		orderby	-- Produced by chaining Query.Order* functions.
		"""
		table_name = data_class.__tablename__
		cmd = SQL.MakeSelectCommand(table_name, args[0], orderby)
		
		# See if value is already in cache
		hash = 0
		if self.use_cache:
			hash = SQL.CommandHash(cmd, args[1])
			if hash in self.cache.keys():
				return self.cache[hash]
		
		cursor = self.connection.cursor()
		cursor.execute(cmd, args[1])
		results = cursor.fetchall()
		search_list = []
		for result in results:
			item = self.ProcessRow(data_class, result)
			search_list.append(item)
		cursor.close()

		if self.use_cache:
			self.cache[hash] = search_list

		return search_list

	def Count(self, data_class:type, args:tuple) -> int:
		"""
		Returns the number of rows in the table that satisfy the query.
		Result not cached.

		Arguments:

		args 	-- Produced by chaining Query functions.
		"""
		table_name = data_class.__tablename__
		cmd = f"select count(1) from {table_name} where {args[0]}"
		cursor = self.connection.cursor()
		cursor.execute(cmd, args[1])
		size = cursor.fetchone()[0]
		cursor.close()
		return size
	
	def SelectAll(self, data_class:type) -> list:
		"""
		Returns all rows of a table from the DB.

		Arguments:

		args 	-- Produced by chaining Query functions.
		"""
		table_name = data_class.__tablename__
		cmd = "select * from {table_name}"

		# See if value is already in cache
		hash = 0
		if self.use_cache:
			hash = SQL.CommandHash(cmd, ())
			if hash in self.cache.keys():
				return self.cache[hash]

		cursor = self.connection.cursor()
		cursor.execute(cmd)
		results = cursor.fetchall()
		search_list = []
		for result in results:
			item = self.ProcessRow(data_class, result)
			search_list.append(item)
		cursor.close()

		if self.use_cache:
			self.cache[hash] = search_list

		return search_list

	def SelectOne(self, data_class:type, args:tuple) -> list:
		"""
		Selects a single row from the DB, and returns an object.

		Arguments:

		args 	-- Produced by chaining Query functions.
		"""
		table_name = data_class.__tablename__
		cmd = SQL.MakeSelectCommand(table_name, args[0])
		
		# See if value is already in cache
		hash = 0
		if self.use_cache:
			hash = SQL.CommandHash(cmd, args[1])
			if hash in self.cache.keys():
				return self.cache[hash]
		
		cursor = self.connection.cursor()
		cursor.execute(cmd, args[1])
		result = cursor.fetchone()
		if result == None:
			return None
		item = self.ProcessRow(data_class, result)
		cursor.close()

		if self.use_cache:
			self.cache[hash] = item

		return item
	
	def SelectAtIndex(self, data_class:type, index:int) -> object:
		"""
		Selects a single row from the DB by its primary key.

		Arguments:

		index 	-- index of row in table (dbid, or primary key)
		"""
		table_name = data_class.__tablename__
		cmd = f"select * from {table_name} where dbid = ?"
		args = (index,)
		# See if value is already in cache
		hash = 0
		if self.use_cache:
			hash = SQL.CommandHash(cmd, args)
			if hash in self.cache.keys():
				return self.cache[hash]

		cursor = self.connection.cursor()
		cursor.execute(cmd, args )
		result = cursor.fetchone()
		if result == None:
			return None
		item = self.ProcessRow(data_class, result)
		cursor.close()

		if self.use_cache:
			self.cache[hash] = item

		return item
	
	def RandomEntries(self, data_class:type, num=1) -> list:
		"""
		Selects rows at random from the DB. Result not cached.

		Arguments:

		num 	-- number of rows to return
		"""
		table_name = data_class.__tablename__
		cursor = self.connection.cursor()
		cursor.execute(
			f"select * from {table_name} order by random() limit {max(1, int(num))}"
		)
		results = cursor.fetchall()
		search_list = []
		for result in results:
			item = self.ProcessRow(data_class, result)
			search_list.append(item)
		cursor.close()
		return search_list