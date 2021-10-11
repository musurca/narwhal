from typing import Generic, TypeVar, get_origin

from .sql import SQL, Query, NULL_INT

T = TypeVar("T")

class Reference(Generic[T]):
	__sql_type__ = "integer"

	child_dc 	: type
	ref_id 		: int
	cached		: object
	initialized	: bool

	def __sql_adapter__(self):
		if self.ref_id == NULL_INT:
			if self.cached != None:
				# Add it if it hasn't already
				SQL.Get().Add(self.cached, commit=False)
				self.ref_id = self.cached.dbid
		return self.ref_id

	def __sql_converter__(self, i):
		self.ref_id = i

	def set_childtype(self, child_dc:type):
		self.child_dc = child_dc

	def __init__(self):
		self.ref_id = NULL_INT
		self.cached = None
		self.initialized = False

	def __set__(self, obj):
		if obj != None:
			assert(type(obj) == self.child_dc)
			self.ref_id = obj.dbid
		else:
			self.ref_id = NULL_INT
		self.cached = obj

	def __get__(self):
		if self.ref_id != NULL_INT:
			if self.child_dc.__immutable__ and self.cached != None:
				return self.cached
			#dc = get_args(self.__orig_class__)[0]
			self.cached = SQL.Get().SelectAtIndex(
				self.child_dc,
				self.ref_id
			)
		self.initialized = True
		return self.cached


class List(Generic[T]):
	parent			: object
	child_dc		: type
	identifier		: str
	initialized		: bool
	items 			: list
	former_items	: list
	from_db			: bool

	# For looking up the parent from the child
	@classmethod
	def ReverseLookup(cls, child:object, parent_dc:type, var_name:str):
		id = SQL.MakeListID( SQL.ListIdentifier(parent_dc, var_name) )
		parent_dbid = child.__dict__[id]
		if parent_dbid != NULL_INT:
			return SQL.Get().SelectOne(
				parent_dc,
				Query.Equals("dbid", parent_dbid)
			)
		return None

	def __init__(self):
		self.identifier = ""
		self.initialized = False
		self.items = []
		self.former_items = []
		self.from_db = False

	def __mark_from_db__(self):
		self.from_db = True

	def __validate_parent__(self):
		# safety check to make sure we have a valid id
		# and a valid parent
		assert(self.identifier != "")
		assert(self.parent.dbid != NULL_INT)

	def __is_loaded__(self):
		if self.from_db:
			return self.initialized
		return True

	def __check_loaded__(self):
		if self.from_db:
			if not self.initialized:
				self.__validate_parent__()
				#dc = get_args(self.__orig_class__)[0]
				sql = SQL.Get()
				self.items = sql.Select(
					self.child_dc,
					Query.Equals(self.id_key, self.parent.dbid),
					Query.OrderAscending(self.order_key)
				)		
				self.initialized = True

	def __len__(self):
		self.__check_loaded__()	
		return len(self.items)

	def __add__(self, x):
		self.__check_loaded__()
		t = type(x)
		if t == list or t == tuple or get_origin(t) == List:
			for item in x:
				assert(type(item) == self.child_dc)
				self.append(item)
		else:
			assert(type(x) == self.child_dc)
			self.append(x)
	
	def __sub__(self, x):
		self.__check_loaded__()
		t = type(x)
		if t == list or t == tuple or get_origin(t) == List:
			for item in x:
				assert(type(item) == self.child_dc)
				if item in self.items:
					self.remove(item)
		else:
			assert(type(x) == self.child_dc)
			if x in self.items:
				self.remove(x)

	def set_parent_child(self, parent:object, child_class:type, varname:str):
		# determine list id
		self.parent = parent
		self.child_dc = child_class
		self.identifier = SQL.ListIdentifier(
			parent.__class__,
			varname
		)

	def get_identifier(self):
		return self.identifier
	
	@property
	def id_key(self):
		return SQL.MakeListID(self.identifier)

	@property
	def order_key(self):
		return SQL.MakeListOrder(self.identifier)

	def pop(self, index):
		self.__check_loaded__()	
		item = self.items[index]
		self.remove(item)

	def __erase__(self, item):
		"""
		Remove an item from a List when it's already been deleted. 
		Ensures that the item is not added to the DB again.
		"""
		assert(type(item) == self.child_dc)
		if self.__is_loaded__():
			if item in self.items:
				id_key = self.id_key
				order_key = self.order_key

				list_index = self.items.index(item)

				# Knock the item off the list
				prev_dict = item.__dict__
				prev_dict[id_key] = NULL_INT
				cur_order = prev_dict[order_key]
				prev_dict[order_key] = NULL_INT

				# Move all items above it down one
				last_index = len(self.items) - 1
				for i in range( list_index, last_index ):
					move_item = self.items[i+1]
					move_item[order_key] = cur_order
					self.items[i] = move_item
					cur_order += 1
				
				self.items.pop(last_index)

				# remove it from former items to update
				if item in self.former_items:
					self.former_items.pop(item)

	def remove(self, item):
		assert(type(item) == self.child_dc)
		self.__check_loaded__()
		id_key = self.id_key
		order_key = self.order_key

		# Knock the item off the list
		list_index = self.items.index(item)

		prev_dict = item.__dict__
		prev_dict[id_key] = NULL_INT
		cur_order = prev_dict[order_key]
		prev_dict[order_key] = NULL_INT
	
		# Move all items above it down one.
		# Also resets the order keys
		last_index = len(self.items) - 1
		for i in range( list_index, last_index ):
			move_item = self.items[i+1]
			move_item[order_key] = cur_order
			self.items[i] = move_item
			cur_order += 1

		self.items.pop(last_index)

		# Add it to the list of former items to update later
		if item not in self.former_items:
			self.former_items.append(item)
		
	def __setitem__(self, index, value):
		assert(type(value) == self.child_dc)
		self.__check_loaded__()

		# Remove any old item that was stored here
		assert(index < len(self.items))
		old_item = self.items[index]
		old_dict = value.__dict__
		order = old_dict[self.order_key]
		self.remove(old_item)

		# Set the new value
		assert(value not in self.items)
		new_dict = value.__dict__
		if hasattr(self.parent, "dbid"):
			new_dict[self.id_key] = self.parent.dbid
		# Set order number -- not necessarily index
		new_dict[self.order_key] = order
		self.items[index] = value

	def __contains__(self, value:object) -> bool:
		assert(type(value) == self.child_dc)
		self.__check_loaded__()
		return value in self.items

	def __getitem__(self, index) -> object:
		self.__check_loaded__()
		
		assert( index < len(self.items) )
		return self.items[index]

	def append(self, item):
		assert(type(item) == self.child_dc)
		self.__check_loaded__()

		order_key = self.order_key

		assert(item not in self.items)
		new_dict = item.__dict__
		if hasattr(self.parent, "dbid"):
			new_dict[self.id_key] = self.parent.dbid
		
		# Set the order number 
		if len(self.items) == 0:
			new_dict[order_key] = 0
		else:
			# Set order from last item
			last_dict = self.items[len(self.items)-1].__dict__
			last_order = last_dict[order_key]
			new_dict[order_key] = last_order + 1

		self.items.append(item)

	def __iter__(self):
		self.__check_loaded__()
		return self.items.__iter__()

	def __set__(self, obj_list):
		t = type(obj_list)
		assert(t == list or t == tuple or get_origin(t) == List)
		self.__check_loaded__()

		id_key = self.id_key
		order_key = self.order_key

		# clear previous table if needed
		for i in range( len(self.items) ):
			item = self.items[i]
			
			# Knock the item off the list
			prev_dict = item.__dict__
			prev_dict[id_key] = NULL_INT
			prev_dict[order_key] = NULL_INT
			if item not in obj_list:
				self.former_items.append(item)
			self.items.pop(i)
		
		assert(len(self.items) == 0)
		for obj in obj_list:
			self.append(obj)

	# NOTE: Oddly, updating with sql.UpdateList (executemany) 
	# is slower than updating with sql.Update (execute).

	def __add_to_db__(self):
		if not self.from_db:
			self.__validate_parent__()

			id_key = self.id_key
			sql = SQL.Get()

			for item in self.items:
				item.__dict__[id_key] = self.parent.dbid
				sql.Update(item, commit=False)
			#sql.UpdateList(self.items, commit=False)
			
			self.from_db = True
			self.initialized = True
		else:
			self.__update_to_db__()

	def __update_to_db__(self):
		if self.initialized:
			self.__validate_parent__()

			id_key = self.id_key
			sql = SQL.Get()

			# Update any items that were removed from this list
			for item in self.former_items:
				sql.Update(item, commit=False)
			#sql.UpdateList(self.former_items, commit=False)
			self.former_items = []
			
			# Update all items that are now in the list
			# TODO: could optimize by what's actually been touched
			for item in self.items:
				item.__dict__[id_key] = self.parent.dbid
				sql.Update(item, commit=False)
			#sql.UpdateList(self.items, commit=False)

			self.from_db = True
	
	def __delete_from_db__(self):
		self.__check_loaded__()
		id_key = self.id_key
		sql = SQL.Get()

		# Update any items that were removed from this list
		for item in self.former_items:
			sql.Update(item, commit=False)
		#sql.UpdateList(self.former_items, commit=False)
		self.former_items = []
		
		# Unlink all items that are now in the list
		for item in self.items:
			item_dict = item.__dict__
			item_dict[self.id_key] = NULL_INT
			item_dict[self.order_key] = NULL_INT
			sql.Update(item, commit=False)
		#sql.UpdateList(self.items, commit=False)
		self.items = []

		self.from_db = True