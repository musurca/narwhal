from typing import get_origin, get_args

from .sql import SQL
from .relations import Reference, List

class DBObject:
	SPECIAL_TYPES 	= [ Reference, List ]
	INIT_TABLE = {
		"Reference" 		: lambda t : Reference[t](),
		"List"				: lambda t : List[t]()
	}

	def __initialize_relations__(self):
		# preinitialize our special types Reference and List,
		# and set their underlying datatypes from Generic
		for item_name, item_type in self.__annotations__.items():
			origin = get_origin(item_type)
			if origin in DBObject.SPECIAL_TYPES:
				child_dc = get_args(item_type)[0]
				self.__dict__[item_name] = DBObject.INIT_TABLE[origin.__name__](child_dc)
				if origin == List:
					self.__dict__[item_name].set_parent_child(self, child_dc, item_name)
				else: # Reference
					self.__dict__[item_name].set_childtype(child_dc)

	def __init__(self):
		self.__initialize_relations__()

	def __get_lists__(self):
		lists = []
		for item_name, item_type in self.__annotations__.items():
			origin = get_origin(item_type)
			if origin == List:
				lists.append(self.__dict__[item_name])
		return lists

	def __mark_lists_from_db__(self):
		for item_name, item_type in self.__annotations__.items():
			origin = get_origin(item_type)
			if origin == List:
				self.__dict__[item_name].__mark_from_db__()

	def __getattribute__(self, name: str):
		# make sure reference accesses get the reference 
		annotations = object.__getattribute__(self, "__annotations__")
		if name in annotations:
			item_type = annotations[name]
			origin = get_origin(item_type)
			if origin == Reference:
				# return object of reference
				idict =  object.__getattribute__(self, "__dict__")
				return idict[name].__get__()
		return object.__getattribute__(self, name)

	def __setattr__(self, name: str, value):
		# make sure reference setters set the reference 
		annotations = object.__getattribute__(self, "__annotations__")
		#annotations = self.__annotations__
		if name in annotations:
			item_type = annotations[name]
			origin = get_origin(item_type)
			if origin in DBObject.SPECIAL_TYPES:
				# calls special set function for both reference & list
				idict =  object.__getattribute__(self, "__dict__")
				idict[name].__set__(value)
				return
		super().__setattr__(name, value)
	
	def __get_reference__(self, name:str):
		# for db use, when we need to access the underlying reference
		annotations = object.__getattribute__(self, "__annotations__")
		if name in annotations:
			item_type = annotations[name]
			origin = get_origin(item_type)
			if origin == Reference:
				return object.__getattribute__(self, name)
		return None
	
	def __set_reference__(self, name:str, value):
		# if, for some reason, we need to set the underlying reference
		annotations = object.__getattribute__(self, "__annotations__")
		if name in annotations:
			item_type = annotations[name]
			origin = get_origin(item_type)
			if origin == Reference:
				idict =  object.__getattribute__(self, "__dict__")
				idict[name] = value

	def Serialize(self, force_update=False):
		sql = SQL.Get()
		idict = object.__getattribute__(self, "__dict__")
		if "dbid" in idict.keys():
			sql.Update(self, force_update)
		else:
			sql.Add(self)
	
	def Delete(self, force_remove=False):
		sql = SQL.Get()
		idict = object.__getattribute__(self, "__dict__")
		if "dbid" in idict.keys():
			sql.Delete(self, force_remove=force_remove)

	def __eq__(self, obj) -> bool:
		return (type(self) == type(obj)) and (self.dbid == obj.dbid)

	@classmethod
	def Select(cls, args:tuple, orderby="") -> list:
		return SQL.Get().Select(cls, args, orderby)

	@classmethod
	def SelectOne(cls, args:tuple) -> object:
		return SQL.Get().SelectOne(cls, args)

	@classmethod
	def SelectAll(cls) -> list:
		return SQL.Get().SelectAll(cls)

	@classmethod
	def SelectAtIndex(cls, index:int) -> object:
		return SQL.Get().SelectAtIndex(cls, index)

	@classmethod
	def SelectRandom(cls, num=1):
		return SQL.Get().SelectRandom(cls, num)

	@classmethod
	def Count(cls, args:tuple) -> int:
		return SQL.Get().Count(cls, args)

	@classmethod
	def __len__(cls) -> int:
		return SQL.Get().TableLength(cls)



class Mutable(DBObject):
	__immutable__ = False


class Immutable(DBObject):
	__immutable__ = True
