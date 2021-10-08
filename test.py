from random import choice

from narwhal.sql import SQL, Query
from narwhal.relations import List

from test import *

if __name__ == '__main__':
	SQL.RegisterTypeConversion(
		Position,
		adapter 	= Position.SQLAdapter,
		converter 	= Position.SQLConverter,
		default 	= Position()
	)

	SQL.RegisterTypeConversion(
		FloatArray,
		adapter 	= FloatArray.SQLAdapter,
		converter 	= FloatArray.SQLConverter,
		default 	= FloatArray(32)
	)

	sql = SQL("test.db")
	sql.RegisterTables([
		Crew,
		VesselClass,
		Vessel,
		HistoryString
	])
	sql.CreateTables()

	clist = []
	for i in range(1000):
		c = Crew()
		c.name = "asdasdafas"
		clist.append( c )
	# adds a list of elements
	sql.AddList(clist) 
	
	c = Crew()
	c.name = "dfasodasd"
	c.courage = 232
	# adds a single element
	sql.Add(c)

	vc = VesselClass()
	vc.name = "Bellona-class"
	# You can also add/update using the Serialize method
	vc.Serialize()

	v = Vessel()
	v.name = "Bellona"
	v.v_class = vc
	crewlist = []
	for i in range(300):
		c = choice(clist)
		v.crew.append(c)
		clist.remove(c)
	v.Serialize()

	c = sql.SelectOne(
		Crew,
		Query.Equals("name", "dfasodasd")
	)
	if c != None:
		# Change the crew member's name,
		# and save him to the db
		c.name = "update_successful"
		c.Serialize()

	c = sql.SelectOne(
		Crew,
		Query.And(
			Query.LessThanEquals("health", 0),
			Query.Equals("courage", 232)
		)
	)

	v = sql.SelectOne(
		Vessel,
		Query.Equals("name", "Bellona")
	)

	if c != None:
		# "update_successful"
		print(c.name)
		# Bellona
		print(v.name)
		# the 121st crewmember's name
		print(v.crew[120].name)
		# Bellona-class (the vessel's class)
		print(v.v_class.name)
		# Bellona (the name of the vessel on which the 121st crew member serves)
		print(List[Crew].ReverseLookup(v.crew[120], Vessel, "crew").name)
		# size of data stored in memory from the db
		print(f"Size of DB data stored in memory: {sql.CacheSize()} bytes")