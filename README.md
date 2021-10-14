# Narwhal

A Python sqlite3 interface & ORM, for enterprise JFA (just-fucking-around) programming.

The Narwhal ORM is designed to perform object-relational mapping with a minimum amount of explicit instruction, allowing you to focus on more pressing JFA tasks.

Narwhal supports foreign key relationships between tables, including both one-to-one and one-to-many.

## Overview

The following simple class is a perfectly valid instruction for the Narwhal ORM to produce a corresponding table and columns. If the database does not already exist, Narwhal will create all registered tables.

```python
from narwhal.sql import SQL
from narwhal.db import Mutable

class Vessel(Mutable):
	name: str
	year_built: int
	nation: str
	heading: int
	speed: float

sql = SQL("test.db")
sql.RegisterTables((Vessel,))
```

To add a new row to the table, simply instantiate the class and call the `Serialize` method.

```python
v = Vessel()
v.name = "Constitution"
v.year_built = 1797
v.Serialize()

# Or, to efficently add a large list of rows:
v_list = []
for i in range( 1000 ):
	v = Vessel()
	v_list.append(v)
SQL.Get().AddList(v_list)
```

To remove an item from the table, use the `Delete` method.

```python
v.Delete()
```

Data can be retrieved from the database by constructing a query.

```python
from narwhal.sql import Query

vessel_list = Vessel.Select(
	Query.And(
		Query.Equals("name", "Constitution"),
		Query.GreaterThan("year_built", 1700)
	)
)
if len(vessel_list) > 0:
	print(v[0].name) # Constitution
```

Tables can either be `Mutable` or `Immutable`. Rows in `Immutable` tables are "write-protected" and cannot be updated after being added without an explicit instruction.

```python
from narwhal.db import Immutable

class VesselClass(Immutable):
	length: float
	beam: float
	displacement: int
	masts: int			
	max_speed: float

# This will successfully add a new VesselClass
v = VesselClass()
v.masts = 3
v.Serialize()

# This will fail, as VesselClass is immutable
v.masts = 2
v.Serialize()

# This will ignore VesselClass's immutability
# and successfully update it
v.Serialize(force_update=True)
```

To express a one-to-one foreign key relation, you can use the `Reference` type. The value of a `Reference` will be deferred, i.e. only retrieved from the database when explicitly requested.

```python
from narwhal.relations import Reference

class Vessel(Mutable):
	vessel_class: Reference[VesselClass]
	...

v = Vessel.SelectOne(
	Query.And(
		Query.Equals("name", "Constitution"),
		Query.GreaterThan("year_built", 1700)
	)
)

if v.vessel_class.masts == 3:
	# The value of vessel_class is fetched from the DB 
	# invisibly
	print("It's a ship!")
```

To express a one-to-many foreign key relationship, you can use the `List` type. `List` behaves just like a Python `list`, but its underlying values are only retrieved from the database when explicitly requested.

```python
from narwhal.relations import List

class Crew(Mutable):
	first_name: str
	last_name: str
	age: str
	able_seaman: bool

class Vessel(Mutable):
	crew: List[Crew]
	...

for man in v.crew:
	# crew is invisibly fetched from the DB
	if man.age > 65:
		print(f"{man.first_name} is too old!")
		v.crew.remove(man)

# Changes to crew are deferred until 
# the parent is serialized
v.Serialize()
```

You can also register custom atomic datatypes beyond the standard Python ones. Their value should be converted into a sqlite-ready type (either a `str`, `int`, or `float`).

```python
class Position:
	pos : list

	def __init__(self):
		self.pos = [0, 0]

	def FromTuple(t):
		p = Position()
		p.pos[0] = t[0]
		p.pos[1] = t[1]
		return p

	def __setitem__(self, index, value: float):
		assert(index < 2)
		self.pos[index] = value

	def __getitem__(self, index) -> float:
		assert(index < 2)
		return self.pos[index]

# Convert the position to/from a semi-colon separated string
SQL.RegisterTypeConversion(
		Position,
		adapter 	= lambda p : f"{p[0]};{p[1]}".encode("ascii"),
		converter 	= lambda p : Position.FromTuple( tuple(map(float, p.split(b";"))) ),
		default 	= Position()
	)
```

## Advanced Use

### Caching

You can cache the results of queries with the `use_cache` argument when creating the SQL connection.

```python
sql = SQL("test.db", use_cache=True)

# Makes a query, and stores the result in the cache
v = Vessel.Select(
	Query.And(
		Query.Equals("name", "Constitution"),
		Query.GreaterThan("year_built", 1700)
	)
)

# The cached result from before will be returned
# instead of hitting the database
v = Vessel.Select(
	Query.And(
		Query.GreaterThan("year_built", 1700),
		Query.Equals("name", "Constitution")
	)
)
```

### Shared Memory

You can store all results from the database in a shared memory space, ensuring that you'll never have more than one copy of a row in local memory. If you store results from previous queries, those results will automatically be updated as new matching queries come in.

This will also slowly build up a local copy of the database as you continue to query it.

(NOTE: shared memory is automatically activated if you're using caching.)

```python
sql = SQL("test.db", use_sharedmemory=True)
```