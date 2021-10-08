# Narwhal

A sqlite3 interface and ORM for Python, for enterprise JFA (just-fucking-around) programming.

The Narwhal ORM is designed to perform object-relational mapping with a minimum amount of explicit instruction, allowing you to focus on more pressing JFA tasks.

Narwhal supports foreign key relationships between tables, including both one-to-one and one-to-many.

## Overview

The following simple class is a perfectly valid instruction for the Narwhal ORM to produce a corresponding table and columns:

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
sql.CreateTables()
```

To add a new row to the table:

```python
v = Vessel()
v.name = "Constitution"
v.year_built = 1797
v.Serialize()
```

To retrieve data from the table:

```python
from narwhal.sql import Query

v = SQL.Get().SelectOne(
	Vessel,
	Query.And(
		Query.Equals("name", "Constitution"),
		Query.GreaterThan("year_built", 1700)
	)
)
print(v.name) # Constitution
```

For a one-to-one foreign key, use the Reference type:

```python
from narwhal.db import Immutable
from narwhal.relations import Reference

class VesselClass(Immutable):
	length: float
	beam: float
	displacement: int
	masts: int			
	max_speed: float

class Vessel(Mutable):
	vessel_class: Reference[VesselClass]
	...

if v.vessel_class.masts == 3:
	print("It's a ship!")
```

For a one-to-many foreign key, use the List type:

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
	if man.age > 65:
		print(f"{man.first_name} is too old!")
		v.crew.remove(man)

# save changes to the crew
v.Serialize()
```

To register a custom data type:

```python
class Position:
	pos : list

	def __init__(self):
		self.pos = [0, 0]

# Convert the position to/from a semi-colon separated string
SQL.RegisterTypeConversion(
		Position,
		adapter 	= lambda p : f"{p[0]};{p[1]}".encode("ascii"),
		converter 	= lambda p : Position.FromTuple( tuple(map(float, p.split(b";"))) ),
		default 	= Position()
	)
```