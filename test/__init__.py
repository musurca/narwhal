from datetime import date

from narwhal.db import Mutable, Immutable
from narwhal.relations import Reference, List

class Position:
	pos : list

	SQLAdapter 		= lambda p : f"{p[0]};{p[1]}".encode("ascii")
	SQLConverter	= lambda p : Position.FromTuple( tuple(map(float, p.split(b";"))) )

	def FromTuple(t):
		p = Position()
		p.pos[0] = t[0]
		p.pos[1] = t[1]
		return p

	def FromGIS(pt):
		p = Position()
		p.pos[0] = pt[1]
		p.pos[1] = pt[0]
		return p

	def __init__(self):
		self.pos = [0, 0]

	def __setitem__(self, index, value : float):
		assert(index < 2)
		self.pos[index] = value

	def __getitem__(self, index) -> float:
		assert(index < 2)
		return self.pos[index]

	def Latitude(self) -> float:
		return self.pos[0]
	
	def Longitude(self) -> float:
		return self.pos[1]

	def Set(self, lat, lng):
		self.pos[0] = lat
		self.pos[1] = lng

	def GISPoint(self) -> tuple:
		return ( self.pos[1], self.pos[0] )

class FloatArray:
	array 	: tuple
	length 	: int

	def SQLAdapter(p):
		txt = ""
		for i in range( p.length - 1 ):
			txt = txt+f"{p[i]};"
		txt = txt+f"{p[p.length-1]}"
		return txt.encode("ascii")

	SQLConverter = lambda p : FloatArray.FromTuple( tuple(map(float, p.split(b";"))) )

	def FromTuple(t):
		size_t = len(t)
		p = FloatArray(size_t)
		p.array = t
		return p

	def __init__(self, length:int):
		self.array = (0.0,)*length
		self.length = length
	
	def __setitem__(self, index, value : float):
		assert(index < self.length)
		self.array[index] = value

	def __getitem__(self, index) -> float:
		assert(index < self.length)
		return self.array[index]

	def __len__(self) -> int:
		return self.length

class VesselClass(Immutable):
	name			: str
	length 			: float	# ft
	beam   			: float	# ft
	displacement 	: int	# short tons
	masts  			: int			
	points_of_sail 	: FloatArray
	max_speed 		: float # knots
	decks			: int
	has_quarterdeck : bool
	max_crew		: int

	def __init__(self):
		# initialize points of sail
		self.points_of_sail = FloatArray(32)

class HistoryString(Immutable):
	line_txt 		: str
	date			: date

class Crew(Mutable):
	name 			: str		# name of person
	rank			: str		# rank (should probably be int)
	nation			: int
	history			: List[HistoryString]	# 1:Many of date/text string pairs
	health			: int
	courage			: int
	strength		: int
	intelligence	: int
	seamanship		: int
	charisma		: int
	reliability		: int
	experience		: int
	leadership		: int
	political		: int		# "interest", essentially
	ambition		: int
	wealth			: int

class Vessel(Mutable):
	v_class			: Reference[VesselClass] # 1:1 relation 
	name			: str
	year			: int		 # year built
	nation			: int		 # nation currently operating
	position_actual	: Position 	 # actual lat-lng position
	position_known	: Position 	 # where the vessel's captain thinks he is
	heading			: int 	   	 # 0-360 (or maybe point of sail?)
	speed  			: float		 # knots
	crew            : List[Crew] # 1:Many relation 