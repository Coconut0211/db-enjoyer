import parsecsv,strutils,times,sequtils,logging
import db_connector/db_sqlite 
import time_logger

type
  Role* = enum
    NONE, Директор, Бухгалтер, Ветеринар
  Post* = object
    dol*: Role
    glavn*: bool
  Person* = ref object of RootObj
    firstname*: string
    lastname*: string
    birthDate*: int64
  Manager* = ref object of Person
    post*: Post
  Staff* = ref object of Person
    uid*: int
  Pet* = ref object of RootObj
    name*: string
    age*: int 
  Shelter* = ref object of RootObj
    staff*: int
    pet*: int
    manager*: int

proc `$`*(self: Manager): string = 
  var glavn = 0
  if self.post.glavn:
    glavn = 1
  "('$1', '$2', $3, '$4', $5)" % [
  self.firstname,
  self.lastname,
  $self.birthDate,
  $self.post.dol,
  $glavn
  ]

proc `$`*(self: Staff): string = 
  "('$1', '$2', $3, $4)" % [
  self.firstname,
  self.lastname,
  $self.birthDate,
  $self.uid,
  ]

proc `$`*(self: Pet): string =
  return "('$1', $2)" % [
  self.name,
  $self.age,
  ]

proc `$`*(self: Shelter): string =
  "($1, $2, $3)" % [
    $self.staff,
    $self.pet,
    $self.manager
  ]

proc toUnix(date: string): int64 =
  try:
    return date.parse("dd'.'MM'.'YYYY").toTime.toUnix
  except TimeParseError:
    stderr.write(getCurrentExceptionMsg() & "\n")
    return result

proc rowToManager(row: seq[string]): Manager =
  var isGlavn = false
  var dolzn = row[3].split()[0]
  if dolzn == "Главный":
    isGlavn = true
    dolzn = row[3].split()[1]
  Manager(firstname: row[0],lastname: row[1], birthdate: toUnix(row[2]), post: Post(dol: parseEnum[Role](dolzn),glavn: isGlavn))

proc readManagers(file: string): seq[Manager] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToManager(parser.row))
  parser.close()
  return result

proc rowToPet(row: seq[string]): Pet =
  Pet(name: row[0],age: row[1].parseInt())

proc readPets(file: string): seq[Pet] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToPet(parser.row))
  parser.close()
  return result

proc rowToStaff(row: seq[string]): Staff =
  Staff(firstname: row[0],lastname: row[1],birthdate: toUnix(row[2]),uid: row[3].parseInt)

proc readStaff(file: string): seq[Staff] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToStaff(parser.row))
  parser.close()
  return result

proc createTable(db: DbConn, tableName: string,fields: varargs[string]) =
  let query = """
  CREATE TABLE IF NOT EXISTS $1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    $2
  )""" % [tableName,fields.join(",")]
  db.exec(sql(query))

proc insert(db: DbConn,logger: TimedRollingFileHandler, tableName,fields: string,values: seq): int = 
  let query = "INSERT INTO $1 ($2) VALUES $3" % [tableName,fields,values.mapIt($it).join(",")]
  let flag =  db.tryInsertID(sql(query))
  if flag > 0:
    logger.log(lvlInfo,"""Successfully added $1 lines in "$2" table""" % [$len(values),tableName])
    return len(values)
  else:
    logger.log(lvlWarn,"""Failed to add $1 lines in "$2" table""" % [$len(values),tableName])
    return 0

when isMainModule:
  let db = open("shelter.db", "", "", "")
  var shelter = Shelter(staff: 0,pet: 0, manager: 0)
  var logger = newTimedRotatingFileHandler(
    filePath= "logs/app_shelter.log",
    whenInterval='M',
    interval=1,
    backupCount=3,
    fmtStr="[$date $time][$levelname] "
  )
  db.createTable(
    "Manager",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "post VARCHAR(12)",
    "glavn BOOLEAN"
    )
  shelter.manager += db.insert(logger,"Manager","firstname, lastname, birthdate, post, glavn",readManagers("data/shelter_managers.csv"))
  db.createTable(
    "Pet",
    "name VARCHAR(25)",
    "age UNSIGNED INTEGER"
    )
  shelter.pet += db.insert(logger,"Pet","name, age",readPets("data/shelter_pets.csv"))
  db.createTable(
    "Staff",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "uid UNSIGNED INTEGER"
    )
  shelter.staff += db.insert(logger,"Staff","firstname, lastname, birthdate, uid",readStaff("data/shelter_staff.csv"))
  db.createTable(
    "Shelter",
    "staffnumber UNSIGNED INTEGER",
    "petsnumber UNSIGNED INTEGER",
    "managersnumber UNSIGNED INTEGER",
    )
  let k = db.insert(logger,"Shelter","staffnumber, petsnumber, managersnumber",@[shelter])
  db.close()