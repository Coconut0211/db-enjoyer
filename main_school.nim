import parsecsv,strutils,times,sequtils,logging
import db_connector/db_sqlite
import time_logger

type
  Subjects* = enum
    NONE, История, География, Математика, Биология
  Person* = ref object of RootObj
    firstname*: string
    lastname*: string
    birthDate*: int64
  Director* = ref object of Person
  Teacher* = ref object of Person
    subject*: Subjects
  Student* = ref object of Person
    classNum*: int
    classLet*: char
  School* = ref object of RootObj
    director*: int
    students*: int
    teachers*: int

proc `$`*(self: Director): string = 
  "('$1', '$2', $3)" % [
  self.firstname,
  self.lastname,
  $self.birthDate
  ]

proc `$`*(self: Teacher): string = 
  "('$1', '$2', $3, '$4')" % [
  self.firstname,
  self.lastname,
  $self.birthDate,
  $self.subject,
  ]

proc `$`*(self: Student): string = 
  "('$1', '$2', $3, $4, '$5')" % [
  self.firstname,
  self.lastname,
  $self.birthDate,
  $self.classNum,
  $self.classLet,
  ]

proc `$`*(self: School): string =
  "($1, $2, $3)" % [
    $self.director,
    $self.students,
    $self.teachers
  ]

proc toUnix(date: string): int64 =
  try:
    return date.parse("dd'.'MM'.'YYYY").toTime.toUnix
  except TimeParseError:
    stderr.write(getCurrentExceptionMsg() & "\n")
    return result

proc rowToDirector(row: seq[string]): Director =
  Director(firstname: row[0],lastname: row[1],birthdate: toUnix(row[2]))

proc readDirectors(file: string): seq[Director] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToDirector(parser.row))
  parser.close()
  return result

proc rowToStudent(row: seq[string]): Student =
  Student(firstname: row[0],lastname: row[1],birthdate: toUnix(row[2]) ,classLet: @row[4][0],classNum: row[3].parseInt)

proc readStudents(file: string): seq[Student] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToStudent(parser.row))
  parser.close()
  return result

proc rowToTeacher(row: seq[string]): Teacher =
  Teacher(firstname: row[0],lastname: row[1],birthdate: toUnix(row[2]), subject: parseEnum[Subjects](row[3]))

proc readTeachers(file: string): seq[Teacher] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToTeacher(parser.row))
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
  let db = open("school.db", "", "", "")
  var school = School(director: 0,students: 0, teachers: 0)
  var logger = newTimedRotatingFileHandler(
    filePath= "logs/app_school.log",
    whenInterval='M',
    interval=1,
    backupCount=3,
    fmtStr="[$date $time][$levelname] "
  )
  db.createTable(
    "Director",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER"
    )
  school.director += db.insert(logger,"Director","firstname, lastname, birthdate",readDirectors("data/school_direcor.csv"))
  db.createTable(
    "Student",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "classnum UNSIGNED INTEGER",
    "classlet VARCHAR(1)"
    )
  school.students += db.insert(logger,"Student","firstname, lastname, birthdate, classnum, classlet",readStudents("data/school_students.csv"))
  db.createTable(
    "Teacher",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "subject VARCHAR(25)",
    )
  school.teachers += db.insert(logger,"Teacher","firstname, lastname, birthdate, subject",readTeachers("data/school_teachers.csv"))
  db.createTable(
    "School",
    "directornumber UNSIGNED INTEGER",
    "studentnumber UNSIGNED INTEGER",
    "teachernumber UNSIGNED INTEGER",
    )
  let k = db.insert(logger,"School","directornumber, studentnumber, teachernumber",@[school])
  db.close()