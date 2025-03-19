import parsecsv,re,strutils,times,sequtils  # используйте для чтения ваших csv файлов
import db_connector/db_sqlite  # или norm/[model, sqlite]


  
# Реализуйте функции чтения и преобразования csv записи
# в соответствующий объект или модель.

# Создайте таблицы в базе данных.
# Реализуйте загрузку экземпляра объекта в соответствующую таблицу.

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
    director*: Director
    students*: seq[Student]
    teachers*: seq[Teacher]

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

proc insert(db: DbConn,tableName,fields: string,values: seq): int64 = 
  let query = "INSERT INTO $1 ($2) VALUES $3" % [tableName,fields,values.mapIt($it).join(",")]
  db.tryInsertID(sql(query))


when isMainModule:
  let db = open("school.db", "", "", "")
  db.createTable(
    "Director",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER"
    )
  echo db.insert("Director","firstname, lastname, birthdate",readDirectors("data/school_direcor.csv"))
  db.createTable(
    "Student",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "classnum UNSIGNED INTEGER",
    "classlet VARCHAR(1)"
    )
  echo db.insert("Student","firstname, lastname, birthdate, classnum, classlet",readStudents("data/school_students.csv"))
  db.createTable(
    "Teacher",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "subject VARCHAR(25)",
    )
  echo db.insert("Teacher","firstname, lastname, birthdate, subject",readTeachers("data/school_teachers.csv"))
  db.close()