import parsecsv,re,strutils,times  # используйте для чтения ваших csv файлов
import db_connector/db_sqlite  # или norm/[model, sqlite]
import school/[functions],school/[types]


  
# Реализуйте функции чтения и преобразования csv записи
# в соответствующий объект или модель.

# Создайте таблицы в базе данных.
# Реализуйте загрузку экземпляра объекта в соответствующую таблицу.

proc rowToDirector(row: seq[string]): Director =
  initDirector(a,row[0],row[1],row[2])
  return a

proc readDirectors(file: string): seq[Director] =
  var parser: CsvParser
  var res: seq[Director]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToDirector(parser.row))
  parser.close
  return res

proc rowToStudent(row: seq[string]): Student =
  initStudent(a,row[0],row[1],row[2],@row[4][0],row[3].parseInt)
  return a

proc readStudents(file: string): seq[Student] =
  var parser: CsvParser
  var res: seq[Student]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToStudent(parser.row))
  parser.close
  return res

proc rowToTeacher(row: seq[string]): Teacher =
  initTeacher(a,row[0],row[1],row[3],row[2])
  return a

proc readTeachers(file: string): seq[Teacher] =
  var parser: CsvParser
  var res: seq[Teacher]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToTeacher(parser.row))
  parser.close
  return res

proc createTable(db: DbConn, tableName: string,fields: varargs[string]) =
  let query = """
  CREATE TABLE IF NOT EXISTS $1 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    $2
  )""" % [tableName,fields.join(",")]
  db.exec(sql(query))

proc insert(db: DbConn,tableName,fields: string,values: varargs[string,`$`]): int64 = 
  let query = "INSERT INTO $1 ($2) VALUES $3" % [tableName,fields,values.join(",")]
  db.tryInsertID(sql(query))

template insertAll(db: DbConn,tableName,fields: string, data: seq[auto]) =
  for el in data:
    if db.insert(tableName,fields,el) == -1:
      echo "Error:",el
  

when isMainModule:
  let db = open("school.db", "", "", "")
  let director = readDirectors("data/school_direcor.csv")
  db.createTable(
    "Director",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate VARCHAR(12)"
    )
  insertAll(db,"Director","firstname, lastname, birthdate",director)
  let students = readStudents("data/school_students.csv")
  db.createTable(
    "Student",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "classnum UNSIGNED INTEHER",
    "classlet VARCHAR(1)",
    "birthdate VARCHAR(12)"
    )
  insertAll(db,"Student","firstname, lastname, classnum, classlet, birthdate",students)
  let teachers = readTeachers("data/school_teachers.csv")
  db.createTable(
    "Teacher",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "subject VARCHAR(25)",
    "birthdate VARCHAR(12)"
    )
  insertAll(db,"Teacher","firstname, lastname, subject, birthdate",teachers)
  db.close()