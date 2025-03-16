import parsecsv,re,strutils,times  # используйте для чтения ваших csv файлов
import db_connector/db_sqlite  # или norm/[model, sqlite]
import shelter/[functions],shelter/[types]


# Реализуйте функции чтения и преобразования csv записи
# в соответствующий объект или модель.

# Создайте таблицы в базе данных.
# Реализуйте загрузку экземпляра объекта в соответствующую таблицу.

proc rowToManager(row: seq[string]): Manager =
  var isGlavn = false
  var dolzn = row[3].split()[0]
  if dolzn == "Главный":
    isGlavn = true
    dolzn = row[3].split()[1]
  initManager(a,row[0],row[1],dolzn,row[2],isGlavn)
  return a

proc readManagers(file: string): seq[Manager] =
  var parser: CsvParser
  var res: seq[Manager]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToManager(parser.row))
  parser.close
  return res

proc rowToPet(row: seq[string]): Pet =
  initPet(a,row[0],row[1].parseInt())
  return a

proc readPets(file: string): seq[Pet] =
  var parser: CsvParser
  var res: seq[Pet]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToPet(parser.row))
  parser.close
  return res

proc rowToStaff(row: seq[string]): Staff =
  initStaff(a,row[0],row[1],row[2],row[3].parseInt)
  return a

proc readStaff(file: string): seq[Staff] =
  var parser: CsvParser
  var res: seq[Staff]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToStaff(parser.row))
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
  let db = open("shelter.db", "", "", "")
  let manager = readManagers("data/shelter_managers.csv")
  db.createTable(
    "Manager",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate VARCHAR(12)",
    "post VARCHAR(12)"
    )
  insertAll(db,"Manager","firstname, lastname, birthdate, post",manager)
  let pets = readPets("data/shelter_pets.csv")
  db.createTable(
    "Pet",
    "name VARCHAR(25)",
    "age UNSIGNED INTEGER"
    )
  insertAll(db,"Pet","name, age",pets)
  let staff = readStaff("data/shelter_staff.csv")
  db.createTable(
    "Staff",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate VARCHAR(12)",
    "uid UNSIGNED INTEGER"
    )
  insertAll(db,"Staff","firstname, lastname, birthdate, uid",staff)
  db.close()