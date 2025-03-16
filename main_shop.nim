import parsecsv,re,strutils,times  # используйте для чтения ваших csv файлов
import db_connector/db_sqlite  # или norm/[model, sqlite]
import shop/[functions],shop/[types]


# Реализуйте функции чтения и преобразования csv записи
# в соответствующий объект или модель.

# Создайте таблицы в базе данных.
# Реализуйте загрузку экземпляра объекта в соответствующую таблицу.

proc rowToStaff(row: seq[string]): Staff =
  initStaff(a,row[0],row[1],row[2],row[3])
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

proc rowToGood(row: seq[string],title: string): Good =
  initGood(a,title,row[1].parseFloat,row[2],row[3].parseFloat / 100,row[4].parseInt)
  return a

proc readGoods(file: string): seq[Good] =
  var parser: CsvParser
  var res: seq[Good]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToGood(parser.row,parser.rowEntry("title")))
  parser.close
  return res

proc rowToCash(row: seq[string]): Cash =
  initCash(a,row[0].parseInt,row[1].parseBool,row[2].parseFloat)
  return a

proc readCash(file: string): seq[Cash] =
  var parser: CsvParser
  var res: seq[Cash]
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow:
    res.add(rowToCash(parser.row))
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
  let db = open("shop.db", "", "", "")
  let staff = readStaff("data/shop_staff.csv")
  db.createTable(
    "Staff",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate VARCHAR(12)",
    "post VARCHAR(25)"
    )
  insertAll(db,"Staff","firstname, lastname, birthdate,post",staff)
  let goods = readGoods("data/shop_goods.csv")
  db.createTable(
    "Good",
    "title VARCHAR(80)",
    "price UNSIGNED FLOAT",
    "enddate VARCHAR(25)",
    "discount UNSIGNED FLOAT",
    "count UNSIGNED INTEGER"
    )
  insertAll(db,"Good","title, price, enddate, discount, count",goods)
  let cashes = readCash("data/shop_cashes.csv")
  db.createTable(
    "Cash",
    "number UNSIGNED INTEGER",
    "free BOOLEAN",
    "totalcash UNSIGNED FLOAT",
    )
  insertAll(db,"Cash","number, free, totalcash",cashes)
  db.close()