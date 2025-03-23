import parsecsv,strutils,times,sequtils,logging 
import db_connector/db_sqlite  
import time_logger

type
  Post* = enum
    NONE, Кассир, Уборщик, Консультант, Менеджер, Директор

  Staff* = ref object of RootObj
    firstName*: string
    lastName*: string
    birthDate*: int64
    post*: Post

  Good* = ref object of RootObj
    title*: string
    price*: float
    endDate*: int64
    discount*: float
    count*: int64

  Cash* = ref object of RootObj
    number*: int
    free*: bool
    totalCash*: float

  Shop* = ref object of RootObj
    staff*: int64
    goods*: int64
    cashes*: int64

proc `$`*(self: Staff): string =
  "('$1', '$2', $3, '$4')" % [
    self.firstName,
    self.lastName,
    $self.birthDate,
    $self.post,
  ]

proc `$`*(self: Good): string =
  "('$1', $2, $3, $4, $5)" % [
    self.title.replace("'","''"),
    $self.price,
    $self.endDate,
    $self.discount,
    $self.count
  ]

proc `$`*(self: Cash): string =
  var status = 0
  if self.free:
    status = 1
  "($1, $2, $3)" % [
    $self.number,
    $status,
    $self.totalCash
  ]

proc `$`*(self: Shop): string =
  "($1, $2, $3)" % [
    $self.staff,
    $self.goods,
    $self.cashes
  ]

proc toUnix(date: string): int64 =
  try:
    return date.parse("dd'.'MM'.'YYYY").toTime.toUnix
  except TimeParseError:
    stderr.write(getCurrentExceptionMsg() & "\n")
    return result

proc rowToStaff(row: seq[string]): Staff =
  return Staff(firstName: row[0], lastName: row[1],birthDate: toUnix(row[2]),post: parseEnum[Post](row[3]))

proc readStaff(file: string): seq[Staff] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToStaff(parser.row))
  parser.close()
  return result

proc rowToGood(row: seq[string]): Good =
   return Good(title: row[0],price: row[1].parseFloat,endDate: toUnix(row[2]), discount: row[3].parseFloat, count: row[4].parseInt)

proc readGoods(file: string): seq[Good] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToGood(parser.row))
  parser.close()
  return result

proc rowToCash(row: seq[string]): Cash =
  return Cash(number: row[0].parseInt,free: row[1].parseBool,totalCash: row[2].parseFloat)

proc readCash(file: string): seq[Cash] =
  var parser: CsvParser
  parser.open(file)
  parser.readHeaderRow()
  while  parser.readRow():
    result.add(rowToCash(parser.row))
  parser.close()

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
  let db = open("shop.db", "", "", "")
  var shop = Shop(staff: 0,goods: 0, cashes: 0)
  var logger = newTimedRotatingFileHandler(
    filePath= "logs/app_shop.log",
    whenInterval='M',
    interval=1,
    backupCount=3,
    fmtStr="[$date $time][$levelname] "
  )

  db.createTable(
    "Staff",
    "firstname VARCHAR(25)",
    "lastname VARCHAR(25)",
    "birthdate UNSIGNED INTEGER",
    "post VARCHAR(25)"
    )
  shop.staff += db.insert(logger,"Staff","firstname, lastname, birthdate,post",readStaff("data/shop_staff.csv"))
  db.createTable(
    "Good",
    "title VARCHAR(80)",
    "price UNSIGNED FLOAT",
    "enddate UNSIGNED INTEGER",
    "discount UNSIGNED FLOAT",
    "count UNSIGNED INTEGER"
    )
  shop.goods += db.insert(logger,"Good","title, price, enddate, discount, count",readGoods("data/shop_goods.csv"))
  db.createTable(
    "Cash",
    "number UNSIGNED INTEGER",
    "free BOOLEAN",
    "totalcash UNSIGNED FLOAT",
    )
  shop.cashes += db.insert(logger,"Cash","number, free, totalcash",readCash("data/shop_cashes.csv"))
  db.createTable(
    "Shop",
    "staffnumber UNSIGNED INTEGER",
    "goodsnumber UNSIGNED INTEGER",
    "cashesnumber UNSIGNED INTEGER",
    )
  let k = db.insert(logger,"Shop","staffnumber, goodsnumber, cashesnumber",@[shop])
  db.close()