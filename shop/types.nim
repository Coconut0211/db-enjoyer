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
    count*: int

  Cash* = ref object of RootObj
    number*: int
    free*: bool
    totalCash*: float

  Shop* = ref object of RootObj
    staff*: seq[Staff]
    goods*: seq[Good]
    cashes*: seq[Cash]