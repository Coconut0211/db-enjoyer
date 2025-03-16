import times
import re, strutils

import types


proc toUnix(date: string): int64 =
  try:
    return date.parse("dd'.'MM'.'YYYY").toTime.toUnix
  except TimeParseError:
    stderr.write(getCurrentExceptionMsg() & "\n")
    return result

proc toStr(date: int64): string =
  fromUnix(date).format("dd'.'MM'.'YYYY")

proc checkStr[T](data: T, description: string): bool =
  var pattern: Regex
  case description
  of "title":
    pattern = re"^([A-Za-z0-9\!-_\. ]{2,80})$"
  of "name":
    pattern = re"^([A-Za-z]{2,})$"
  data.match(pattern)

proc checkDigit[T](data: T, description: string): bool =
  case description
  of "discount":
    data >= 0 and data <= 1
  else:
    data > 0

template getAttrValue(obj, attr): auto = obj.attr

template setDate*(obj, attr; date: string) =
  obj.attr = date.toUnix

template setAttrValue*(
    obj, attr, value, checkFunc;
    description: string) =
  if checkFunc(value, description):
    obj.attr = value

proc setStuffPost*(self: Staff, post: string) =
  try:
    self.post = parseEnum[Post](post)
  except ValueError:
    stderr.write("Нет такой должности $1\n" % post)
    self.post = NONE

proc setCashFree*(self: Cash, free: bool) =
  self.free = free

template initStaff*(varName;
                   firstNameValue, lastNameValue,
                   birthDateValue, postValue: string) =
  let varName = Staff()
  varName.setAttrValue(firstName, firstNameValue, checkStr, "name")
  varName.setAttrValue(lastName, lastNameValue, checkStr, "name")
  varName.setDate(birthDate, birthDateValue)
  varName.setStuffPost(postValue)

template initGood*(varName;
                  titleValue: string, priceValue: float,
                  endDateValue: string, discountValue: float,
                  countValue: int) =
  let varName = Good()
  varName.setAttrValue(title, titleValue, checkStr, "title")
  varName.setAttrValue(price, priceValue, checkDigit, "price")
  varName.setDate(endDate, endDateValue)
  varName.setAttrValue(discount, discountValue, checkDigit, "discount")
  varName.setAttrValue(count, countValue, checkDigit, "count")

template initCash*(varName;
                  numberValue: int, freeValue: bool,
                  totalCashValue: float) =
  let varName = Cash()
  varName.setAttrValue(number, numberValue, checkDigit, "number")
  varName.setCashFree(freeValue)
  varName.setAttrValue(totalCash, totalCashValue, checkDigit, "totalCashe")

proc `$`*(self: Staff): string =
  "('$1', '$2', '$3', '$4')" % [
    self.getAttrValue(firstName),
    self.getAttrValue(lastName),
    self.getAttrValue(birthDate).toStr,
    $self.getAttrValue(post),
  ]

proc `$`*(self: Good): string =
  "('$1', $2, '$3', $4, $5)" % [
    self.getAttrValue(title),
    $self.getAttrValue(price),
    self.getAttrValue(endDate).toStr,
    $self.getAttrValue(discount),
    $self.getAttrValue(count)
  ]

proc `$`*(self: Cash): string =
  var status: int
  if self.getAttrValue(free):
    status = 1
  else:
    status = 0
  "($1, $2, $3)" % [
    $self.getAttrValue(number),
    $status,
    $self.getAttrValue(totalCash)
  ]
