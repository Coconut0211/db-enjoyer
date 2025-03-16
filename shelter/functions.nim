import re,strutils,times
import types

proc toUnix(date: string): int64 =
  try:
    return date.parse("dd'.'MM'.'YYYY").toTime.toUnix
  except TimeParseError:
    stderr.write(getCurrentExceptionMsg() & "\n")
    return result

proc toStr(date: int64): string =
  fromUnix(date).format("dd'.'MM'.'YYYY")

proc checkStr[T](data: T): bool = 
  match(data,re"^([A-Za-z]{2,})$")

proc checkDigit[T](data: T): bool =
  data > 0

template getAttrValue(obj,attr): auto = obj.attr

template setAttrValue*(obj,attr,value,checkFunc) =
  if checkFunc(value):
    obj.attr = value

template setBirthdate*(obj,attr,value) =
  obj.attr = value.toUnix()

proc setManagerDol*(self: Manager, data: string, glavnVal: bool) =
  try:
    self.post.dol = parseEnum[Role](data)
    self.post.glavn = glavnVal
  except ValueError:
    stderr.write("Нет должности $1\n" % data)
    self.post.dol = NONE
    self.post.glavn = false


template initManager*(varName; firstnameval: string, lastnameval: string, dolval: string, birthDateval: string, glavnval: bool) =
  let varName = Manager()
  varName.setAttrValue(firstname,firstnameval,checkStr)
  varName.setAttrValue(lastname,lastnameval,checkStr)
  varName.setManagerDol(dolval,glavnval)
  varName.setBirthdate(birthDate,birthDateval)

template initStaff*(varName; firstnameval: string, lastnameval: string, birthDateval: string,uidval:int) =
  let varName = Staff()
  varName.setAttrValue(firstname,firstnameval,checkStr)
  varName.setAttrValue(lastname,lastnameval,checkStr)
  varName.setAttrValue(uid,uidval,checkDigit)
  varName.setBirthdate(birthDate,birthDateval)

template initPet*(varName; nameval:string, ageval:int) =
  let varName = Pet()
  varName.setAttrValue(name,nameval,checkStr)
  varName.setAttrValue(age,ageval,checkDigit)

proc `$`*(self: Manager): string = 
  var prep = ""
  if self.post.glavn:
    prep = "Главный "
  "('$1', '$2', '$3','$4$5')" % [
  self.getAttrValue(firstname),
  self.getAttrValue(lastname),
  self.getAttrValue(birthDate).toStr,
  prep,
  $self.getAttrValue(post).dol
  ]

proc `$`*(self: Staff): string = 
  "('$1', '$2', '$3', $4)" % [
  self.getAttrValue(firstname),
  self.getAttrValue(lastname),
  self.getAttrValue(birthDate).toStr,
  $self.getAttrValue(uid),
  ]

proc `$`*(self: Pet): string =
  #[ var podp: string = "лет"
  if self.getAttrValue(age) == 1:
    podp = "год"
  elif self.getAttrValue(age) <= 4 and self.getAttrValue(age) >= 2:
    podp = "года" ]#
  return "('$1', $2)" % [
  self.getAttrValue(name),
  $self.getAttrValue(age),
  ]





