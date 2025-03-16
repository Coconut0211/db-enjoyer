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
  data >= 1 and data <= 11

proc checkChar[T](data: T): bool =
  match($data,re"^[A-Z]$")


template getAttrValue(obj,attr): auto = obj.attr

template setAttrValue*(obj,attr,value,checkFunc) =
  if checkFunc(value):
    obj.attr = value

template setBirthdate*(obj,attr,value) =
  obj.attr = value.toUnix()

proc setTeacherSubj*(self: Teacher, data: string) =
  try:
    self.subject = parseEnum[Subjects](data)
  except ValueError:
    stderr.write("Нет предмета $1\n" % data)
    self.subject = NONE


template initDirector*(varName; firstnameval, lastnameval,birthDateval: string) =
  let varName = Director()
  varName.setAttrValue(firstname,firstnameval,checkStr)
  varName.setAttrValue(lastname,lastnameval,checkStr)
  varName.setBirthdate(birthDate,birthDateval)

template initTeacher*(varName; firstnameval, lastnameval,subjectval,birthDateval: string) =
  let varName = Teacher()
  varName.setAttrValue(firstname,firstnameval,checkStr)
  varName.setAttrValue(lastname,lastnameval,checkStr)
  varName.setTeacherSubj(subjectval)
  varName.setBirthdate(birthDate,birthDateval)

template initStudent*(varName; firstnameval:string, lastnameval:string, birthDateval: string, classLetval: char, classNumval: int) =
  let varName = Student()
  varName.setAttrValue(firstname,firstnameval,checkStr)
  varName.setAttrValue(lastname,lastnameval,checkStr)
  varName.setAttrValue(classLet,classLetval,checkChar)
  varName.setAttrValue(classNum,classNumval,checkDigit)
  varName.setBirthdate(birthDate,birthDateval)

proc `$`*(self: Director): string = 
  "('$1', '$2', '$3')" % [
  self.getAttrValue(firstname),
  self.getAttrValue(lastname),
  self.getAttrValue(birthDate).toStr()
  ]

proc `$`*(self: Teacher): string = 
  "('$1', '$2', '$3', '$4')" % [
  self.getAttrValue(firstname),
  self.getAttrValue(lastname),
  $self.getAttrValue(subject),
  self.getAttrValue(birthDate).toStr
  ]

proc `$`*(self: Student): string = 
  "('$1', '$2', $3, '$4', '$5')" % [
  self.getAttrValue(firstname),
  self.getAttrValue(lastname),
  $self.getAttrValue(classNum),
  $self.getAttrValue(classLet),
  self.getAttrValue(birthDate).toStr
  ]




