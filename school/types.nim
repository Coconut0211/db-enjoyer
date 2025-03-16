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