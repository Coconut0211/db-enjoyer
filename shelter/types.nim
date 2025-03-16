type
  Role* = enum
    NONE, Директор, Бухгалтер, Ветеринар
  Post* = object
    dol*: Role
    glavn*: bool
  Person* = ref object of RootObj
    firstname*: string
    lastname*: string
    birthDate*: int64
  Manager* = ref object of Person
    post*: Post
  Staff* = ref object of Person
    uid*: int
  Pet* = ref object of RootObj
    name*: string
    age*: int 
  Shelter* = ref object of RootObj
    staff*: seq[Staff]
    pet*: seq[Pet]
    manager*: seq[Manager]