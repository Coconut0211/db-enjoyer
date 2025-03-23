import logging, os, times, strutils, sequtils, algorithm

type
  TimedRollingFileHandler* = ref object of Logger
    ## Обработчик логов
    ## filename - имя файла
    ## whenInterval - 'D' - день, 'H' - час, 'M' - минута, 'S' - секунда
    ## interval - заданный интервал
    ## backupCount - максимальное число файлов
    ## currentFile - текущий файл для записи
    ## nextRolloverTime - когда следующая ротация
    filename: string
    dirname: string
    whenInterval: char  
    interval: int
    backupCount: int
    currentFile: File
    nextRolloverTime: Time

proc calculateNextRolloverTime(whenInterval: char, interval: int): Time =
  ## Вычисляет время следующей ротации.
  let now = getTime()
  case whenInterval:
    of 'D':
      return now + initTimeInterval(days = interval)
    of 'H':
      return now + initTimeInterval(hours = interval)
    of 'M':
      return now + initTimeInterval(minutes = interval)
    of 'S':
      return now + initTimeInterval(seconds = interval)
    else:
      echo "Интервал не поддерживается"


proc newTimedRotatingFileHandler*(
    filePath: string,
    whenInterval: char,
    interval: int,
    backupCount: int,
    fmtStr: string
  ): TimedRollingFileHandler =
  ## Создает новый обработчик с ротацией по времени.
  new(result)
  result.filename = filePath.splitPath().tail
  result.dirname = getAppDir()
  if filePath.splitPath().head != "":
    result.dirname = filePath.splitPath().head
  result.fmtStr = fmtStr
  result.whenInterval = whenInterval
  result.interval = interval
  result.backupCount = backupCount
  if not result.dirname.dirExists():
    createDir(result.dirname)
  result.currentFile = open(filePath, fmAppend)
  result.nextRolloverTime = calculateNextRolloverTime(result.whenInterval, result.interval)


proc rotateFile(logger: TimedRollingFileHandler) =
  ## Выполняет ротацию файла.
  logger.currentFile.close()
  let currentTime =  format(getTime(),"yyyy'_'MM'_'dd'_'HH'_'mm'_'ss")
  moveFile(logger.dirname / logger.filename, logger.dirname / join([currentTime,logger.filename]))
  logger.currentFile = open(logger.dirname / logger.filename, fmAppend)

  # Удаляем старые файлы, если их слишком много
  var logFiles = toSeq(walkFiles(logger.dirname / "*.log"))  # Так можно получить список всех логов, если изначально задано такое расширение.
  logFiles.sort() # Отсортируем по старшинству
  if logFiles.len > logger.backupCount:
    for i in 0 ..< logFiles.len - logger.backupCount:
      removeFile(logFiles[i])

  # Обновляем время следующей ротации
  logger.nextRolloverTime = calculateNextRolloverTime(logger.whenInterval, logger.interval)


proc log*(logger: TimedRollingFileHandler, level: Level, args: varargs[string, `$`]) =
  ## Процедура для записи логов.
  let now = getTime()  # Время вызова записи лога
  if now >= logger.nextRolloverTime:
    logger.rotateFile()
  let message = args.join(" ")  # Объединяем переданные строки в одну
  var fmtStr = logger.fmtStr
  fmtStr = fmtStr.replace("$date", now.format("yyyy-MM-dd"))
  fmtStr = fmtStr.replace("$time", now.format("HH:mm:ss"))
  fmtStr = fmtStr.replace("$datetime", now.format("yyyy-MM-dd'T'HH:mm:ss"))
  fmtStr = fmtStr.replace("$app", getAppFilename())
  fmtStr = fmtStr.replace("$appdir", getAppFilename().splitFile.dir)
  fmtStr = fmtStr.replace("$appname", getAppFilename().splitFile.name)
  fmtStr = fmtStr.replace("$levelid", $LevelNames[level][0])
  fmtStr = fmtStr.replace("$levelname", LevelNames[level])
  logger.currentFile.writeLine(fmtStr & message)  # Записываем данные в файл
  logger.currentFile.flushFile()  # Принудительно освобождаем поток вывода
