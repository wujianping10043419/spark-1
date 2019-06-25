package org.apache.spark

import java.io.File
import scala.io.Source
import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

/**
  * Created by 10130564 on 2017/4/18.
  */
object FileUtils {
  def getFiles(dir: File, filter: File => Boolean = (file: File) => true): List[File] = {
    @tailrec
    def helper(dirs: List[File], files: List[File]): List[File] = {
      if (dirs.isEmpty)
        files
      else {
        val subDirs = dirs.flatMap(dir => dir.listFiles.filter(_.isDirectory))
        val newFiles = dirs.flatMap(dir => dir.listFiles.filter(filter))
        helper(subDirs, newFiles ::: files)
      }
    }
    require(dir != null && dir.isDirectory, s"$dir is not a dir")
    val subDirs = dir.listFiles.filter(_.isDirectory).toList
    val files = dir.listFiles.filter(filter).toList
    helper(subDirs, files)
  }

  def getFileHeader(file: File, charset: String = "UTF-8"): String = withFileLineIterator[String](file, charset)(itr => itr.next())

  def getFileContentByFilter(file: File, charset: String = "UTF-8")(filter: String => Boolean): Array[String] = {
    withFileLineIterator[Array[String]](file, charset) {
      itr => itr.filter(filter).toArray
    }
  }

  def getFileContentByColumn(file: File, column: Array[String], filterColumns: Array[String] = Array.empty, charset: String = "UTF-8")(filter: Array[String] => Boolean): Array[Array[String]] = {
    withFileLineIterator[Array[Array[String]]](file, charset) {
      itr => val headers = splitLine(itr.next())
        val cols = column.map(c => headers.indexOf(c.toUpperCase))
        val filterColumnIndex = filterColumns.map(c => headers.indexOf(c.toUpperCase))
        require(cols.forall(_ >= 0) && filterColumnIndex.forall(_ >= 0), s"Assigned field name not found in data file")
        val fieldData = for {line <- itr
                             array = splitLine(line)
                             if filter(filterColumnIndex.map(index => array(index)))
        } yield (cols.map(index => array(index)))
        fieldData.toArray
    }
  }

  def fileReader(path: String): String = {
    val fileTry = Try(Source.fromFile(path, "GBK"))
    if (fileTry.isSuccess) {
      val rst = Try(fileTry.get.mkString)
      fileTry.get.close()
      if (rst.isSuccess) return rst.get
    }
    val utfTry = Try(Source.fromFile(path, "UTF-8"))
    if (utfTry.isSuccess) {
      val rst = Try(utfTry.get.mkString)
      utfTry.get.close()
      if (rst.isSuccess) return rst.get
    }
    ""
  }

  def fileReader(file: File, charset: String = "UTF-8"): String = {
    val source = Source.fromFile(file, charset)
    Try(source.mkString) match {
      case scala.util.Success(content) =>
        source.close()
        content
      case Failure(e) =>
        s"File ${file.getName} read failed!The exception is ${e.getMessage}"
    }
  }

  /*删除file，如果file是文件夹，则先删除文件夹下文件，再删除file*/
  def deleteFiles(dir: File): Boolean = {
    if (!dir.exists())
      true
    else if (dir.isFile)
      dir.delete()
    else {
      val files = getFiles(dir, file => true)
      files.forall(_.delete()) && dir.delete()
    }
  }

  /*删除dir目录下的所有文件，但保留dir*/
  def deleteDirFiles(dir: File, filter: File => Boolean): Unit = {
    if (!dir.exists())
      return
    else if (dir.isFile)
      dir.delete()
    else {
      val files = getFiles(dir, filter)
      files.foreach(_.delete())
    }
  }

  def withFileLineIterator[T](file: File, charset: String = "UTF-8")(p: Iterator[String] => T) = {
    val source = Source.fromFile(file, charset)
    try {
      p(source.getLines())
    } finally {
      source.close()
    }
  }

  private def splitLine(line: String): Array[String] = line.replace("\"", "").split("\t")
}
