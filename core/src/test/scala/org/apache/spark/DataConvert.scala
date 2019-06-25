package org.apache.spark

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/**
  * Created by 10129659 on 15-11-23.
  */
object DateConvert {
  def timeDelay(day: String, delayTime: Long): Date = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    new Date(date.getTime() + delayTime)
  }

  def dateToDay(date: Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(date)
  }

  def date2TimeString(date: Date, format: String = "yyyyMMddHHmmss"): String = {
    val df = new SimpleDateFormat(format)
    df.format(date)
  }

  def dateToDayTime(date: Date): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    df.format(date)
  }

  def dateTimeToDate(dateTime: String, format: String = "yyyy-MM-dd HH:mm:ss"): Date = {
    val df = new SimpleDateFormat(format)
    df.parse(dateTime)
  }

  def currentDate(): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(new Date())
  }

  def dayOfWeek(day: String): Int = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_WEEK) match {
      case 1 => 7
      case x => x - 1
    }
  }

  def dayOfMonth(day: String): Int = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_MONTH)
  }

  def dayBefore(day: String, beforeDays: Long = 1): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    df.format(new Date(date.getTime() - beforeDays * 24 * 3600 * 1000))
  }

  def dayBefore(day: Date, beforeDays: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(new Date(day.getTime() - beforeDays * 24 * 3600 * 1000))
  }

  def dayAfter(day: String, afterDays: Long = 1): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    df.format(new Date(date.getTime() + afterDays * 24 * 3600 * 1000))
  }

  def dayAfter(day: Date, afterDays: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(new Date(day.getTime() + afterDays * 24 * 3600 * 1000))
  }

  def dateBefore(date: Date, beforeDays: Long): Date = new Date(date.getTime() - beforeDays * 24 * 3600 * 1000)

  def dateAfter(date: Date, afterDays: Long): Date = new Date(date.getTime() + afterDays * 24 * 3600 * 1000)

  def weekOfDay(day: String): Int = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.WEEK_OF_YEAR)
  }

  def monthOfDay(day: String): Int = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.MONTH) + 1
  }

  def yearOfDay(day: String): Int = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    val date = df.parse(day)
    val cal = Calendar.getInstance()
    cal.setTime(date)
    cal.get(Calendar.YEAR)
  }

  def milliSecondsToDay(ms: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.format(new Date(ms))
  }

  def formatTransfer(timeString: String, original: String, target: String): String = {
    val df1 = new SimpleDateFormat(original)
    val df2 = new SimpleDateFormat(target)
    df2.format(df1.parse(timeString))
  }

  //取周最后一天的date,是否与当前时间对比，是的话，如果比当前时间大那就用当前时间
  def getDateByWeek(year: Int, week: Int, isCompare: Boolean = true): String = {
    val date = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setMinimalDaysInFirstWeek(4)
    calendar.setWeekDate(year, week, 1)
    if (isCompare && calendar.getTimeInMillis > new Date().getTime)
      date.format(new Date())
    else
      date.format(calendar.getTime)
  }

  //取月最后一天的date,是否与当前时间对比，是的话，如果比当前时间大那就用当前时间
  def getDateByMonth(year: Int, month: Int, isCompare: Boolean = true): String = {
    val date = new SimpleDateFormat("yyyy-MM-dd")
    val calendar = Calendar.getInstance()
    calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH))
    calendar.set(year, month, 0)
    if (isCompare && calendar.getTimeInMillis > new Date().getTime)
      date.format(new Date())
    else
      date.format(calendar.getTime)
  }

  def getWeekInYear(dateStr: String): Int = {
    val date = new SimpleDateFormat("yyyy-MM-dd").parse(dateStr)
    val calendar = Calendar.getInstance()
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setMinimalDaysInFirstWeek(4)
    calendar.setTime(date)
    val week = calendar.get(Calendar.WEEK_OF_YEAR)
    week
  }
}
