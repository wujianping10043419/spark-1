package org.apache.spark

//import com.zte.bigdata.vmax.performance.autochart.services.analysis.spark.utils.{JsonProtocol, Utils}
import org.apache.spark.util.JsonProtocol
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.util._
import org.apache.spark.scheduler._
//import com.zte.bigdata.vmax.performance.autochart.utils.DateConvert
import java.util.Date
import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.collection._
import scala.util.Try

/**
  * Created by 10130564 on 2017/4/28.
  */
class SparkUITaskMetrics(val index: Int, val taskId: Long, val attempt: Int, val taskType: String, val status: String, val localityLevel: String, val isSpeculative: Boolean,
                         val executorId: String, val host: String, val launchTime: Long, val duration: Long,
                         val executorRunTime: Long,
                         val schedulerDelay: Long, val taskDeserializationTime: Long,
                          val gcTime: Long,
                         val resultSerializationTime: Long, val gettingResultTime: Long,
                         val memoryBytesSpilled: Long, val diskBytesSpilled: Long, val peakExecutionMemory: Long,
                         val inputSize: Long, val inputRecords: Long, val shuffleReadTotalSize: Long,
                         val shuffleReadLocalSize: Long, val shuffleReadRemoteSize: Long, val shuffleReadRecords: Long, val shuffleReadWait: Long,
                         val outputSize: Long, val outputRecords: Long, val shuffleWriteSize: Long, val shuffleWriteRecords: Long, val shuffleWriteTime: Long, val error: String = "",
                         val writeTime: Long, val hadoopRddTime: Long, val iteratorTime: Long, val funcTime: Long, val externalAccums: Seq[AccumulableInfo]) {
  def this(task: SparkListenerTaskEnd) = this(task.taskInfo.index, task.taskInfo.taskId, task.taskInfo.attemptNumber, task.taskType, task.taskInfo.status, task.taskInfo.taskLocality.toString, task.taskInfo.speculative,
    task.taskInfo.executorId, task.taskInfo.host, task.taskInfo.launchTime, task.taskInfo.duration,
    task.taskMetrics.executorRunTime,
    SparkStagePage.getSchedulerDelay(task.taskInfo, task.taskMetrics, task.taskInfo.finishTime), task.taskMetrics.executorDeserializeTime,
    task.taskMetrics.jvmGCTime,
    task.taskMetrics.resultSerializationTime, SparkStagePage.getGettingResultTime(task.taskInfo, task.taskInfo.finishTime),
    task.taskMetrics.memoryBytesSpilled, task.taskMetrics.diskBytesSpilled, task.taskMetrics.peakExecutionMemory,
    task.taskMetrics.inputMetrics.bytesRead, task.taskMetrics.inputMetrics.recordsRead, task.taskMetrics.shuffleReadMetrics.totalBytesRead,
    task.taskMetrics.shuffleReadMetrics.localBytesRead, task.taskMetrics.shuffleReadMetrics.remoteBytesRead,
    task.taskMetrics.shuffleReadMetrics.recordsRead, task.taskMetrics.shuffleReadMetrics.fetchWaitTime,
    task.taskMetrics.outputMetrics.bytesWritten, task.taskMetrics.outputMetrics.recordsWritten,
    task.taskMetrics.shuffleWriteMetrics.bytesWritten, task.taskMetrics.shuffleWriteMetrics.recordsWritten,
    task.taskMetrics.shuffleWriteMetrics.writeTime, task.reason.toString,
    task.taskMetrics.writeTime, task.taskMetrics.hadoopRddTime,
    task.taskMetrics.iteratorTime, task.taskMetrics.funcTime, task.taskInfo.accumulables
  )
}

class SparkUITaskMetricsAggregatedByExecutor(val executorId: String, val address: String, val addedTime: Long, val cores: Int, val blockManagerMaxSize: Long, val taskTime: Long,
                                             val totalTasks: Int, val failedTasks: Int, val successTasks: Int,
                                             val inputSize: Long, val inputRecords: Long,
                                             val shuffleReadTotalSize: Long, val shuffleReadLocalSize: Long,
                                             val shuffleReadRemoteSize: Long, val shuffleReadWait: Long,
                                             val shuffleReadRecords: Long,
                                             val outputSize: Long, val outputRecords: Long,
                                             val shuffleWriteSize: Long, val shuffleWriteRecords: Long,
                                             val schedulerDelay: Long, val taskDeserializationTime: Long,
                                             val gcTime: Long, val resultSerializationTime: Long, val gettingResultTime: Long,
                                             val memoryBytesSpilled: Long, val diskBytesSpilled: Long, val peakExecutionMemory: Long,
                                             val writeTime: Long, val hadoopRddTime: Long, val iteratorTime: Long, val funcTime: Long)

object SparkUITaskMetricsAggregatedByExecutor {
  def apply(taskMetricsInStage: List[SparkUITaskMetrics], executor: Map[String, SparkListenerExecutorAdded], blockManager: Map[String, SparkListenerBlockManagerAdded]): List[SparkUITaskMetricsAggregatedByExecutor] = {
    def getSumSizeInfo(metrics: List[SparkUITaskMetrics]): scala.collection.mutable.Map[String, Long] = {
      val map = new scala.collection.mutable.HashMap[String, Long]()
      metrics.foreach {
        m => map += (("inputSize", m.inputSize + map.get("inputSize").getOrElse(0L)))
          map.+=(("inputRecords", m.inputRecords + map.get("inputRecords").getOrElse(0L)))
          map.+=(("shuffleReadTotalSize", m.shuffleReadTotalSize + map.get("shuffleReadTotalSize").getOrElse(0L)))
          map.+=(("shuffleReadLocalSize", m.shuffleReadLocalSize + map.get("shuffleReadLocalSize").getOrElse(0L)))
          map.+=(("shuffleReadRemoteSize", m.shuffleReadRemoteSize + map.get("shuffleReadRemoteSize").getOrElse(0L)))
          map.+=(("shuffleReadWait", m.shuffleReadWait + map.get("shuffleReadWait").getOrElse(0L)))
          map.+=(("shuffleReadRecords", m.shuffleReadRecords + map.get("shuffleReadRecords").getOrElse(0L)))
          map.+=(("outputSize", m.outputSize + map.get("outputSize").getOrElse(0L)))
          map.+=(("outputRecords", m.outputRecords + map.get("outputRecords").getOrElse(0L)))
          map.+=(("shuffleWriteSize", m.shuffleWriteSize + map.get("shuffleWriteSize").getOrElse(0L)))
          map.+=(("shuffleWriteRecords", m.shuffleWriteRecords + map.get("shuffleWriteRecords").getOrElse(0L)))
          map.+=(("schedulerDelay", m.schedulerDelay + map.get("schedulerDelay").getOrElse(0L)))
          map.+=(("taskDeserializationTime", m.schedulerDelay + map.get("taskDeserializationTime").getOrElse(0L)))
          map.+=(("gcTime", m.schedulerDelay + map.get("gcTime").getOrElse(0L)))
          map.+=(("resultSerializationTime", m.schedulerDelay + map.get("resultSerializationTime").getOrElse(0L)))
          map.+=(("gettingResultTime", m.schedulerDelay + map.get("gettingResultTime").getOrElse(0L)))
          map.+=(("memoryBytesSpilled", m.schedulerDelay + map.get("memoryBytesSpilled").getOrElse(0L)))
          map.+=(("diskBytesSpilled", m.schedulerDelay + map.get("diskBytesSpilled").getOrElse(0L)))
          map.+=(("peakExecutionMemory", m.schedulerDelay + map.get("peakExecutionMemory").getOrElse(0L)))
          map.+=(("writeTime", m.schedulerDelay + map.get("writeTime").getOrElse(0L)))
          map.+=(("hadoopRddTime", m.schedulerDelay + map.get("hadoopRddTime").getOrElse(0L)))
          map.+=(("iteratorTime", m.schedulerDelay + map.get("iteratorTime").getOrElse(0L)))
          map.+=(("funcTime", m.schedulerDelay + map.get("funcTime").getOrElse(0L)))
      }
      map
    }

    taskMetricsInStage.groupBy(_.executorId).map {
      case (executorId, metrics) if metrics.isEmpty => null
      case (executorId, metrics) => val taskTime = metrics.map(_.executorRunTime).sum
        val failedTasks = metrics.count(_.status != "SUCCESS")
        val successTasks = metrics.count(_.status == "SUCCESS")
        val sumSizeInfo = getSumSizeInfo(metrics)
        val address = blockManager.get(executorId).map(_.blockManagerId.hostPort).getOrElse(metrics.head.host + ":" + "Unknown")
        new SparkUITaskMetricsAggregatedByExecutor(executorId, address, executor.get(executorId).map(_.time).getOrElse(0L),
          executor.get(executorId).map(_.executorInfo.totalCores).getOrElse(-1), blockManager.get(executorId).map(_.maxMem).getOrElse(-1L), taskTime,
          successTasks + failedTasks, failedTasks, successTasks,
          sumSizeInfo.getOrElse("inputSize", 0L), sumSizeInfo.getOrElse("inputRecords", 0L),
          sumSizeInfo.getOrElse("shuffleReadTotalSize", 0L), sumSizeInfo.getOrElse("shuffleReadRecords", 0L),
          sumSizeInfo.getOrElse("shuffleReadLocalSize", 0L), sumSizeInfo.getOrElse("shuffleReadRemoteSize", 0L),
          sumSizeInfo.getOrElse("shuffleReadWait", 0L),
          sumSizeInfo.getOrElse("outputSize", 0L), sumSizeInfo.getOrElse("outputRecords", 0L),
          sumSizeInfo.getOrElse("shuffleWriteSize", 0L), sumSizeInfo.getOrElse("shuffleWriteRecords", 0L),
          sumSizeInfo.getOrElse("schedulerDelay", 0L), sumSizeInfo.getOrElse("taskDeserializationTime", 0L),
          sumSizeInfo.getOrElse("gcTime", 0L), sumSizeInfo.getOrElse("resultSerializationTime", 0L),
          sumSizeInfo.getOrElse("gettingResultTime", 0L), sumSizeInfo.getOrElse("memoryBytesSpilled", 0L),
          sumSizeInfo.getOrElse("diskBytesSpilled", 0L), sumSizeInfo.getOrElse("peakExecutionMemory", 0L),
          sumSizeInfo.getOrElse("writeTime",0L),sumSizeInfo.getOrElse("hadoopRddTime",0L),
          sumSizeInfo.getOrElse("iteratorTime",0L), sumSizeInfo.getOrElse("funcTime",0L)
        )
      case _ => throw new Exception(s"SparkUITaskMetricsAggregatedByExecutor construct fail for task metrics error.")
    }.filterNot(_ == null).toList
  }
}

case class SparkUITaskSummaryMetrics(metricDesc: String, min: Long, percentile25: Long, median: Long, percentile75: Long, max: Long)

object SparkUITaskSummaryMetrics {
  def apply(taskMetricsInStage: List[SparkUITaskMetrics]): List[SparkUITaskSummaryMetrics] = {
    def getSparkUITaskSummaryMetrics(metricDesc: String, metrics: Array[SparkUITaskMetrics])(getter: SparkUITaskMetrics => Long) = {
      if (metrics == null || metrics.isEmpty) SparkUITaskSummaryMetrics(metricDesc, 0L, 0L, 0L, 0L, 0L)
      else {
        val orderedArray = metrics.sortBy(getter)
        SparkUITaskSummaryMetrics(metricDesc, getter(orderedArray(0)), getter(orderedArray(orderedArray.length / 4)), getter(orderedArray(orderedArray.length * 2 / 4)), getter(orderedArray(orderedArray.length * 3 / 4)), getter(orderedArray(orderedArray.length - 1)))
      }
    }
    val array = taskMetricsInStage.toArray
    getSparkUITaskSummaryMetrics("Duration", array)(_.duration) :: getSparkUITaskSummaryMetrics("Executor Run Time", array)(_.executorRunTime) ::
      getSparkUITaskSummaryMetrics("Scheduler Delay", array)(_.schedulerDelay) ::
      getSparkUITaskSummaryMetrics("Task Deserialization Time", array)(_.taskDeserializationTime) :: getSparkUITaskSummaryMetrics("GC Time", array)(_.gcTime) ::
      getSparkUITaskSummaryMetrics("Result Serialization Time", array)(_.resultSerializationTime) :: getSparkUITaskSummaryMetrics("Getting Result Time", array)(_.gettingResultTime) ::
      getSparkUITaskSummaryMetrics("Memory Bytes Spilled", array)(_.memoryBytesSpilled) :: getSparkUITaskSummaryMetrics("Disk Bytes Spilled", array)(_.diskBytesSpilled) ::
      getSparkUITaskSummaryMetrics("Peak Execution Memory", array)(_.peakExecutionMemory) :: getSparkUITaskSummaryMetrics("Input Size", array)(_.inputSize) ::
      getSparkUITaskSummaryMetrics("Input Records", array)(_.inputRecords) :: getSparkUITaskSummaryMetrics("Shuffle Total Read Size", array)(_.shuffleReadTotalSize) ::
      getSparkUITaskSummaryMetrics("Shuffle Read Records", array)(_.shuffleReadRecords) :: getSparkUITaskSummaryMetrics("Shuffle Read Local Size", array)(_.shuffleReadLocalSize) ::
      getSparkUITaskSummaryMetrics("Shuffle Read Remote Size", array)(_.shuffleReadRemoteSize) :: getSparkUITaskSummaryMetrics("Shuffle Read Blocked Time", array)(_.shuffleReadWait) ::
      getSparkUITaskSummaryMetrics("Output Size", array)(_.outputSize) :: getSparkUITaskSummaryMetrics("Output Records", array)(_.outputRecords) ::
      getSparkUITaskSummaryMetrics("Shuffle Write Size", array)(_.shuffleWriteSize) :: getSparkUITaskSummaryMetrics("Shuffle Write Records", array)(_.shuffleWriteRecords) ::
      getSparkUITaskSummaryMetrics("Shuffle Write Time", array)(_.shuffleWriteTime) ::
      getSparkUITaskSummaryMetrics("Write cost", array)(_.writeTime)::
      getSparkUITaskSummaryMetrics("HadoopRdd cost", array)(_.hadoopRddTime)::
      getSparkUITaskSummaryMetrics("Iterator cost", array)(_.iteratorTime)::
      getSparkUITaskSummaryMetrics("Func cost", array)(_.funcTime)::
      Nil
  }
}

case class SparkUITaskInfo(stageId: Int, stageAttemptId: Int, summaryMetrics: List[SparkUITaskSummaryMetrics], metricsAggregatedByExecutor: List[SparkUITaskMetricsAggregatedByExecutor], taskMetrics: List[SparkUITaskMetrics]) {
  def this(stageId: Int, stageAttemptId: Int, taskMetrics: List[SparkUITaskMetrics], executor: Map[String, SparkListenerExecutorAdded], blockManager: Map[String, SparkListenerBlockManagerAdded]) = this(stageId, stageAttemptId, SparkUITaskSummaryMetrics(taskMetrics), SparkUITaskMetricsAggregatedByExecutor(taskMetrics, executor, blockManager), taskMetrics)

  def this(stage: StageInfo, summaryMetrics: List[SparkUITaskSummaryMetrics], metricsAggregatedByExecutor: List[SparkUITaskMetricsAggregatedByExecutor], taskMetrics: List[SparkUITaskMetrics]) = this(stage.stageId, stage.attemptId, summaryMetrics, metricsAggregatedByExecutor, taskMetrics)
}

object SparkUITaskInfo {
  def apply(stageId: Int, tasksEnd: List[SparkListenerTaskEnd], executor: Map[String, SparkListenerExecutorAdded], blockManager: Map[String, SparkListenerBlockManagerAdded]): List[SparkUITaskInfo] = {
    require(tasksEnd.forall(_.stageId == stageId), s"SparkUITaskInfo construct fail for stageId error.")
    tasksEnd.groupBy(_.stageAttemptId).map {
      case (_, tasks) if tasks.isEmpty => null
      case (attemptId, tasks) => val metrics = tasks.map(t => new SparkUITaskMetrics(t))
        new SparkUITaskInfo(stageId, attemptId, metrics, executor, blockManager)
    }.toList
  }
}

case class SparkUIStageInfo(stageId: Int, attemptId: Int, description: String, submittedTime: Long, duration: Long, stageResult: String,
                            tasksSuccess: Int, taskTotal: Int,
                            input: Long, output: Long, shuffleRead: Long, shuffleWrite: Long, hasShuffledRDD: Boolean, accInfo: List[AccumulableInfo], taskInfo: SparkUITaskInfo) {
  def this(stageEndEvent: SparkListenerStageCompleted, taskInfo: SparkUITaskInfo) = {
    this(stageEndEvent.stageInfo.stageId, stageEndEvent.stageInfo.attemptId, stageEndEvent.stageInfo.name,
      stageEndEvent.stageInfo.submissionTime.getOrElse(0), stageEndEvent.stageInfo.duration, stageEndEvent.stageInfo.getStatusString,
      Try(taskInfo.metricsAggregatedByExecutor.map(_.successTasks).sum).getOrElse(0), stageEndEvent.stageInfo.numTasks,
      Try(taskInfo.metricsAggregatedByExecutor.map(_.inputSize).sum).getOrElse(0), Try(taskInfo.metricsAggregatedByExecutor.map(_.outputSize).sum).getOrElse(0),
      Try(taskInfo.metricsAggregatedByExecutor.map(_.shuffleReadTotalSize).sum).getOrElse(0), Try(taskInfo.metricsAggregatedByExecutor.map(_.shuffleWriteSize).sum).getOrElse(0),
      stageEndEvent.stageInfo.rddInfos.exists(_.name.toLowerCase().contains("shuffle")),
      stageEndEvent.stageInfo.accumulables.map(_._2).toList,taskInfo)
  }

  def this(stage: StageInfo, taskInfo: SparkUITaskInfo) = {
    this(stage.stageId, stage.attemptId, stage.name, stage.submissionTime.getOrElse(0), stage.duration, stage.getStatusString,
      taskInfo.metricsAggregatedByExecutor.map(_.successTasks).sum, stage.numTasks,
      taskInfo.metricsAggregatedByExecutor.map(_.inputSize).sum, taskInfo.metricsAggregatedByExecutor.map(_.outputSize).sum,
      taskInfo.metricsAggregatedByExecutor.map(_.shuffleReadTotalSize).sum, taskInfo.metricsAggregatedByExecutor.map(_.shuffleWriteSize).sum,
      stage.rddInfos.exists(_.name.toLowerCase().contains("shuffle")),
      stage.accumulables.map(_._2).toList,taskInfo)
  }

  override def toString: String = {
    String.format(s"%-10s%-10s%-100s%-22s%-12s%-18s%-24s%-24s%-24s%-10s", stageId.toString, attemptId.toString, description,
      DateConvert.dateToDayTime(new Date(submittedTime)), Utils.msDurationToString(duration), stageResult,
      s"$tasksSuccess/$taskTotal", s"${Utils.bytesToString(input)}/${Utils.bytesToString(output)}", s"${Utils.bytesToString(shuffleRead)}/${Utils.bytesToString(shuffleWrite)}", hasShuffledRDD.toString)
  }
}

object SparkStagePage {
  def getGettingResultTime(info: TaskInfo, currentTime: Long): Long = {
    if (info.gettingResult) {
      if (info.finished) info.finishTime - info.gettingResultTime
      else currentTime - info.gettingResultTime // The task is still fetching the result.
    } else 0L
  }

  def getSchedulerDelay(info: TaskInfo, metrics: TaskMetrics, currentTime: Long): Long = {
    if (info.finished) {
      val totalExecutionTime = info.finishTime - info.launchTime
      val executorOverhead = metrics.executorDeserializeTime + metrics.resultSerializationTime
      math.max(0, totalExecutionTime - metrics.executorRunTime - executorOverhead - getGettingResultTime(info, currentTime))
    } else 0L // The task is still running and the metrics like executorRunTime are not available.
  }
}

case class SparkUIJobInfo(jobId: Int, description: String, submittedTime: Long, duration: Long, jobResult: String,
                          stagesSuccess: Int, stagesSkip: Int, stagesFail: Int, stagesRetry: Int, stagesTotal: Int,
                          tasksSuccess: Int, tasksTotal: Int, tasksSkip: Int,   stages: List[SparkUIStageInfo]) {
  def this(startEvent: SparkListenerJobStart, endEvent: SparkListenerJobEnd, stages: List[SparkUIStageInfo]) = {
    this(startEvent.jobId, startEvent.stageInfos.map(_.name).mkString("\n"), startEvent.time, endEvent.time - startEvent.time, endEvent.jobResult.toString,
      startEvent.stageInfos.count(_.getStatusString == "succeeded"),
      startEvent.stageInfos.count(_.getStatusString == "skipped"),
      startEvent.stageInfos.count(_.getStatusString == "failed"),
      stages.groupBy(_.stageId).count(_._2.length > 1),
      stages.length, stages.map(_.tasksSuccess).sum, stages.map(_.taskTotal).sum,
      startEvent.stageInfos.map(_.numTasks).sum - stages.map(_.taskTotal).sum,
      stages ::: startEvent.stageInfos.filterNot(si => stages.exists(_.stageId == si.stageId)).map(stage => new SparkUIStageInfo(stage, new SparkUITaskInfo(stage, Nil, Nil, Nil))).toList)
  }

  override def toString: String = {
    val desc = description.split("\n").dropWhile(_.trim == "").headOption.getOrElse("Unknown")
    val stageSummary = (List("skip", stagesSkip) :: List("fail", stagesFail) :: List("retry", stagesRetry) :: Nil).filter(_.last.toString.toInt > 0).map(_.mkString(" ")).mkString(",")
    String.format("%-10s%-100s%-22s%-12s%-24s%-24s%-36s",
      jobId.toString, desc, DateConvert.dateToDayTime(new Date(submittedTime)), Utils.msDurationToString(duration), jobResult,
      s"$stagesSuccess/$stagesTotal${if(stageSummary != "") s"($stageSummary)" else ""}", s"$tasksSuccess/$tasksTotal${if (tasksSkip > 0) s"(skip $tasksSkip)" else ""}")
  }
}

case class SparkUIApplicationInfo(appId: String, appAttemptId: String, appName: String, cores: Int, memoryPerNode: Long, coresMax: Int, submittedTime: Long, user: String, state: String, duration: Long, jobs: List[SparkUIJobInfo]) {
  override def toString: String = {
    String.format("%-26s%-50s%-10s%-10s%-10s%-22s%-10s%-18s%-12s", appId, appName, cores.toString, coresMax.toString, Utils.bytesToString(memoryPerNode), DateConvert.dateToDayTime(new Date(submittedTime)), user, state, Utils.msDurationToString(duration))
  }

  def isFinished: Boolean = if (state.toLowerCase == "finish") true else false
}

object SparkUIApplicationInfo extends SparkEventLogAnalyser {
  def apply(appStart: SparkListenerApplicationStart, appEnd: SparkListenerApplicationEnd, env: SparkListenerEnvironmentUpdate, jobs: List[SparkUIJobInfo]): SparkUIApplicationInfo = {
    val sysProp = env.environmentDetails.get("System Properties").getOrElse(Seq.empty[(String, String)])
    val splits = sysProp.find(_._1 == "sun.java.command").getOrElse("sun.java.command", "")._2.split(" ")
    //    val cores = splits.dropWhile(_.trim != "--total-executor-cores").drop(1).headOption.getOrElse("0").toInt
    val memoryPerNode = splits.dropWhile(_.trim != "--executor-memory").drop(1).headOption.getOrElse("0G").trim.toUpperCase match {
      case s if s.endsWith("P") => (s.dropRight(1).toDouble * 1024L * 1024 * 1024 * 1024 * 1024).toLong
      case s if s.endsWith("T") => (s.dropRight(1).toDouble * 1024L * 1024 * 1024 * 1024).toLong
      case s if s.endsWith("G") => (s.dropRight(1).toDouble * 1024L * 1024 * 1024).toLong
      case s if s.endsWith("M") => (s.dropRight(1).toDouble * 1024L * 1024).toLong
      case s if s.endsWith("K") => (s.dropRight(1).toDouble * 1024L).toLong
      case _@s => s.dropRight(1).toLong
    }
    val cores = env.environmentDetails.get("Spark Properties").getOrElse(Seq.empty[(String, String)]).find(_._1 == "spark.executor.instances").getOrElse("spark.executor.instances", "-1")._2.toInt
    val coresMax = env.environmentDetails.get("Spark Properties").getOrElse(Seq.empty[(String, String)]).find(_._1 == "spark.cores.max").getOrElse("spark.cores.max", "50")._2.toInt

    SparkUIApplicationInfo(appStart.appId.getOrElse("Unknown"), appStart.appAttemptId.getOrElse(""), appStart.appName, cores, memoryPerNode, coresMax, appStart.time, appStart.sparkUser, if (appEnd != null) "Finish" else "UnfinishedOrBroken", appEnd.time-appStart.time, jobs)
  }

  def apply(eventLog: File, charset: String = "UTF-8"): SparkUIApplicationInfo = {
    require(eventLog.exists(), s"SparkUIApplicationInfo construct fail for event log file=${eventLog.getPath} not exists")
    val sparkEventMap = getSparkEventsMap(eventLog, charset)
    require(sparkEventMap.nonEmpty, s"SparkUIApplicationInfo construct fail for spark envent map empty.")
    getSparkUIApplicationInfo(sparkEventMap.get)
  }
}


sealed trait SparkEventLogAnalyser {

  def sparkEventsFromFile(file: java.io.File, charset: String = "UTF-8"): List[SparkListenerEvent] = {
      FileUtils.withFileLineIterator(file, charset)(_.map(JsonProtocol.sparkEventFromJson).toList).filter(_!=null)
  }

  protected def getSparkEventsMap(eventLog: File, charset: String = "UTF-8") = {
    try {
      Some(sparkEventsFromFile(eventLog, charset).groupBy(_.getClass.getSimpleName))
    } catch {
      case e: Throwable => e.printStackTrace()
        None
    }
  }



  protected def getSparkUIApplicationInfo(sparkEventsMap: Map[String, List[SparkListenerEvent]]): SparkUIApplicationInfo = {
    val appStarts = getSparkSingleEvent[SparkListenerApplicationStart](sparkEventsMap, classOf[SparkListenerApplicationStart], null)
    require(appStarts != null, s"getSparkUIApplicationInfo fail for SparkListenerApplicationStart is null")
    val appEnds = getSparkSingleEvent[SparkListenerApplicationEnd](sparkEventsMap, classOf[SparkListenerApplicationEnd], null)
    val uiJobInfo = getSparkUIJobInfo(sparkEventsMap)
    val env = getSparkSingleEvent[SparkListenerEnvironmentUpdate](sparkEventsMap, classOf[SparkListenerEnvironmentUpdate], null)
    SparkUIApplicationInfo(appStarts, appEnds, env, uiJobInfo)
  }

  protected def getSparkUIJobInfo(sparkEventsMap: Map[String, List[SparkListenerEvent]]): List[SparkUIJobInfo] = {
    val jobStarts = getSparkEventsByClass[SparkListenerJobStart](sparkEventsMap, classOf[SparkListenerJobStart])
    val jobEnds = getSparkEventsByClass[SparkListenerJobEnd](sparkEventsMap, classOf[SparkListenerJobEnd])
    assertLengthEqual(jobStarts, jobEnds, "getSparkUIJobInfo")
    val uiStageInfo = getSparkUIStageInfo(sparkEventsMap)
    def setJobStageInfo(stage: StageInfo, uiStage: SparkUIStageInfo): Unit = {
      if (uiStage.submittedTime != 0) stage.submissionTime = Some(uiStage.submittedTime)
      if (uiStage.duration > 0) stage.completionTime = Some(uiStage.submittedTime + uiStage.duration)
      //      if (uiStage.stageResult == "failed") stage.stageFailed(uiStage.stageResult)
    }

    jobStarts.zip(jobEnds).map {
      case (start, end) => start.stageInfos.foreach(stage => uiStageInfo.filter(_.stageId == stage.stageId) match {
        case Nil =>
        case List(uiStage) => setJobStageInfo(stage, uiStage)
        case l => setJobStageInfo(stage, l.sortBy(_.attemptId).last)
      })
        new SparkUIJobInfo(start, end, uiStageInfo.filter(stage => start.stageIds.contains(stage.stageId)))
    }
  }

  protected def getSparkUIStageInfo(sparkEventsMap: Map[String, List[SparkListenerEvent]]): List[SparkUIStageInfo] = {
    val uiTaskInfo: List[SparkUITaskInfo] = getSparkUITaskInfo(sparkEventsMap)
    getSparkEventsByClass[SparkListenerStageCompleted](sparkEventsMap, classOf[SparkListenerStageCompleted]).map {
      stageComplete => val taskInfo = uiTaskInfo.find(stage => stage.stageId == stageComplete.stageInfo.stageId && stage.stageAttemptId == stageComplete.stageInfo.attemptId).getOrElse(null)
        new SparkUIStageInfo(stageComplete, taskInfo)
    }
  }

  protected def getSparkUITaskInfo(sparkEventsMap: Map[String, List[SparkListenerEvent]]): List[SparkUITaskInfo] = {
    val executorMap = getSparkEventsByClass[SparkListenerExecutorAdded](sparkEventsMap, classOf[SparkListenerExecutorAdded]).groupBy(_.executorId).map(kv => kv._1 -> kv._2.sortBy(_.time).last)
    val blockManagerMap = getSparkEventsByClass[SparkListenerBlockManagerAdded](sparkEventsMap, classOf[SparkListenerBlockManagerAdded]).groupBy(_.blockManagerId.executorId).map(kv => kv._1 -> kv._2.sortBy(_.time).last)
    getSparkEventsByClass[SparkListenerTaskEnd](sparkEventsMap, classOf[SparkListenerTaskEnd]).groupBy(_.stageId).map {
      case (stageId, tasks) => SparkUITaskInfo(stageId, tasks, executorMap, blockManagerMap)
      case _ => throw new Exception(s"getSparkUITaskInfo fail for tasksStageMap error")
    }.flatten.toList
  }

  private def getSparkEventsByClass[T](sparkEventsMap: Map[String, List[SparkListenerEvent]], cls: Class[T]): List[T] = {
    sparkEventsMap.get(cls.getSimpleName) match {
      case Some(l: List[T]) => l
      case _ => Nil
    }
  }

  private def getSparkSingleEvent[T <: SparkListenerEvent](sparkEventsMap: Map[String, List[SparkListenerEvent]], cls: Class[T], default: T): T = {
    getSparkEventsByClass[T](sparkEventsMap, cls).headOption match {
      case Some(start) => start
      case _ => default
    }
  }

  private def assertLengthEqual(starts: List[AnyRef], ends: List[AnyRef], functionName: String): Boolean = {
    require(starts.length == ends.length, s"$functionName fail for length not equal, starts.length=${starts.length}, ends.length=${ends.length}")
    true
  }
}