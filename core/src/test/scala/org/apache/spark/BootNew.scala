package org.apache.spark

import java.io._


import scala.collection.mutable
import scala.util.Try

/**
  * Created by 10130564 on 2017/4/18.
  */
object BootNew {
  def main(args: Array[String]) {
    val pathBase = if (args.length >= 1) {
      args(0)
    } else {
      """E:\spark-2.0.2-base\example-eventLog"""
    }

    //    val driverRst = new mutable.HashMap[Int, DriverCost]()
    //DriverLog.calc(s"${pathBase}/submitSparkAppFile")
    //    val executorRst = ExecutorLog.calc(s"${pathBase}/work")

    //    val path = s"${pathBase}/history"
    val path = pathBase

    //    println("taskName","taskId","appId","appStart","duration",
    //      "insertDriverCost,broadcastCost,getFileSchemaCost,getFileStatusCost,getFileSizeCost",
    //      "schedulerDelay,deserializationTime",
    //      "shuffleReadTime,executorRunTime,shuffleWriteTime",
    //      "resultSerializationTime,gettingResultTime,gcTime,iteratorTime,funcTime,sortTime,writeTime,hadoopRddTime",
    //      "batchedScanTime,scanExistingRDDTime,inMemScanTime,inMemRelationTime,parquetScanTime,parqeutNotBatchScanTime,windowTime,windowChildTime,codegenTime,shuffleReadStreamTime",
    //      "tableReadCost",
    //      "BroadcastHashJoinExec_time_Inner,BroadcastHashJoinExec_time_LeftOuter,BroadcastHashJoinExec_time_RightOuter,BroadcastHashJoinExec_time_LeftSemi,BroadcastHashJoinExec_LeftAnti,BroadcastHashJoinExec_time_Existence,BroadcastHashJoinExec_time_match,BroadcastHashJoinExec_time_codegen_match,SortMergeJoinExec_time_Inner,SortMergeJoinExec_time_LeftOuter,SortMergeJoinExec_time_RightOuter,SortMergeJoinExec_time_FullOuter,SortMergeJoinExec_time_LeftSemi,SortMergeJoinExec_time_LeftAnti,SortMergeJoinExec_time_Existence,SortExec_time_timSort,SortExec_time_RadixSort,ExpandExec_time_total,GenerateExec_time_case1,GenerateExec_time_case2,Broadcast_buildTime,Broadcast_collectTime",
    //      "hasgAggTime,sortAggregateTime,projectTime,projectTimeCodegen,filterTime,filterTimeCodegen,hashAggNonCodegen,hashAggNonCodegenChildCost")
    //    println("appId", "sortTime(ms)")
    println("taskName,taskId,appId,appStart,duration, cores, executor_max, executor_min, executor_sum," +
      "schedulerDelay,deserializationTime," +
      "resultSerializationTime,gettingResultTime,gcTime,executorRunTime,iteratorTime,funcTime," +
      "shuffleReadTime,shuffleWriteTime," +
      "batchedScanTime,pipelineTime,sortTime,insertSortRowCost,hashAggregateTime,hashAggNonCodegen,hashAggNonCodegenChildCost,sortAggregateTime," +
      "writeTime,hadoopRddTime,insertIntoHiveTableMetric,parquetfilescantimeMetric,notbatchparquetfilescantimeMetric," +
      "hiveTableScanTime1,hiveTableScanTime2,hiveTableScanTime3,executedCommandExec,tableReadTi," +
      "projectTime,projectTimeCodegen,inMemScanTime,inMemRelationTime,scanExistingRDDTime,filterTime,filterTimeCodegen,codegenTime,windowTime,windowChildTime,shuffleReadStreamTime," +
      "ShuffledHashJoinExec_time_Inner,ShuffledHashJoinExec_time_LeftOuter,ShuffledHashJoinExec_time_RightOuter,ShuffledHashJoinExec_time_LeftSemi,ShuffledHashJoinExec_LeftAnti,ShuffledHashJoinExec_time_Existence,ShuffledHashJoinExec_time_match," +
      "BroadcastHashJoinExec_time_Inner,BroadcastHashJoinExec_time_LeftOuter,BroadcastHashJoinExec_time_RightOuter,BroadcastHashJoinExec_time_LeftSemi,BroadcastHashJoinExec_LeftAnti,BroadcastHashJoinExec_time_Existence,BroadcastHashJoinExec_time_match," +
      "SortMergeJoinExec_time_Inner,SortMergeJoinExec_time_LeftOuter,SortMergeJoinExec_time_RightOuter,SortMergeJoinExec_time_LeftSemi,SortMergeJoinExec_LeftAnti,SortMergeJoinExec_time_Existence,SortMergeJoinExec_time_FullOuter," +    //比率
      "excutorTime_ratio,iteratorTime_ratio,funcTime_ratio,shuffleReadTime_ratio,shuffleWriteTime_ratio,batchedScanTime_ratio,pipelineTime_ratio,sortTime_ratio,insertSortRowCost_ratio," +
      "hashAggregateTime_ratio,hashAggNonCodegen_ratio,hashAggNonCodegenChildCost_ratio,sortAggregateTime_ratio,writeTime_ratio,hadoopRddTime_ratio,insertIntoHiveTableMetric_ratio," +
      "parquetfilescantimeMetric_ratio,notbatchparquetfilescantimeMetric_ratio,hiveTableScanTime1_ratio,hiveTableScanTime2_ratio,hiveTableScanTime3_ratio,executedCommandExec_ratio,tableReadTi_ratio,projectTime_ratio,projectTimeCodegen_ratio," +
      "inMemScanTime_ratio,inMemRelationTime_ratio,scanExistingRDDTime_ratio,filterTime_ratio,filterTimeCodegen_ratio,codegenTime_ratio,windowTime_ratio,windowChildTime_ratio,shuffleReadStreamTime_ratio," +
      "ShuffledHashJoinExec_time_Inner_ratio,ShuffledHashJoinExec_time_LeftOuter_ratio,ShuffledHashJoinExec_time_RightOuter_ratio,ShuffledHashJoinExec_time_LeftSemi_ratio,ShuffledHashJoinExec_LeftAnti_ratio,ShuffledHashJoinExec_time_Existence_ratio,ShuffledHashJoinExec_time_match_ratio," +
      "BroadcastHashJoinExec_time_Inner_ratio,BroadcastHashJoinExec_time_LeftOuter_ratio,BroadcastHashJoinExec_time_RightOuter_ratio,BroadcastHashJoinExec_time_LeftSemi_ratio,BroadcastHashJoinExec_LeftAnti_ratio,BroadcastHashJoinExec_time_Existence_ratio,BroadcastHashJoinExec_time_match_ratio," +
      "SortMergeJoinExec_time_Inner_ratio,SortMergeJoinExec_time_LeftOuter_ratio,SortMergeJoinExec_time_RightOuter_ratio,SortMergeJoinExec_time_LeftSemi_ratio,SortMergeJoinExec_LeftAnti_ratio,SortMergeJoinExec_time_Existence_ratio,SortMergeJoinExec_time_FullOuter_ratio")
    var errorAppId = List[String]()
    var errorFile = List[String]()

    def printEventTime(file: File) {
      try {
        val eventLog = SparkUIApplicationInfo.apply(file)


        //        var pipeLineTime: Long = 0

        val taskNameReg = """meta_task_[\d]*_([\d]*)_([\s\S]*)_[\s\S]*""".r
        var taskName = eventLog.appName
        var taskId = 0
        if (taskNameReg.findFirstIn(eventLog.appName).isDefined) {
          val taskNameReg(id, name) = eventLog.appName
          taskName = name
          taskId = id.toInt
        }
        if( !eventLog.isFinished) return

        eventLog.jobs.foreach { x =>


          x.stages.foreach(s => {
            if (s.taskInfo != null) {

              var taskNum = 0L
              var schedulerDelay: Long = 0
              var deserializationTime: Long = 0
              var shuffleReadTime: Long = 0
              var executorRunTime: Long = 0
              val excutorMap = new mutable.HashMap[String,Long]()
              var shuffleWriteTime: Long = 0
              var resultSerializationTime: Long = 0
              var gettingResultTime: Long = 0
              var gcTime: Long = 0
              var writeTime: Long = 0
              var hadoopRddTime: Long = 0
              var iteratorTime: Long = 0
              var funcTime: Long = 0

              var removeCost: Long = 0
              var sortTime: Long = 0
              var insertSortRowCost: Long = 0
              var batchedScanTime: Long = 0
              var scanExistingRDDTime: Long = 0
              var inMemScanTime: Long = 0
              var inMemRelationTime: Long = 0
              var parquetScanTime: Long = 0
              var parqeutNotBatchScanTime: Long = 0
              var windowTime: Long = 0
              var windowChildTime: Long = 0
              var codegenTime: Long = 0
              var shuffleReadStreamTime: Long = 0

              var BroadcastHashJoinExec_time_Inner: Long = 0
              var BroadcastHashJoinExec_time_LeftOuter: Long = 0
              var BroadcastHashJoinExec_time_RightOuter: Long = 0
              var BroadcastHashJoinExec_time_LeftSemi: Long = 0
              var BroadcastHashJoinExec_LeftAnti: Long = 0
              var BroadcastHashJoinExec_time_Existence: Long = 0
              var BroadcastHashJoinExec_time_match: Long = 0
              //        var BroadcastHashJoinExec_time_codegen_match: Long = 0

              var SortMergeJoinExec_time_Inner: Long = 0
              var SortMergeJoinExec_time_LeftOuter: Long = 0
              var SortMergeJoinExec_time_RightOuter: Long = 0
              var SortMergeJoinExec_time_FullOuter: Long = 0
              var SortMergeJoinExec_time_LeftSemi: Long = 0
              var SortMergeJoinExec_time_LeftAnti: Long = 0
              var SortMergeJoinExec_time_Existence: Long = 0

              //          var SortExec_time_timSort: Long = 0
              //          var SortExec_time_RadixSort: Long = 0
              //          var ExpandExec_time_total: Long = 0
              //          var GenerateExec_time_case1: Long = 0
              //          var GenerateExec_time_case2: Long = 0
              //          var buildTime: Long = 0
              //          var collectTime: Long = 0


              var ShuffledHashJoinExec_time_Inner: Long = 0
              var ShuffledHashJoinExec_time_LeftOuter: Long = 0
              var ShuffledHashJoinExec_time_RightOuter: Long = 0
              var ShuffledHashJoinExec_time_LeftSemi: Long = 0
              var ShuffledHashJoinExec_LeftAnti: Long = 0
              var ShuffledHashJoinExec_time_Existence: Long = 0
              var ShuffledHashJoinExec_time_match: Long = 0

              var hashAggregateTime: Long = 0
              var hashAggNonCodegen: Long = 0
              var hashAggNonCodegenChildCost: Long = 0
              var sortAggregateTime: Long = 0

              var projectTime: Long = 0
              var projectTimeCodegen: Long = 0
              var filterTime: Long = 0
              var filterTimeCodegen: Long = 0

              var insertIntoHiveTableMetric: Long = 0
              var parquetfilescantimeMetric: Long = 0
              var notbatchparquetfilescantimeMetric: Long = 0
              var hiveTableScanTime1: Long = 0
              var hiveTableScanTime2: Long = 0
              var hiveTableScanTime3: Long = 0
              var executedCommandExec: Long = 0
              var tableReadTi: Long = 0
              s.taskInfo.taskMetrics.foreach(t => {
                taskNum += 1
                schedulerDelay += t.schedulerDelay
                deserializationTime += t.taskDeserializationTime
                resultSerializationTime += t.resultSerializationTime
                gettingResultTime += t.gettingResultTime
                gcTime += t.gcTime
                executorRunTime += t.executorRunTime
                if(excutorMap.contains(t.executorId)) {
                  excutorMap(t.executorId) += t.executorRunTime
                } else {
                  excutorMap(t.executorId) = t.executorRunTime
                }
                shuffleReadTime += t.shuffleReadWait*1000000
                shuffleWriteTime += t.shuffleWriteTime
                writeTime += t.writeTime
                hadoopRddTime += t.hadoopRddTime
                iteratorTime += t.iteratorTime
                funcTime += t.funcTime
                t.externalAccums.foreach { acc =>
                  acc.name match {
                    case Some(metric) =>

                      if (metric.equalsIgnoreCase("scan time total (min, med, max)")) {  //BatchedDataSourceScanExec   batched data source
                        batchedScanTime += acc.update.getOrElse(0).toString.toLong*1000000
                      } else if (metric.equalsIgnoreCase("duration total (min, med, max)")) {  //WholeStageCodegenExec  pipelinetime  这个指标统计不准确，可以不关注
                        removeCost += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("sort time total (min, med, max)")) {  //SortExec  包含doExcute和doProduce
                        sortTime += acc.update.getOrElse(0).toString.toLong *1000000
                      } else if (metric.equalsIgnoreCase("time to build hash map total (min, med, max)")) {   //ShuffledHashJoinExec buildHashedRelatio的时长
                        insertSortRowCost += acc.update.getOrElse(0).toString.toLong *1000000
                      }else if (metric.equalsIgnoreCase("aggregate time total (min, med, max)")) { //HashAggregateExec的codegen聚合时长
                        hashAggregateTime += acc.update.getOrElse(0).toString.toLong *1000000
                      }else if (metric.equalsIgnoreCase("hashAggNonCodegen total (min, med, max)")) { //HashAggregateExec的非codegen聚合时长
                        hashAggNonCodegen += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("hashAggNonCodegenChildCost total (min, med, max)")) { //HashAggregateExec的非codegen流程的非聚合时长（可以理解为读数据时间）
                        hashAggNonCodegenChildCost += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("sort aggregate time total (min, med, max)")) {//SortAggregateExec的非codegen聚合时长
                        sortAggregateTime += acc.update.getOrElse(0).toString.toLong
                      } else if (metric.equalsIgnoreCase("insertIntoHiveTable total (min, med, max)")) {
                        insertIntoHiveTableMetric += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("parquet file scan time")) {
                        parquetfilescantimeMetric += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("not batch parquet file scan time")) {
                        notbatchparquetfilescantimeMetric += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("hive table scan time 1 total (min, med, max)")) {
                        hiveTableScanTime1 += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("hive table scan time 2 total (min, med, max)")) {
                        hiveTableScanTime2 += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("hive table scan time 3 total (min, med, max)")) {
                        hiveTableScanTime3 += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("sideEffectResultExecutedCommandExec total (min, med, max)")) {
                        executedCommandExec += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("tableReadTi total (min, med, max)")) {
                        tableReadTi += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("project time total (min, med, max)")){
                        projectTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("project time codegen total (min, med, max)")) {
                        projectTimeCodegen += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("inMem table scan time total (min, med, max)")) {
                        inMemScanTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("inMem Relation time total (min, med, max)")) {
                        inMemRelationTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("scan existing RDD total (min, med, max)")) {
                        scanExistingRDDTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("filter time total (min, med, max)")) {
                        filterTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("filter time Codegen total (min, med, max)")) {
                        filterTimeCodegen += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("codegen time total (min, med, max)")) {
                        codegenTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("window time")) {
                        windowTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("window child time total (min, med, max)")) {
                        windowChildTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("shuffle read stream cost total (min, med, max)")) {
                        shuffleReadStreamTime += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_match total (min, med, max)")) {
                        ShuffledHashJoinExec_time_match += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_Inner total (min, med, max)")) {
                        ShuffledHashJoinExec_time_Inner += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_LeftOuter total (min, med, max)")) {
                        ShuffledHashJoinExec_time_LeftOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_RightOuter total (min, med, max)")) {
                        ShuffledHashJoinExec_time_RightOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_LeftSemi total (min, med, max)")) {
                        ShuffledHashJoinExec_time_LeftSemi += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_LeftAnti total (min, med, max)")) {
                        ShuffledHashJoinExec_LeftAnti += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("ShuffledHashJoinExec_time_Existence total (min, med, max)")) {
                        ShuffledHashJoinExec_time_Existence += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_match")) {
                        BroadcastHashJoinExec_time_match += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_Inner")) {
                        BroadcastHashJoinExec_time_Inner += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_LeftOuter")) {
                        BroadcastHashJoinExec_time_LeftOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_RightOuter")) {
                        BroadcastHashJoinExec_time_RightOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_LeftSemi")) {
                        BroadcastHashJoinExec_time_LeftSemi += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_LeftAnti")) {
                        BroadcastHashJoinExec_LeftAnti += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("BroadcastHashJoinExec_time_Existence")) {
                        BroadcastHashJoinExec_time_Existence += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_FullOuter")) {
                        SortMergeJoinExec_time_FullOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_Inner")) {
                        SortMergeJoinExec_time_Inner += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_LeftOuter")) {
                        SortMergeJoinExec_time_LeftOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_RightOuter")) {
                        SortMergeJoinExec_time_RightOuter += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_LeftSemi")) {
                        SortMergeJoinExec_time_LeftSemi += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_LeftAnti")) {
                        SortMergeJoinExec_time_LeftAnti += acc.update.getOrElse(0).toString.toLong
                      }else if (metric.equalsIgnoreCase("SortMergeJoinExec_time_Existence")) {
                        SortMergeJoinExec_time_Existence += acc.update.getOrElse(0).toString.toLong
                      }



                    case None =>
                  }
                }
              })


              val rstStr = s"$taskName,$taskId,${eventLog.appId},${eventLog.submittedTime},${eventLog.duration}, ${eventLog.coresMax}, ${Try(excutorMap.values.max).getOrElse(0)}, ${Try(excutorMap.values.min).getOrElse(0)}, ${excutorMap.values.sum}," +
                s"$schedulerDelay,$deserializationTime," +
                s"$resultSerializationTime,$gettingResultTime,$gcTime,$executorRunTime,${iteratorTime/1e6},${funcTime/1e6}," +
                s"${shuffleReadTime/ 1e6},${shuffleWriteTime / 1e6}," +
                s"${batchedScanTime/ 1e6},${removeCost / 1e6},${sortTime/ 1e6},${insertSortRowCost/ 1e6},${hashAggregateTime/ 1e6},${hashAggNonCodegen/1e6},${hashAggNonCodegenChildCost/1e6},${sortAggregateTime/1e6}," +
                s"${writeTime/1e6},${hadoopRddTime/1e6},${insertIntoHiveTableMetric/1e6},${parquetfilescantimeMetric/1e6},${notbatchparquetfilescantimeMetric/1e6}," +
                s"${hiveTableScanTime1/1e6},${hiveTableScanTime2/1e6},${hiveTableScanTime3/1e6},${executedCommandExec/1e6}, ${tableReadTi/1e6}," +
                s"${projectTime/1e6},${projectTimeCodegen/1e6}," +
                s"${inMemScanTime/1e6},${inMemRelationTime/1e6},${scanExistingRDDTime/1e6},${filterTime/1e6},${filterTimeCodegen/1e6},${codegenTime/1e6}," +
                s"${windowTime/1e6},${windowChildTime/1e6},${shuffleReadStreamTime/1e6}," +
                s"${ShuffledHashJoinExec_time_Inner/1e6},${ShuffledHashJoinExec_time_LeftOuter/1e6},${ShuffledHashJoinExec_time_RightOuter/1e6},${ShuffledHashJoinExec_time_LeftSemi/1e6},${ShuffledHashJoinExec_LeftAnti/1e6},${ShuffledHashJoinExec_time_Existence/1e6},${ShuffledHashJoinExec_time_match/1e6}," +
                s"${BroadcastHashJoinExec_time_Inner/1e6},${BroadcastHashJoinExec_time_LeftOuter/1e6},${BroadcastHashJoinExec_time_RightOuter/1e6},${BroadcastHashJoinExec_time_LeftSemi/1e6},${BroadcastHashJoinExec_LeftAnti/1e6},${BroadcastHashJoinExec_time_Existence/1e6},${BroadcastHashJoinExec_time_match/1e6}," +
                s"${SortMergeJoinExec_time_Inner/1e6},${SortMergeJoinExec_time_LeftOuter/1e6},${SortMergeJoinExec_time_RightOuter/1e6},${SortMergeJoinExec_time_LeftSemi/1e6},${SortMergeJoinExec_time_LeftAnti/1e6},${SortMergeJoinExec_time_Existence/1e6},${SortMergeJoinExec_time_FullOuter/1e6}," +   //比率
                s"${executorRunTime * 1.0/(eventLog.duration * eventLog.coresMax)},${iteratorTime/1e6/executorRunTime},${funcTime/1e6/executorRunTime},${shuffleReadTime/ 1e6/executorRunTime},${shuffleWriteTime /1e6/executorRunTime}," +
                s"${batchedScanTime/ 1e6/executorRunTime},${removeCost/ 1e6/executorRunTime},${sortTime/ 1e6/executorRunTime},${insertSortRowCost/ 1e6/executorRunTime},${hashAggregateTime/ 1e6/executorRunTime},${hashAggNonCodegen/1e6/executorRunTime},${hashAggNonCodegenChildCost/1e6/executorRunTime},${sortAggregateTime/1e6/executorRunTime}," +
                s"${writeTime/1e6/executorRunTime},${hadoopRddTime/1e6/executorRunTime},${insertIntoHiveTableMetric/1e6/executorRunTime},${parquetfilescantimeMetric/1e6/executorRunTime},${notbatchparquetfilescantimeMetric/1e6/executorRunTime}," +
                s"${hiveTableScanTime1/1e6/executorRunTime},${hiveTableScanTime2/1e6/executorRunTime},${hiveTableScanTime3/1e6/executorRunTime},${executedCommandExec/1e6/executorRunTime}, ${tableReadTi/1e6/executorRunTime}," +
                s"${projectTime/1e6/executorRunTime},${projectTimeCodegen/1e6/executorRunTime}," +
                s"${inMemScanTime/1e6/executorRunTime},${inMemRelationTime/1e6/executorRunTime},${scanExistingRDDTime/1e6/executorRunTime},${filterTime/1e6/executorRunTime},${filterTimeCodegen/1e6/executorRunTime},${codegenTime/1e6/executorRunTime}," +
                s"${windowTime/1e6/executorRunTime},${windowChildTime/1e6/executorRunTime},${shuffleReadStreamTime/1e6/executorRunTime}," +
                s"${ShuffledHashJoinExec_time_Inner/1e6/executorRunTime},${ShuffledHashJoinExec_time_LeftOuter/1e6/executorRunTime},${ShuffledHashJoinExec_time_RightOuter/1e6/executorRunTime},${ShuffledHashJoinExec_time_LeftSemi/1e6/executorRunTime},${ShuffledHashJoinExec_LeftAnti/1e6/executorRunTime},${ShuffledHashJoinExec_time_Existence/1e6/executorRunTime},${ShuffledHashJoinExec_time_match/1e6/executorRunTime}," +
                s"${BroadcastHashJoinExec_time_Inner/1e6/executorRunTime},${BroadcastHashJoinExec_time_LeftOuter/1e6/executorRunTime},${BroadcastHashJoinExec_time_RightOuter/1e6/executorRunTime},${BroadcastHashJoinExec_time_LeftSemi/1e6/executorRunTime},${BroadcastHashJoinExec_LeftAnti/1e6/executorRunTime},${BroadcastHashJoinExec_time_Existence/1e6/executorRunTime},${BroadcastHashJoinExec_time_match/1e6/executorRunTime}," +
                s"${SortMergeJoinExec_time_Inner/1e6/executorRunTime},${SortMergeJoinExec_time_LeftOuter/1e6/executorRunTime},${SortMergeJoinExec_time_RightOuter/1e6/executorRunTime},${SortMergeJoinExec_time_LeftSemi/1e6/executorRunTime},${SortMergeJoinExec_time_LeftAnti/1e6/executorRunTime},${SortMergeJoinExec_time_Existence/1e6/executorRunTime},${SortMergeJoinExec_time_FullOuter/1e6/executorRunTime}"
              println(rstStr)
            }
            else {
              errorAppId = s"${eventLog.appId}#${taskName}" :: errorAppId
            }

          })

        }

      } catch {
        case e: Throwable => errorFile = (file.getName + e.getMessage) :: errorFile

      }

    }

    new File(path).listFiles().filter(_.isFile).toList.par.foreach(printEventTime)
    //    new File(path).listFiles().filter(_.isFile).toList
    //      .map(f =>{
    //        if (f.getName.endsWith(".lz4")) {
    //          val lz4Decode = new LZ4BlockInputStream(new FileInputStream(f))
    //          val outFileName = f.getAbsolutePath.replace(".lz4","")
    //          val outFile = new File(outFileName)
    //          if( !outFile.exists()) {
    //            outFile.createNewFile()
    //          }
    //          val out = new FileOutputStream(outFile)
    //          copyStream(lz4Decode, out, true)
    //          new File(outFileName)
    //        }
    //        else f
    //      })
    //      .par.foreach(printEventTime)
    //    println("---------------------------------------")
    //    errorAppId.foreach(println)
    //    println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
    //    errorFile.foreach(println)
  }

  def copyStream(
                  in: InputStream,
                  out: OutputStream,
                  closeStreams: Boolean = false,
                  transferToEnabled: Boolean = false): Long = {
    try {

      var count = 0L
      val buf = new Array[Byte](8192)
      var n = 0
      while (n != -1) {
        n = in.read(buf)
        if (n != -1) {
          out.write(buf, 0, n)
          count += n
        }
      }
      count

    }
    finally {
      if (closeStreams) {
        try {
          in.close()
        } finally {
          out.close()
        }
      }
    }
  }
}
