package com.asto.dmp.shu.base

import com.asto.dmp.shu.dao.impl.{BaseDao, BizDao}
import com.asto.dmp.shu.service.impl.{CalculateService, PrepareService}
import com.asto.dmp.shu.util._
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if (argsIsIllegal(args)) return
    runServicesBy(args)
    closeResources()
    printEndLogs(startTime)
  }

  private def runServicesBy(args: Array[String]) {
    Constants.App.TIMESTAMP = args(0).toLong
    //从外部传入的是秒级别的时间戳，所以要乘以1000
    Constants.App.TODAY = DateUtils.timestampToStr(Constants.App.TIMESTAMP * 1000, "yyyyMM/dd")
   // new PrepareService().run()
    //BizDao.getTempCategoryAndShu.foreach(println)
    //BizDao.getSegSum.foreach(println)
   // CalculateService.generateMiddleFiles
    /*    BizDao.getCategory.foreach(println)
        BizDao.getCategoryDetails.foreach(println)*/

    /*BizDao.getDup.foreach(println)*/
    //BizDao.getShu.sortBy(t => (t._2,t._3))foreach(println)
    //BizDao.getDup2.foreach(println)

    BizDao.getTrendData.foreach(println)
  }

  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
    Contexts.stopSparkContext()
  }

  /**
   * 判断传入的参数是否合法
   */
  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length != 1) {
      logError(Utils.logWrapper("请传入程序参数:时间戳"))
      true
    } else {
      false
    }
  }

  /**
   * 打印程序运行的时间
   */
  private def printRunningTime(startTime: Long) {
    logInfo(Utils.logWrapper(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  /**
   * 如果程序在运行过程中出现错误。那么在程序的最后打印出这些错误。
   * 之所以这么做是因为，Spark的Info日志太多，往往会把错误的日志淹没。
   */
  private def printErrorLogsIfExist() {
    if (Constants.App.ERROR_LOG.toString != "") {
      logError(Utils.logWrapper(s"程序在运行过程中遇到了如下错误：${Constants.App.ERROR_LOG.toString}"))
    }
  }

  /**
   * 最后打印出一些提示日志
   */
  private def printEndLogs(startTime: Long): Unit = {
    printErrorLogsIfExist()
    printRunningTime(startTime: Long)
  }

}