package com.asto.dmp.shu.service

import com.asto.dmp.shu.base.{Constants, Contexts}
import com.asto.dmp.shu.util.Utils
import com.asto.dmp.shu.util.mail.MailAgent
import org.apache.spark.Logging

trait Service extends Logging {
  protected val sqlContext = Contexts.sqlContext
  protected var errorHend: String = s"${getClass.getSimpleName}的run()方法出现异常"

  protected def handlingExceptions(t: Throwable) {
    MailAgent(t, errorHend).sendMessage()
    logError(errorHend, t)
    Constants.App.ERROR_LOG.append(s"$errorHend\n${t.toString}\n${t.getStackTraceString}\n")
  }

  protected def printStartLog() = logInfo(Utils.logWrapper(s"开始运行${getClass.getSimpleName}的run()方法"))

  protected def printEndLog() = logInfo(Utils.logWrapper(s"${getClass.getSimpleName}的run()方法运行结束"))

  protected def runServices()

  def run() {
    try {
      printStartLog()
      runServices()
    } catch {
      case t: Throwable => handlingExceptions(t)
    } finally {
      printEndLog()
    }
  }
}
