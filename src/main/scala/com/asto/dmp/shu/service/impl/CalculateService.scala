package com.asto.dmp.shu.service.impl

import com.asto.dmp.shu.base.Constants
import com.asto.dmp.shu.dao.impl.BizDao
import com.asto.dmp.shu.service.Service
import com.asto.dmp.shu.util.{Utils, FileUtils}
import org.apache.spark.Logging

object CalculateService extends Logging {
  def generateMiddleFiles = {
    logInfo(Utils.logWrapper("开始产生中间文件"))
    FileUtils.saveAsTextFile(BizDao.getTempSegAndShu, Constants.OutputPath.TEMP_SEG_AND_SHU)
    FileUtils.saveAsTextFile(BizDao.getSegAndShu, Constants.OutputPath.SEG_AND_SHU)
    FileUtils.saveAsTextFile(BizDao.getNoDup, Constants.OutputPath.NO_DUP)
    FileUtils.saveAsTextFile(BizDao.getSegSum, Constants.OutputPath.SEG_SUM)
    FileUtils.saveAsTextFile(BizDao.getDup, Constants.OutputPath.DUP)
    FileUtils.saveAsTextFile(BizDao.getDup2, Constants.OutputPath.DUP2)
    FileUtils.saveAsTextFile(BizDao.getDup3, Constants.OutputPath.DUP3)
    FileUtils.saveAsTextFile(BizDao.getAllData, Constants.OutputPath.ALL_DATA)
    FileUtils.saveAsTextFile(BizDao.getModelData, Constants.OutputPath.MODEL_DATA)
    FileUtils.saveAsTextFile(BizDao.getSeasonIndex, Constants.OutputPath.SEASON_INDEX)
    FileUtils.saveAsTextFile(BizDao.getTrendData, Constants.OutputPath.SEASON_INDEX)

    logInfo(Utils.logWrapper("产生中间文件结束"))
  }
}

class CalculateService extends Service {
  override protected def runServices() = {
    if (Constants.App.SAVE_MIDDLE_FILES) CalculateService.generateMiddleFiles
  }
}
