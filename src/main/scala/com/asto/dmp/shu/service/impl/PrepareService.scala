package com.asto.dmp.shu.service.impl

import com.asto.dmp.shu.base.Constants
import com.asto.dmp.shu.dao.impl.BizDao
import com.asto.dmp.shu.service.Service
import com.asto.dmp.shu.util.FileUtils

object PrepareService {

}

class PrepareService extends Service {
  override protected def runServices(): Unit = {
    FileUtils.saveAsTextFile(BizDao.unionShu(),Constants.InputPath.SYCM_SHU_ALL)
  }
}
