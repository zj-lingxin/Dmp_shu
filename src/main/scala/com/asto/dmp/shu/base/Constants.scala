package com.asto.dmp.shu.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "淘宝行业大类的被搜索数的月度预测"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/ycd"
    var TODAY: String = _
    var STORE_ID: String = _
    var RUN_CODE: String = _
    var TIMESTAMP: Long = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
    var MESSAGES: StringBuffer = new StringBuffer("")
  }
  
  object Hadoop {
    val JOBTRACKER_ADDRESS = "appcluster"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }
  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"

    private val OFFLINE_DIR = s"${App.DIR}/input/offline/${App.TODAY}"
    val SYCM_SHU = s"$OFFLINE_DIR/${App.TIMESTAMP}/datag_sycm_shu"
    val SYCM_SHU_NEW = s"$OFFLINE_DIR/${App.TIMESTAMP}/datag_sycm_shu_new"
    val LINKAGE = s"$OFFLINE_DIR/${App.TIMESTAMP}/linkage"
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val ONLINE_DIR = s"${App.DIR}/output/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    private val OFFLINE_DIR = s"${App.DIR}/output/offline/${App.TODAY}/${App.TIMESTAMP}"

  }

  /** 表的模式 **/
  object Schema {
    //关键字表：id,三级类目的主键ID（linkage表的主键, pid=49),三级类目的名称,抓取目标日期,淘宝指数,创建时间,更新时间
    val SYCM_SHU = "id,type_id,type_name,gmt_target,shu,gmt_created,gmt_modified,gmt_modified"
    val SYCM_SHU_NEW = SYCM_SHU
    val LINKAGE = "id,status,sort,type_id,pid,name,value,addtime,addip,sign_id,,,,"
  }
}
