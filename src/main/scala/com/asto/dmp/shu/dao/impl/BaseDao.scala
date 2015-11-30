package com.asto.dmp.shu.dao.impl

import com.asto.dmp.shu.base._
import com.asto.dmp.shu.dao.{Dao, SQL}

object BaseDao extends Dao {

  def getLoanStoreProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.LOAN_STORE, Constants.Schema.LOAN_STORE, "loan_store", sql)
}
