package com.asto.dmp.shu.dao.impl

import com.asto.dmp.shu.dao.SQL
import com.asto.dmp.shu.util.DateUtils

object BizDao {
  def unionShu() = {
    BaseDao.getShuProps()
      .union(BaseDao.getShuProps())
      .map(a => (a(0).toString, a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString))
      .distinct()
  }

  /**
   * 获取行业分类
   * 注意:type_id = '47'的是行业分类
   */
  def getCategory = {
    BaseDao.getLinkageProps(SQL().select("id,type_id,name,sign_id").where("type_id = '47'"))
      .map(a => (a(0).toString, a(1).toString, convertCategoryNameFor(a(2).toString), a(3).toString))
  }

  /**
   * 获取二级行业分类
   * 注意:type_id = '48'的是二级行业分类
   */
  def getCategoryDetails = {
    BaseDao.getLinkageProps(SQL().select("pid,name,sign_id").where("type_id = '48'"))
      .map(a => (a(0).toString, a(1).toString, a(2).toString))
  }

  /**
   * 获取关键字
   * 注意:type_id = '49'的是关键字
   */
  def getKeyWords = {
    BaseDao.getLinkageProps(SQL().select("id,pid,name,sign_id").where("type_id = '49'")).map(a => (a(0).toString, a(1).toString, a(2).toString, a(3).toString))
  }

  /**
   * 获取关键字的查询数
   * 按关键字和年月分类,取近五年的数据
   * @return
   */
  def getShu = {
    BaseDao.getShuALLProps(SQL().select("type_id,type_name,gmt_target,shu").where(s"gmt_target < '${DateUtils.monthsAgo(0, "yyyy-MM-01 00:00:00")}' and gmt_target >= '${DateUtils.monthsAgo(61, "yyyy-MM-01 00:00:00")}'"))
      .map(a => (a(0).toString, a(1).toString, DateUtils.strToStr(a(2).toString, "yyyy-MM-dd hh:mm:ss", "yyyy-MM"), a(3).toString.toLong))
      .map(t => ((t._1, t._2, t._3), t._4))
      .groupByKey()
      .map(t => (t._1._1, t._1._2, t._1._3, t._2.sum))
  }

  /**
   * 返回：(服饰,连体裤,2011-07,4863271)
   */
  def getCategoryAndSearchNum = {
    getCategory.map(t => (t._4, (t._1, t._2, t._3)))
      .leftOuterJoin(getCategoryDetails.map(t => (t._1, (t._2, t._3)))) //(12,((1695,47,3C数码),Some((数码相机/单反相机/摄像机,1221))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._2.get._2, (t._2._1._1, t._2._1._2, t._2._1._3, t._1, t._2._2.get._1))) //(1221,(1695,47,3C数码,12,数码相机/单反相机/摄像机))
      .leftOuterJoin(getKeyWords.map(t => (t._2, (t._1, t._3, t._4)))) //(1221,((1695,47,3C数码,12,数码相机/单反相机/摄像机),Some((2039,单反相机,11221101))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._2.get._1, (t._2._1._3, t._2._1._5, t._2._2.get._2, t._2._2.get._3)))
      .leftOuterJoin(getShu.map(t => (t._1, (t._2, t._3, t._4)))) //(1845,((服饰,女士内衣/男士内衣/家居服,男士保暖内衣,11012114),Some((男士保暖内衣,2013-05,10989))))
      .filter(_._2._2.isDefined)
      .map(t => (t._2._1._1, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._1._2))
      .distinct().sortBy(t=>(t._1,t._2,t._3,t._4)).persist()
  }

  /**
   * 不考虑分类，将同一年月下的关键词出现的次数
   * 返回：((婚纱/礼服/旗袍,2012-06),3)
   */
  def getKeywordsNum = {
    val keywordsNum = getCategoryAndSearchNum.map(t => ((t._2, t._3), 1)).groupByKey().map(t => (t._1, t._2.sum)).persist()
    getCategoryAndSearchNum
      .map(t => ((t._2, t._3), (t._1, t._4)))
      .leftOuterJoin(keywordsNum) //((商务休闲鞋,2011-10),((鞋类箱包,18169),Some(1)))
      .map(t => (t._2._1._1, t._1._1, t._1._2, t._2._1._2, t._2._2.getOrElse(0))).sortBy(t=>(t._1, t._2,t._3,t._4)).persist()
  }

  /**
   * 返回：(服装内衣,文胸套装,2012-08,217542)
   */
  def getDup = {
    getKeywordsNum.filter(_._5 > 1).map(t => (t._1, t._2, t._3, t._4,t._5)).persist()
  }

  /**
   * 对file NoDup，取近五年的数据, 按分类分组后对shu求和），存为SegSum。
   * 返回：(服装内衣,女士棒球帽,2013-08,75393)
   */
  def getNoDup = {
    getKeywordsNum.filter(_._5 == 1).map(t => (t._1, t._2, t._3, t._4)).persist()
  }

  /**
   * 返回：(鞋包配饰,2414180935)
   * @return
   */
  def getShuByNoDupCategory = {
    getNoDup.map(t => (t._1, t._4)).groupByKey().map(t => (t._1, t._2.sum))
  }

  /**
   * getShuByNoDupCategory按Category拼接到getDup上
   * 返回：(服装内衣,Hodo/红豆,2014-03,133865,5486880354)
   */
  def getDup2 = {
    getDup.map(t => (t._1,(t._2,t._3,t._4)))
      .leftOuterJoin(getShuByNoDupCategory)
      .filter(_._2._2.isDefined)
      .map(t => (t._1,t._2._1._1,t._2._1._2,t._2._1._3,t._2._2.get)) //(服装内衣,Hodo/红豆,2014-03,133865,5486880354)
  }

  /**
   * 对Dup2，按年月和关键字分组下，对shu_t求和，记为变量shu_tt。计算新变量shu_n = shu * (shu_t / shu_tt)
   */
  def getDup3 = {
    //getDup2.map(t => ((t._2,t._3),))
  }

  /**
   * 该方法最好从文件中读取(有时间就修改)
   */
  def convertCategoryNameFor(oldCategoryName: String) = {
    oldCategoryName match {
      case "服饰" => "服装内衣"
      case "鞋类箱包" => "鞋包配饰"
      case "3C数码" => "手机数码"
      case "家装家具家纺" => "家纺居家/家具建材"
      case "家用电器" => "家电办公"
      case "母婴" => "母婴用品"
      case "运动户外" => "运动户外"
      case "食品" => "美食特产"
      case "化妆品(含美容工具)" => "护肤彩妆"
      case "图书音像" => "文化娱乐"
      case "汽车及配件" => "汽车摩托"
      case "居家日用" => "日用百货"
      case "珠宝配饰" => "珠宝手表"
      case "乐器" => "本地生活/虚拟服务/其他"
      case "服务大类" => "本地生活/虚拟服务/其他"
      case _ => oldCategoryName
    }
  }

}
