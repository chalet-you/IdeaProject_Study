package cn.com.paic.dataDML

import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
 * 对Kudu表的数据，进行CRUD操作
 */
object _03KuduSparkDataDemo {
	
	/**
	 * 向Kudu表中插入数据
	 */
	def insertData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		// a. 模拟产生数据
		// TODO: 当RDD或Seq中数据类型为元组时，直接调用toDF，指定列名称，转换为DataFrame
		val usersDF: DataFrame = spark.createDataFrame(
			Seq(
				(1001, "zhangsan", 23, "男"),
				(1002, "lisi", 22, "男"),
				(1003, "xiaohong", 24, "女"),
				(1004, "zhaoliu2", 33, "男")
			)
		).toDF("id", "name", "age", "gender")



		// b. 将数据保存至Kudu表
		kuduContext.insertRows(usersDF, tableName)
		
		// 更新数据、upsert数据和删除数据
		/*
			kuduContext.updateRows()
			kuduContext.upsertRows()
			kuduContext.deleteRows()
		*/
	}
	
	/**
	 * 从Kudu表中读取数据，封装到RDD数据集
	 */
	def selectData(spark: SparkSession, kuduContext: KuduContext, tableName: String): Unit = {
		/*
		  def kuduRDD(
		      sc: SparkContext,
		      tableName: String,
		      columnProjection: Seq[String] = Nil,
		      options: KuduReadOptions = KuduReadOptions()
		  ): RDD[Row]
		 */
		val rowsRDD: RDD[Row] = kuduContext.kuduRDD(spark.sparkContext, tableName, Seq("id", "name", "gender"))
		
		// 遍历RDD中数据
		rowsRDD.foreach{row =>
			println(s"id = ${row.getInt(0)}, " +
				s"name = ${row.getString(1)}, " +
				s"gender = ${row.getString(2)}, "
			)
		}
	}
	
	def main(args: Array[String]): Unit = {
		// 1. 构建SparkSession实例对象
		val spark: SparkSession = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[2]")
			.getOrCreate()
		
		// TODO: 创建KuduContext对象
		val kuduContext: KuduContext = new KuduContext("node2.itcast.cn:7051", spark.sparkContext)
		
		val tableName = "kudu_itcast_scala"
		
		// 插入数据
		//insertData(spark, kuduContext, tableName)
		
		// 查询数据
		selectData(spark, kuduContext, tableName)
		
		
		// 应用结束，关闭资源
		spark.stop()
	}
	
}
