package cn.com.paic.dataDML

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * 编写SparkSQL程序，从Kudu表加载load数据，进行转换，最终保存到Kudu表中。
 */
object _04KuduSparkSQLDemo {

  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()
    import spark.implicits._

    // TODO: 2. 从Kudu表加载数据，采用read方式
    val kuduDF: DataFrame = spark.read
      .format("kudu")
      .option("kudu.master", "node2.itcast.cn:7051")
      .option("kudu.table", "kudu_itcast_scala")
      .load()
    kuduDF.printSchema()
    kuduDF.show(10, truncate = false)

    // TODO: 3. 数据转换 -> 自定义UDF函数，将gender转换为male或female
    val to_gender: UserDefinedFunction = udf(
      (gender: String) => {
        gender match {
          case "男" => "male"
          case "女" => "female"
          case _ => "未知"
        }
      }
    )
    // 将gender值替换
    val resultDF: DataFrame = kuduDF.withColumn("gender", to_gender($"gender"))

    // 4. 保存转换后数据到Kudu表
    resultDF
      .coalesce(1)
      .write
			// 数据的ETL一般都用这个 append保存模式
      .mode(SaveMode.Append)
      .format("kudu")
      .option("kudu.master", "node2.itcast.cn:7051")
      .option("kudu.table", "kudu_itcast_scala")
			// 操作类型是upsert，主键存在就更新，不存在就插入
      .option("kudu.operation", "upsert")
      .save()

    // 5. 应用结束，关闭资源
    spark.stop()
  }

}
