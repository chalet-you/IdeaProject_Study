package cn.com.paic.tableDDL

import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


/**
 * Kudu与Spark集成，使用KuduContext创建表和删除表
 */
object _02KuduSparkTableDemo {
  /**
   * 创建Kudu表，指定名称
   *
   * @param tableName   表的名称
   * @param kuduContext KuduContext实例对象
   */
  def createKuduTable(tableName: String, kuduContext: KuduContext): Unit = {
    // a. 表的Schema信息
    val schema: StructType = StructType(
      Array(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("gender", StringType, nullable = true)
      )
    )
    // b. 表的主键
    val keys: Seq[String] = Seq("id")
    // c. 创建表的选项设置
    val options: CreateTableOptions = new CreateTableOptions()
    options.setNumReplicas(1)
    import scala.collection.JavaConverters._
    // TODO：主键列作为分区列
    options.addHashPartitions(keys.asJava, 3)

    // TODO 调用创建表方法（这里使用的是 4个参数创建kudu表）
    /*
      def createTable(
          tableName: String,
          schema: StructType,
          keys: Seq[String],
          options: CreateTableOptions
      ): KuduTable
     */
    val kuduTable: KuduTable = kuduContext.createTable(tableName, schema, keys, options)
    println("Kudu Table ID: " + kuduTable)
  }

  /**
   * 删除Kudu中表
   *
   * @param tableName   表的名称
   * @param kuduContext KuduContext实例对象
   */
  def dropKuduTable(tableName: String, kuduContext: KuduContext) = {
    if (kuduContext.tableExists(tableName)) {
      kuduContext.deleteTable(tableName)
    }
  }


  def main(args: Array[String]): Unit = {
    // 1. 构建SparkSession实例对象
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .config("spark.sql.shuffle.partitions", "2")
      .getOrCreate()

    // TODO: 创建KuduContext对象
    val kuduContext: KuduContext = new KuduContext("node2.itcast.cn:7051", spark.sparkContext)
    println(s"KuduContext: ${kuduContext}")

    // 任务1： 创建表
    createKuduTable("kudu_itcast_scala", kuduContext)

    // 任务2： 删除表
    //dropKuduTable("kudu_itcast_scala", kuduContext)


    // 应用结束，关闭资源
    spark.stop()
  }

}
