package cn.com.paic.tableDDL

import java.util

import org.apache.kudu.client.{CreateTableOptions, KuduTable}
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.kudu.{ColumnSchema, Schema, Type}
import org.apache.spark.sql.SparkSession


/**
 * Kudu与Spark集成，使用KuduContext创建表和删除表
 */
object _01KuduSparkTableDemo {
  /**
   * 创建Kudu表，指定名称
   *
   * @param tableName   表的名称
   * @param kuduContext KuduContext实例对象
   */
  def createKuduTable(tableName: String, kuduContext: KuduContext): Unit = {
    // a. 定义列表，存储ColumnSchema对象
    val columns: util.List[ColumnSchema] = util.Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("id",Type.INT32).key(true).build(),
      new ColumnSchema.ColumnSchemaBuilder("name",Type.STRING).key(false).build(),
      new ColumnSchema.ColumnSchemaBuilder("age",Type.INT32).key(false).build(),
      new ColumnSchema.ColumnSchemaBuilder("gender",Type.STRING).key(false).build()
    )
    // b. kudu表的Schema信息
    val schema: Schema = new Schema(columns)

    // c. 创建表的选项设置  id的列的哈希分区，3个分区，每个分区1个副本
    val options: CreateTableOptions = new CreateTableOptions()
    options.addHashPartitions(util.Arrays.asList("id"), 3)
    options.setNumReplicas(1)

    // TODO：调用创建表方法(3 个参数的)
    /*
      def createTable(
          tableName: String,
          schema: Schema,
          options: CreateTableOptions
      ): KuduTable
     */
    val kuduTable: KuduTable = kuduContext.createTable(tableName, schema, options)
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
