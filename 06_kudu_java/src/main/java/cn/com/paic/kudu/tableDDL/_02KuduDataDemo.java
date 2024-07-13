package cn.com.paic.kudu.tableDDL;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
  * 基于Java API，创建kudu表，使用范围分区建表
  */
public class _02KuduDataDemo {
    //对于Kudu操作，获取KuduClient客户端实例对象
    KuduClient kuduClient = null;

    /**
     * 初始化KuduClient实例对象
     */
    @Before
    public void init() {
        // todo Kudu Master Servers 地址信息
        String masterAddress = "node2.itcast.cn:7051";
        // todo 构建KuduClient实例对象
        kuduClient = new KuduClient.KuduClientBuilder(masterAddress)
                // 设置超时时间间隔，默认值为10s
                .defaultOperationTimeoutMs(60000)
                // 采用建造者模式构建实例对象
                .build();
    }

    /**
     * 用于构建Kudu表中每列的字段信息Schema
     * @param name 字段名称
     * @param type 字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey){
        // 创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name,type);
        //设置是否为主键
        column.key(isKey);
        return column.build();
    }
    /**
     * 创建Kudu中的表，采用范围分区策略
     */

    @Test
    public void createKuduTestRange() throws KuduException {
        List<ColumnSchema> columnSchemas = new ArrayList<>();
        columnSchemas.add(newColumnSchema("id",Type.INT32,true));
        columnSchemas.add(newColumnSchema("name",Type.STRING,false));
        columnSchemas.add(newColumnSchema("age",Type.INT8,false));
        // todo 定义Schema信息
        Schema schema = new Schema(columnSchemas);
        // Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();
        //哈希分区
        // TODO 按照 id 进行范围分区操作，分区键必须是主键或 主键的一部分
        tableOptions.setRangePartitionColumns(Arrays.asList("id")); // 设置范围分区列名称
        // TODO 设置分区范围，类似HBase表的Region预分区操作
        /**
         * id < 100   [0, 100)
         * 100 <= id < 500   [100,500)
         * 500 <= id         [500,+oo)
         */
        // value < 100

        PartialRow upper = new PartialRow(schema);
        upper.addInt("id",100);
        //添加分区范围
        tableOptions.addRangePartition(new PartialRow(schema),upper);
        // 100 <= value <500
        PartialRow lower100 = new PartialRow(schema) ;
        lower100.addInt("id", 100);
        PartialRow upper500 =  new PartialRow(schema)  ;
        upper500.addInt("id", 500);
        // 添加分区范围
        tableOptions.addRangePartition(lower100, upper500 );

        // 500 <= value
        PartialRow lower500 = new PartialRow(schema) ;
        lower500.addInt("id", 500);
        // 添加分区范围
        tableOptions.addRangePartition(lower500, new PartialRow(schema) );

        // 副本数设置
        // illegal replication factor 2 (replication factor must be odd)
        tableOptions.setNumReplicas(1) ;

        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_range", schema, tableOptions);
        System.out.println(userTable.toString());
    }

    /**
      * 操作结束，关闭KuduClient
     */
    @After
    public void clear() throws KuduException {
        if(null != kuduClient){
            kuduClient.close();
        }
    }

}
