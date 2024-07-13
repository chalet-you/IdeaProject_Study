package cn.com.paic.kudu.tableDDL;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
/**
  * 基于Java API，创建kudu表，使用多级分区建表
  */
public class _03KuduDataDemo {
    KuduClient kuduClient = null;

    /**
     * 初始化KuduClient实例对象
     */
    @Before
    public void init(){
        // Kudu Master Servers 地址信息
        String masterAddresses = "node2.itcast.cn:7051" ;
        // 构建KuduClient实例对象
        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses) //
                // 设置超时时间间隔，默认值为10s
                .defaultSocketReadTimeoutMs(6000)
                // 采用建造者模式构建实例对象
                .build() ;
    }

    /**
     * 用于构建Kudu表中每列的字段信息Schema
     * @param name 字段名称
     * @param type 字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey) {
        // 创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        // 设置是否为主键
        column.key(isKey) ;
        // 构建	ColumnSchema
        return column.build() ;
    }
    /**
     * 创建Kudu中的表，采用多级分区策略，结合哈希分区和范围分区组合使用
     */
    @Test
    public void createKuduTableMulti() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        columnSchemas.add(newColumnSchema("id", Type.INT32, true)) ;
        columnSchemas.add(newColumnSchema("age", Type.INT8, true)) ;
        columnSchemas.add(newColumnSchema("name", Type.STRING, false)) ;
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas) ;

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions() ;

        // TODO： e.1. 设置哈希分区
        List<String> columnsHash = new ArrayList<>() ;
        columnsHash.add("id") ;
        tableOptions.addHashPartitions(columnsHash, 5) ;

        // TODO: e.2. 设值范围分区
		/*
		  age 做 range分区，分3个区**
			- < 21（小于等于20岁）
			- 21 -  41（21岁到40岁）
			- 41（41岁以上，涵盖41岁）
		 */
        List<String> columnsRange = new ArrayList<>() ;
        columnsRange.add("age") ;
        tableOptions.setRangePartitionColumns(columnsRange) ;
        // 添加范围分区
        PartialRow upper21 = new PartialRow(schema) ;
        upper21.addByte("age", (byte)21);
        tableOptions.addRangePartition(new PartialRow(schema), upper21) ;

        // 添加范围分区
        PartialRow lower21 = new PartialRow(schema) ;
        lower21.addByte("age", (byte)21);
        PartialRow upper41 = new PartialRow(schema) ;
        upper41.addByte("age", (byte)41);
        tableOptions.addRangePartition(lower21, upper41) ;

        // 添加范围分区
        PartialRow lower41 = new PartialRow(schema) ;
        lower41.addByte("age", (byte)41);
        tableOptions.addRangePartition(lower41, new PartialRow(schema)) ;

        // 副本数设置
        tableOptions.setNumReplicas(1) ;

        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_multi", schema, tableOptions);
        System.out.println(userTable.toString());
    }

    /**
     * 删除Kudu中的表
     */
    @Test
    public void dropKuduTable() throws KuduException {
        // 判断表是否存在，如果存在即删除
        if(kuduClient.tableExists("itcast_users")){
            DeleteTableResponse response = kuduClient.deleteTable("itcast_users") ;
            System.out.println(response.getElapsedMillis());
        }
    }


    /**
     * 操作结束，关闭KuduClient
     */
    @After
    public void clean() throws KuduException {
        // 关闭KuduClient对象，释放资源
        if(null != kuduClient) kuduClient.close();
    }

}
