package cn.com.paic.kudu.tableDDL;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
  * 基于Java API，创建kudu表，使用哈希分区建表
  */
public class _01KuduDataDemo {
    // 对于Kudu操作，获取KuduClient客户端实例对象
    KuduClient kuduClient = null;

    /**
     * 初始化KuduClient实例对象
     */
    @Before
    public void init() {
        // Kudu Master Servers 地址信息
        String masterAddresses = "node2.itcast.cn:7051";
        // 构建KuduClient实例对象
        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses) //
                // 设置超时时间间隔，默认值为10s
                .defaultSocketReadTimeoutMs(6000)
                // 采用建造者模式构建实例对象
                .build();
    }

    /**
     * 用于构建Kudu表中每列的字段信息Schema
     *
     * @param name  字段名称
     * @param type  字段类型
     * @param isKey 是否为Key
     * @return ColumnSchema对象
     */
    private ColumnSchema newColumnSchema(String name, Type type, boolean isKey) {
        // 创建ColumnSchemaBuilder实例对象
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        // 设置是否为主键
        column.key(isKey);
        // 构建	ColumnSchema
        return column.build();
    }

    /**
     * 创建Kudu中的表，表的结构如下所示：
     * create table itcast_user(
     * id int,
     * name string,
     * age byte,
     * primary key(id)
     * )
     * partition by hash(id) partitions 3
     * stored as kudu ;
     */
    @Test
    public void createKuduTable() throws KuduException {
        // a. 构建表的Schema信息
        List<ColumnSchema> columnSchemas = new ArrayList<ColumnSchema>();
        // public ColumnSchemaBuilder(String name, Type type)
        columnSchemas.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        // 自己封装一个方法：newColumnSchema 用来创建 ColumnSchema
        columnSchemas.add(newColumnSchema("name", Type.STRING, false));
        columnSchemas.add(newColumnSchema("age", Type.INT8, false));
        // 定义Schema信息
        Schema schema = new Schema(columnSchemas);

        // b. Kudu表的分区策略及分区副本数目设置
        CreateTableOptions tableOptions = new CreateTableOptions();
        // 哈希分区
        List<String> columns = new ArrayList<String>();
        columns.add("id"); // 按照id进行分区操作
        tableOptions.addHashPartitions(columns, 3); // TODO：根据id这列设置哈希分区，设置三个分区数buckets

        // 副本数设置
        // illegal replication factor 2 (replication factor must be odd)
        tableOptions.setNumReplicas(1);

		/*
			public KuduTable createTable(String name, Schema schema, CreateTableOptions builder)
		 */
        // c. 在Kudu中创建表
        KuduTable userTable = kuduClient.createTable("itcast_users_hash", schema, tableOptions);
        System.out.println(userTable.getTableId());
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