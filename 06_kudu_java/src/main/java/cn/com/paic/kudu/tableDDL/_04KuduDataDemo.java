package cn.com.paic.kudu.tableDDL;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.AlterTableResponse;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
  * 基于Java API，对kudu表添加一列和删除一列
  */
public class _04KuduDataDemo {
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
     *对Kudu中表进行修改，增加列：address，String
     *
     */
    @Test
    public void alterKuduTableAddColumn() throws KuduException {
        // 构建修改表的对象
        AlterTableOptions ato = new AlterTableOptions();
        // 添加列
        ato.addColumn("gender",Type.STRING,"male");
        // 添加列
        AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);

        System.out.println(response.getTableId());
    }

    /**
     * 对Kudu中表进行修改，删除列：gender
     */
    @Test
    public void alterKuduTableDropColumn() throws KuduException{
        // 构建修改表的对象
        AlterTableOptions ato = new AlterTableOptions();
        // 指定要删除的列
        ato.dropColumn("gender");
        // 添加列
        AlterTableResponse response = kuduClient.alterTable("itcast_users", ato);

        System.out.println(response.getTableId());
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