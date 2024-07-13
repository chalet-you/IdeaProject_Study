package cn.com.paic.kudu.dataDML;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class _07KuduDataDemo_Update {
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
     * 更新Kudu Table表中的数据
     */
    @Test
    public void updateData() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users_hash");

        // b. 获取KuduSession，用于对集群进行交互，比如表的CRUD操作
        KuduSession kuduSession = kuduClient.newSession();
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);

        // c. 更新数据，获取Update实例对象
        Update update = kuduTable.newUpdate();

        PartialRow partialRow = update.getRow();
        partialRow.addInt("id", 1051);
        partialRow.addString("name", "小明-update");
        partialRow.addByte("age",(byte)12);

        // d. 插入数据到表中
        kuduSession.apply(update);
        kuduSession.flush();

        // e. 关闭Session会话
        kuduSession.close();
    }


    /**
     * 操作结束，关闭KuduClient
     */
    @After
    public void clean() throws KuduException {
        // 关闭KuduClient对象，释放资源
        if (null != kuduClient) kuduClient.close();
    }


}
