package cn.com.paic.kudu.dataDML;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Random;

/**
 * 基于Java API对Kudu进行CRUD操作，包含创建表及删除表的操作
 */
public class _05KuduDataDemo_Insert {
    //对于Kudu操作，获取KuduClient客户端实例对象
    KuduClient kuduClient = null;


    @Before
    public void init() {
        String masterAddresses = "node2.itcast.cn:7051";
        kuduClient = new KuduClient.KuduClientBuilder(masterAddresses)
                .defaultOperationTimeoutMs(6000)
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
        ColumnSchema.ColumnSchemaBuilder column = new ColumnSchema.ColumnSchemaBuilder(name, type);
        return column.build();
    }

    /**
     * 将单条数据插入到Kudu Table中：
     *      INSERT INTO (id, name, age) VALUES (1001, "zhangsan", 26)
     */
    @Test
    public void insertKuduDataSingle() throws KuduException {
        // a. 获取KuduSession会话实例对象，真正进行数据插入操作
        KuduSession kuduSession = kuduClient.newSession();

        // b. 依据表的名称，获取表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users");

        // c. 构建Insert对象
        Insert insert = kuduTable.newInsert();
        // d. 获取Row对象，设置每条数据的值
        PartialRow insertRow = insert.getRow();
        insertRow.addInt("id", 1001);
        insertRow.addString("name", "itcast");
        insertRow.addByte("age", (byte)15);

        // e. 插入数据
        OperationResponse response = kuduSession.apply(insert);
        System.out.println(response.getElapsedMillis());

        // 关闭
        kuduSession.close();
    }

    /**
     * 将批量数据插入到Kudu Table中：
     *      INSERT INTO (id, name, age) VALUES (1001, "zhangsan", 26),(1002, "lisi", 27),(1003, "wangwu", 28), ......
     */
    @Test
    public void insertKuduDataBatch() throws KuduException {
        // a.获取KuduSession会话实例对象，真正进行数据插入操作
        KuduSession kuduSession = kuduClient.newSession();
        // b.依据表的名称，获取表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users_hash");
        // TODO：设置批量插入
        kuduSession.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        kuduSession.setMutationBufferSpace(3000);
        Random random = new Random();
        // 循环100次，产生100条数据
        for (int index = 0; index < 100; index++) {
            // c.构建Insert对象
            Insert insert = kuduTable.newInsert();
            // d.获取Row对象，设置每条数据的值
            PartialRow partiaRow = insert.getRow();
            partiaRow.addInt("id", 1001 + index);
            partiaRow.addString("name", "zhangsan_" + index);
            partiaRow.addByte("age", (byte) (18 + random.nextInt(10)));
            // 添加数据到批次
            kuduSession.apply(insert);
        }
        // 手动提交
        kuduSession.flush();
        // 关闭
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
