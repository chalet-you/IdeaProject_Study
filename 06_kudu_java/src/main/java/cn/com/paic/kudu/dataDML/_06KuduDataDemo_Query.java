package cn.com.paic.kudu.dataDML;


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class _06KuduDataDemo_Query {
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
     * 从Kudu表中全量加载数据
     */

    @Test
    public void selectDataFromKuduFullLoad() throws KuduException {
        // a.获取表的句柄KuduTable，根据表名称获取
        KuduTable kuduTable = kuduClient.openTable("itcast_users_hash");
        // b.获取表的扫描器，全量查询，整张表扫描数据
        KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();
        // c.遍历数据
        int index = 0;
        while (scanner.hasMoreRows()) {  // 表示的是判断是否还有 Tablet数据未读取
            System.out.println("index = " + index);
            // 获取每个Tablet中的数据
            RowResultIterator rowResults = scanner.nextRows();
            while (rowResults.hasNext()) {
                RowResult row = rowResults.next();
                // 根据字段名以及字段类型，获取每个字段的值
                int idValue = row.getInt("id");
                String nameValue = row.getString("name");
                byte ageValue = row.getByte("age");
                System.out.println("id = " + idValue + ", name = " + nameValue + ", age = " + ageValue);
            }

        }
    }

    /**
     * 从Kudu表中过滤加载数据
     *      设置过滤：比如只查询id和age两个字段的值，年龄age小于等于25，id大于1050
     */
    @Test
    public void queryKuduData() throws KuduException {
        // a. 依据表的名称获取KuduTable实例对象，操作表的句柄
        KuduTable kuduTable = kuduClient.openTable("itcast_users_hash");

        // b. 构建KuduScanner扫描器，进行查询数据，类似HBase中Scan类
        KuduScanner.KuduScannerBuilder scannerBuilder = kuduClient.newScannerBuilder(kuduTable);
        // TODO: 设置过滤：比如只查询id和age两个字段的值，年龄age小于25，id大于1050
        // i. 设置获取指定列的字段值
        List<String> columnNames = Arrays.asList("id","age");

        // iii. 设置条件 id > 2050
        KuduPredicate predicateId = KuduPredicate.newComparisonPredicate(
                newColumnSchema("id", Type.INT32, true), //
                KuduPredicate.ComparisonOp.GREATER, // greater 比较操作符，>
                1050
        );

        // ii. 设置条件 age <= 25
        KuduPredicate predicateAge = KuduPredicate.newComparisonPredicate(
                newColumnSchema("age", Type.INT8, false), //
                KuduPredicate.ComparisonOp.LESS_EQUAL, // less_equal 比较操作符，<=
                (byte) 25
        );
        scannerBuilder
                .setProjectedColumnNames(columnNames) // TODO：设置project，投影，选择字段
                .addPredicate(predicateAge)  // TODO：设置predicate，谓词，过滤条件
                .addPredicate(predicateId);
        KuduScanner kuduScanner = scannerBuilder.build();

        int batchTime = 1;
        // c. 迭代获取数据, 分批次(每个分区查询一次，封装在一起)返回查询的数据，判断是否有数据
        while (kuduScanner.hasMoreRows()) {
            System.out.println("Batch = " + batchTime++);
            // 获取返回中某批次数据，有多条数据，以迭代器形式返回
            RowResultIterator rowResults = kuduScanner.nextRows();

            // 从迭代器中获取每条数据
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                System.out.println(
                        "id = " + rowResult.getInt("id") +
                         ", age = " + rowResult.getByte("age")
                );
            }
        }

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
