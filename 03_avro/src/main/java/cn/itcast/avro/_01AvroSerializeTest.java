package cn.itcast.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
/**
 Avro序列化演示

 */
public class _01AvroSerializeTest {
    /**
     * 1. 创建Bean对象
     * 2. 封装数据
     * 3. 序列化
     */
    public static void main(String[] args) throws IOException {
        // 1. 创建Bean对象，并封装数据
        User user1 = new User();
        user1.setName("张三");
        user1.setAge(18);
        user1.setAddress("北京");

        User user2 = new User("李四", 19, "上海");

        User user3 = User.newBuilder()
                .setName("王五")
                .setAge(20)
                .setAddress("广州")
                .build();

        // 2. 序列化 user1, user2 and user3 到磁盘中
        // TODO  2.1、定义序列化模式，规定schema
        SpecificDatumWriter<User> datumWriter = new SpecificDatumWriter<>(User.class);
//        SpecificDatumWriter<User> datumWriter = new SpecificDatumWriter<>(user1.getSchema());

        // TODO  2.2、创建序列化对象
        DataFileWriter<User> dataFileWriter = new DataFileWriter<>(datumWriter);

        // TODO 2.3、设置序列化路径，序列化到哪里去
        dataFileWriter.create(user1.getSchema(),new File("avro.text"));

        // TODO 2.4、封装要序列的数据
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);

        // TODO 2.5、关流
        dataFileWriter.close();



    }
}
