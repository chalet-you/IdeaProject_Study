package cn.itcast.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.io.IOException;

/**
 Avro反序列化演示

 */
public class _02AvroDeserializeTest {
    /**
     * 1. 创建Bean对象
     * 2. 封装数据
     * 3. 反序列化 user1, user2 and user3
     */
    public static void main(String[] args) throws IOException {
        // TODO  1、定义反序列化模式，规定schema
        SpecificDatumReader<User> datumReader = new SpecificDatumReader<>(User.class);

        // TODO  2、创建反序列化对象
        DataFileReader<User> dataFileReader = new DataFileReader<>(new File("avro.text"), datumReader);

        // TODO 3、反序列对象，拿到数据
        while (dataFileReader.hasNext()){
            System.out.println(dataFileReader.next());
        }

        // TODO 4、关流
        dataFileReader.close();
    }
}
