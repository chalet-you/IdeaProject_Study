package cn.itcast.avro;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * 使用 ByteArrayOutputStream 字节数组输出流实现 Avro 序列化和反序列化操作
 */
public class _03AvroSerializeDeserializeTest {
    public static void main(String[] args) throws IOException {
        // 1. 创建Bean对象，并封装数据
        User user = User.newBuilder()
                .setName("王五")
                .setAge(20)
                .setAddress("广州")
                .build();

        // 2. 序列化 user1 到内存中
        // TODO  2.1、定义序列化模式，规定schema
        SpecificDatumWriter<User> specificDatumWriter = new SpecificDatumWriter<>(User.class);
//        SpecificDatumWriter<User> specificDatumWriter = new SpecificDatumWriter<>(user.getSchema());

        // TODO  2.2、创建序列化对象
        //创建字节数组输出流
        ByteArrayOutputStream bos = new ByteArrayOutputStream();

        //获取编码对象
        BinaryEncoder binaryEncoder = EncoderFactory.get().directBinaryEncoder(bos, null);

        // TODO 2.3、设置序列化到内存中
        specificDatumWriter.write(user,binaryEncoder);

        // TODO 2.4、封装要序列的数据

        SpecificDatumReader<User> specificDatumReader = new SpecificDatumReader<>(User.class);

        //反序列化操作
        ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        BinaryDecoder binaryDecoder = DecoderFactory.get().directBinaryDecoder(bis, null);
        User user1 = specificDatumReader.read(null, binaryDecoder);
        System.out.println(user1);


        // TODO 2.5、关流
        bis.close();
        bos.close();
    }
}
