package cn.com.paic.write;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class ExcelWriteTest {

    /**
     * 向Excel文档中写数据(方式一)
     * 最简单的写
     */
    @Test
    public void writeExcel1() {
        //创建文件保存的位置,以及文件名
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user1.xlsx";

        /**
         * 构建要写入的数据
         * User类是一个自定义的特殊类,专门用来构建向Excel中写数据的类型类
         * @ExcelProperty是easyexcel提供的注解,用来定义表格的头部
         */
        List<User> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            User user = new User(i,"名字"+i,"男",Math.random()*10000,new Date());
            data.add(user);
        }

        //将数据写到Excel的第一个sheet标签中,并且给sheet标签起名字
        EasyExcel.write(fileName,User.class).sheet("用户信息").doWrite(data);
        //文件流会自动关闭
    }

    /**
     * 向Excel文档中写数据(方式二)
     * 最简单的写
     */
    @Test
    public void writeExcel2() {
        //创建文件保存的位置,以及文件名
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user2.xlsx";
        /**
         * 构建要写入的数据
         * User类是一个自定义的特殊类,专门用来构建向Excel中写数据的类型类
         * @ExcelProperty是easyexcel提供的注解,用来定义表格的头部
         */
        List<User> data = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            User user = new User(i,"名字"+i,"男",Math.random()*10000,new Date());
            data.add(user);
        }
        //创建Excel写对象
        ExcelWriter excelWriter = EasyExcel.write(fileName, User.class).build();
        //创建sheet对象
        WriteSheet writeSheet = EasyExcel.writerSheet("用户信息").build();
        //将数据写到sheet标签中
        excelWriter.write(data, writeSheet);
        //关闭流,文件流手动关闭
        excelWriter.finish();
    }
}
