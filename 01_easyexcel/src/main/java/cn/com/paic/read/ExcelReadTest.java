package cn.com.paic.read;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelReader;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.read.metadata.ReadSheet;
import org.junit.Test;

/**
 * @Author 夜泊
 * @BLOG   https://ukoko.gitee.io/2022/05/16/EasyExcel(%E7%94%9F%E6%88%90Excel%E6%8A%A5%E8%A1%A8)%E5%9F%BA%E6%9C%AC%E6%93%8D%E4%BD%9C/#more/
 */
public class ExcelReadTest {

    /*
     * 最简单的读取Excel内容(方式一)
     * 默认读取Excel文件中的第一个sheet
     */
    @Test
    public void testRead1() {
        //读取的文件路径
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user3.xlsx";
        Class<DemoData> head = DemoData.class; //创建一个数据格式来承装读取到数据
        //读取数据
        EasyExcel.read(fileName, head, new AnalysisEventListener<DemoData>() {

            /**
             * 解析每一条数据的时候被调用
             */
            @Override
            public void invoke(DemoData data, AnalysisContext context) {
                //在这里操作，将解析的每一条数据保存到数据库中,在这里可以调用数据库
                System.out.println("解析的数据为: "+data);
            }

            /**
             * 解析完所有数据的时候被调用
             */
            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                System.out.println("数据解析完成..........");
            }
        }).sheet().headRowNumber(2).doRead();// headRowNumber 不写，默认是从第一行读取的

    }



    /*
     * 最简单的读取Excel内容(方式二)
     * 默认读取Excel文件中的第一个sheet
     */
    @Test
    public void testRead2() {
        //读取的文件路径
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user3.xlsx";
        Class<DemoData> head = DemoData.class; //创建一个数据格式来承装读取到数据
        //创建ExcelReader对象
        ExcelReader excelReader = EasyExcel.read(fileName, head, new AnalysisEventListener<DemoData>() {

            @Override
            public void invoke(DemoData data, AnalysisContext context) {
                System.out.println("读取到的数据为:"+data);
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                System.out.println("数据解析已完成");
            }
        }).build();
        //创建sheet对象,并读取Excel的第1个sheet(下标从0开始)
        ReadSheet readSheet = EasyExcel.readSheet(0).headRowNumber(2).build();
        excelReader.read(readSheet);
        //关闭流操作，在读取文件时会创建临时文件,如果不关闭,磁盘爆掉
        excelReader.finish();
    }

    /*
     * 根据名称或者下标获取指定的Excel表格数据
     */
    @Test
    public void testRead3() {
        //读取的文件路径
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user3.xlsx";
        Class<IndexOrNameData> head = IndexOrNameData.class; //创建一个数据格式来承装读取到数据
        //读取数据
        EasyExcel.read(fileName, head, new AnalysisEventListener<IndexOrNameData>() {

            /**
             * 解析每一条数据的时候被调用
             */
            @Override
            public void invoke(IndexOrNameData data, AnalysisContext context) {
                //在这里操作，将解析的每一条数据保存到数据库中,在这里可以调用数据库
                System.out.println("解析的数据为: "+data);
            }

            /**
             * 解析完所有数据的时候被调用
             */
            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                System.out.println("数据解析完成..........");
            }
        }).sheet().headRowNumber(2).doRead();

    }


    /*
     * 格式化Excel中的数据格式(例如时间)
     */
    @Test
    public void testRead4() {
        //读取的文件路径
        String fileName="E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\user3.xlsx";
        Class<ConverterData> head = ConverterData.class; //创建一个数据格式来承装读取到数据
        //读取数据
        EasyExcel.read(fileName, head, new AnalysisEventListener<ConverterData>() {

            /**
             * 解析每一条数据的时候被调用
             */
            @Override
            public void invoke(ConverterData data, AnalysisContext context) {
                //在这里操作，将解析的每一条数据保存到数据库中,在这里可以调用数据库
                System.out.println("解析的数据为: "+data);
            }

            /**
             * 解析完所有数据的时候被调用
             */
            @Override
            public void doAfterAllAnalysed(AnalysisContext context) {
                System.out.println("数据解析完成..........");
            }
        }).sheet().headRowNumber(2).doRead();
    }

}
