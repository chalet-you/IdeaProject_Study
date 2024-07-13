package cn.com.paic.write;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.FileOutputStream;

public class ExcelWriteTest {

    //TODO：HSSF只能操作 03版的excel，最多65536行数据
    @Test
    public void testWrite03() throws Exception{
        // 1、创建一个工作簿 03
        Workbook workbook = new HSSFWorkbook();
        // 2、创建一个工作表
        Sheet sheet = workbook.createSheet("测试统计表");
        // 3、创建一行 (1,1)
        Row row1 = sheet.createRow(0);
        // 4、创建一个单元格
        Cell cell11 = row1.createCell(0);
        cell11.setCellValue("今日新增观众");
        //  (1,2)
        Cell cell12 = row1.createCell(1);
        cell12.setCellValue(666);

        // 第二行 (2,1)
        Row row2 = sheet.createRow(1);
        Cell cell21 = row2.createCell(0);
        cell21.setCellValue("统计时间");
        //  (2,2)
        Cell cell22 = row2.createCell(1);
        String time = new DateTime().toString("yyyy-MM-dd HH:mm:ss");
        cell22.setCellValue(time);

        // 生成一张表（IO 流） 03 版本就是使用 xls结尾
        FileOutputStream fos = new FileOutputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\测试统计表03.xls");

        workbook.write(fos);
        // 关闭流
        fos.close();
        System.out.println("测试统计表03 生成完毕!");

    }

    //TODO：XSSF只能操作 07版的excel，最多1048576行数据
    @Test
    public void testWrite07() throws Exception{
        // 1、创建一个工作簿 07
        Workbook workbook = new XSSFWorkbook();
        // 2、创建一个工作表
        Sheet sheet = workbook.createSheet("测试统计表");
        // 3、创建一行 (1,1)
        Row row1 = sheet.createRow(0);
        // 4、创建一个单元格
        Cell cell11 = row1.createCell(0);
        cell11.setCellValue("今日新增观众");
        //  (1,2)
        Cell cell12 = row1.createCell(1);
        cell12.setCellValue(666);

        // 第二行 (2,1)
        Row row2 = sheet.createRow(1);
        Cell cell21 = row2.createCell(0);
        cell21.setCellValue("统计时间");
        //  (2,2)
        Cell cell22 = row2.createCell(1);
        String time = new DateTime().toString("yyyy-MM-dd HH:mm:ss");
        cell22.setCellValue(time);

        // 生成一张表（IO 流） 07 版本就是使用 xlsx结尾
        FileOutputStream fos = new FileOutputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\测试统计表07.xlsx");

        workbook.write(fos);
        // 关闭流
        fos.close();
        System.out.println("测试统计表07 生成完毕!");

    }

    // TODO 大文件写HSSF，最多只能处理 65536 行，否则会抛出异常
    // TODO 过程中写入缓存，不操作磁盘，最后一次性写入磁盘，速度快
    @Test
    public void testWrite03BigData() throws Exception{
        // 时间
        long start = System.currentTimeMillis();

        // 创建一个薄
        Workbook workbook = new HSSFWorkbook();
        // 创建表
        Sheet sheet = workbook.createSheet();
        for (int rowNum = 0; rowNum < 65536; rowNum++) {
            Row row = sheet.createRow(rowNum);
            for (int cellNum = 0; cellNum < 10; cellNum++) {
                Cell cell = row.createCell(cellNum);
                cell.setCellValue(cellNum);
            }
        }
        System.out.println("over");
        FileOutputStream fos = new FileOutputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\testWrite03BigData.xls");
        workbook.write(fos);
        fos.close();

        long end = System.currentTimeMillis();
        System.out.println((end-start) / 1000.0);
    }

    // TODO 大文件写XSSF，缺点：写数据时速度非常慢耗时长，非常耗内存，也会发生内存溢出，如 100万
    // TODO 优点：可以写较大的数据量，如 20万
    @Test
    public void testWrite07BigData() throws Exception{
        // 时间
        long start = System.currentTimeMillis();

        // 创建一个薄
        Workbook workbook = new XSSFWorkbook();
        // 创建表
        Sheet sheet = workbook.createSheet();
        for (int rowNum = 0; rowNum < 100000; rowNum++) {
            Row row = sheet.createRow(rowNum);
            for (int cellNum = 0; cellNum < 10; cellNum++) {
                Cell cell = row.createCell(cellNum);
                cell.setCellValue(cellNum);
            }
        }
        System.out.println("over");
        FileOutputStream fos = new FileOutputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\testWrite07BigData.xlsx");
        workbook.write(fos);
        fos.close();

        long end = System.currentTimeMillis();
        System.out.println((end-start) / 1000.0);
    }


    // TODO 大文件写SXSSF，优点：可以写非常大的数据，如 100万甚至更多，写数据速度快，占用更少的内存

    /** TODO ：注意
                   过程中会产生临时文件，需要清理临时文件
                   默认由 100条记录被保存在内存中，如果超过这数量，则最前面的数据被写入临时文件
     */
    @Test
    public void testWrite07BigDataS() throws Exception{
        // 时间
        long start = System.currentTimeMillis();

        // 创建一个薄
        Workbook workbook = new SXSSFWorkbook();
        // 创建表
        Sheet sheet = workbook.createSheet();
        for (int rowNum = 0; rowNum < 100000; rowNum++) {
            Row row = sheet.createRow(rowNum);
            for (int cellNum = 0; cellNum < 10; cellNum++) {
                Cell cell = row.createCell(cellNum);
                cell.setCellValue(cellNum);
            }
        }
        System.out.println("over");
        FileOutputStream fos = new FileOutputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\testWrite07BigDataS.xlsx");
        workbook.write(fos);
        fos.close();

        // todo 清除临时文件！
        ((SXSSFWorkbook)workbook).dispose();
        long end = System.currentTimeMillis();
        System.out.println((end-start) / 1000.0);
    }
}
