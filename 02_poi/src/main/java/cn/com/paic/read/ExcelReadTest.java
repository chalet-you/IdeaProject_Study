package cn.com.paic.read;

import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFDateUtil;
import org.apache.poi.hssf.usermodel.HSSFFormulaEvaluator;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.FileInputStream;
import java.util.Date;

public class ExcelReadTest {

    // TODO：HSSFWorkbook 读取 03 xls文件
    @Test
    public void testRead03() throws Exception {

        // 获取文件流
        FileInputStream fis = new FileInputStream("E:\\youxuan\\bigdata\\IdeaProject\\datas\\测试统计表03.xls");
        // 1、创建一个工作薄
        Workbook workbook = new HSSFWorkbook(fis);
        // 2、得到表
        Sheet sheet = workbook.getSheetAt(0);
        // 3、得到行
        Row row = sheet.getRow(0);
        Cell cell = row.getCell(1);
        // TODO：读取值的时候，一定需要注意类型！！
        // getStringCellValue() 获取字符串类型
//        System.out.println(cell.getStringCellValue());
        System.out.println(cell.getNumericCellValue());
        fis.close();

    }

    // TODO：XSSHWorkbook 读取 07 xlsx文件
    @Test
    public void testRead07() throws Exception {

        // 获取文件流
        FileInputStream fis = new FileInputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\测试统计表07.xlsx");
        // 1、创建一个工作薄
        Workbook workbook = new XSSFWorkbook(fis);
        // 2、得到表
        Sheet sheet = workbook.getSheetAt(0);
        // 3、得到行
        Row row = sheet.getRow(0);
        Cell cell = row.getCell(1);
        // TODO：读取值的时候，一定需要注意类型！！
        // getStringCellValue() 获取字符串类型
//        System.out.println(cell.getStringCellValue());
        System.out.println(cell.getNumericCellValue());
        fis.close();


    }

    // TODO：HSSFWorkbook 读取 03 xls文件
    @Test
    public void testCellType() throws Exception {

        // 获取文件流
        FileInputStream fis = new FileInputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\明细表.xls");
        // 1、创建一个工作薄
        Workbook workbook = new HSSFWorkbook(fis);
        // 2、得到表
        Sheet sheet = workbook.getSheetAt(0);
        // 3、获取标题
        Row rowTitle = sheet.getRow(0);
        if (rowTitle != null) {
            int cellCount = rowTitle.getPhysicalNumberOfCells();
            for (int cellNum = 0; cellNum < cellCount; cellNum++) {
                Cell cell = rowTitle.getCell(cellNum);
                if (cell != null) {
                    int cellType = cell.getCellType();
                    String cellValue = cell.getStringCellValue();
                    System.out.print(cellValue + " | ");
                }
            }
            System.out.println("");
        }
        // 获取表中的内容
        int rowCount = sheet.getPhysicalNumberOfRows();
        for (int rowNum = 1; rowNum < rowCount; rowNum++) {
            Row rowData = sheet.getRow(rowNum);
            if (rowData != null) {
                // 读取列
                int cellCount = rowTitle.getPhysicalNumberOfCells();
                for (int cellNum = 0; cellNum < cellCount; cellNum++) {
                    System.out.print("[" + (rowNum + 1) + "-" + (cellNum + 1) + "]");

                    Cell cell = rowData.getCell(cellNum);

                    // 匹配列的数据类型
                    if (cell != null) {
                        int cellType = cell.getCellType();

                        String cellValue = "";

                        switch (cellType){
                            case HSSFCell.CELL_TYPE_STRING : // 字符串
                                System.out.print("【String】");
                                cellValue = cell.getStringCellValue();
                                break;
                            case HSSFCell.CELL_TYPE_BOOLEAN:  // 布尔
                                System.out.print("【Boolean】");
                                cellValue = cell.getBooleanCellValue() +"";
                                break;
                            case HSSFCell.CELL_TYPE_BLANK:  // 空
                                System.out.print("【blank】");
                                break;
                            case HSSFCell.CELL_TYPE_NUMERIC: // 数字（日期、普通数字）
                                System.out.print("【numeric】");
                                if (HSSFDateUtil.isCellDateFormatted(cell)){ // 日期
                                    System.out.print("【日期】");
                                    Date date = cell.getDateCellValue();
                                    cellValue = new DateTime(date).toString("yyyy-MM-dd");
                                }else{
                                    // 不是日期格式，防止数字过长！
                                    System.out.print("【转换为字符串输出】");
                                    cell.setCellType(HSSFCell.CELL_TYPE_STRING);
                                    cellValue = cell.toString();
                                }
                                break;

                            case HSSFCell.CELL_TYPE_ERROR:
                                System.out.print("【数据类型错误】");
                                break;
                        }
                        System.out.println(cellValue);
                    }
                }
            }
        }


        fis.close();

    }

    @Test
    public void testFormula() throws Exception{
        FileInputStream fos = new FileInputStream("E:\\youxuan\\bigdata\\IdeaProject_Study\\datas\\公式.xls");
        Workbook workbook = new HSSFWorkbook(fos);
        Sheet sheet = workbook.getSheetAt(0);

        Row row = sheet.getRow(4);
        Cell cell = row.getCell(0);

        // 拿到计算公式
        FormulaEvaluator formulaEvaluator = new HSSFFormulaEvaluator((HSSFWorkbook) workbook);

        // 输出单元格内容
        int cellType = cell.getCellType();
        switch (cellType){
            case Cell.CELL_TYPE_FORMULA:
                String formula = cell.getCellFormula();
                System.out.println(formula);

                // 计算
                CellValue evaluate = formulaEvaluator.evaluate(cell);
                String cellValue = evaluate.formatAsString();
                System.out.println(cellValue);
                break;
        }

    }

}
