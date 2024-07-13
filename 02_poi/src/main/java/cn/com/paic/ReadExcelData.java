package cn.com.paic;


import javafx.util.Pair;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import sun.security.provider.SHA;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ReadExcelData {
    public static void main(String[] args) throws Exception {
        String filePathExcel = "D:\\yx_job_file\\other\\BOM2.0【2022_11_07】.xlsx";
        String filePathMap = "D:\\Users\\EX-YOUXUAN001\\IdeaProjects\\NewIdeaProject1\\ph_rbdpp\\release\\20220512\\node_tablename.txt";

        // 获取节点名称 和 表名称
        HashMap<String, String> nodeNameAndTableName = readTxtFile(filePathMap);

        // 获取节点名称 和  表的中文描述
        HashMap<String, String> nodeNameAndComment = readExcel(filePathExcel);

        // 最后获取 表名称 和 表的中文描述
        for (Map.Entry<String, String> entry : nodeNameAndTableName.entrySet()) {
            String nodeName = entry.getKey();
            String tableName = entry.getValue();

            String comment = nodeNameAndComment.getOrDefault(nodeName, "无");
            System.out.println(tableName +"\t" + comment);
        }



    }

    // TODO 1：读取node_tablename.txt文件数据封装到HashMap集合中， 节点  ----> app表名

    public static HashMap<String, String> readTxtFile(String filePath) throws Exception {
        // 1.创建一个HashMap集合
        HashMap<String, String> nodeNameAndTableName = new HashMap<>();
        // 2.使用 Files.readAllLines(Path path) 一行一行读取文件封装到List集合中，每行代表集合一个元素
        List<String> allLines = Files.readAllLines(Paths.get(filePath));
        // 3.转换成流，使用Stream流来配合Lambda表达式来处理数据
        nodeNameAndTableName = allLines.stream()
                .map(line -> {
                    // 切割每一行，封装到元祖中
                    String[] arrs = line.split("\\s+");
                    Pair<String, String> pair = new Pair<>(arrs[0].trim(), arrs[1].trim());
                    return pair;
                })
                // 转换成HashMap
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue, String::concat, HashMap::new));

        return nodeNameAndTableName;
    }

    //TODO 2：读取 excel表，把节点名称和 app表中文描述封装到 HashMap中
    public static HashMap<String,String> readExcel(String filePath) throws Exception{
        // 0.创建要返回的HashMap对象
        HashMap<String, String> nodeNameAndComment = new HashMap<>();
        // 1.创建字节输入流并指向要处理的 excel表文件
        FileInputStream fis = new FileInputStream(filePath);
        // 2.创建一个 excel工作薄对象
        XSSFWorkbook workbook = new XSSFWorkbook(fis);
        // 3.从 excel簿中获取某个 sheet工作表
        XSSFSheet sheet = workbook.getSheet("BlazeBOM");
        // 4.获取此工作表中一共有多少行数据
        /**
         * TODO:
         *  getLastRowNum() ：读取总行数
         *  getLastRowNum()方法返回的是最后一行的索引，会比总行数少1
         */
        int rowNum = sheet.getLastRowNum();
        // 5.遍历 sheet工作表每一行
        for (int i = 0; i < rowNum; i++) {
            XSSFRow row = sheet.getRow(i);
            if (row == null){
                continue;
            }

            // 根据excel表中的列数，获取节点路径以及字段名以及字段的中文描述
            String nodeParent = row.getCell(0) == null ? "————" : row.getCell(0).toString().trim();
            String node1 = row.getCell(1) == null ? "————" : row.getCell(1).toString().trim();
            String node2 = row.getCell(2) == null ? "————" : row.getCell(2).toString().trim();
            String node3 = row.getCell(3) == null ? "————" : row.getCell(3).toString().trim();
            String node4 = row.getCell(4) == null ? "————" : row.getCell(4).toString().trim();
            String node5 = row.getCell(5) == null ? "————" : row.getCell(5).toString().trim();
            String fieldName = row.getCell(6) == null ? " " : row.getCell(6).toString().trim();
            String fieldComment = row.getCell(10) == null ? "————" : row.getCell(10).toString().trim();

            if (fieldName.replaceAll("————","").trim().isEmpty()){
                // 拼接 节点 路径，中间用 _
                String nodeName = nodeParent + "_" + node1 + "_" + node2 + "_" + node3 + "_" + node4 + "_" + node5;
                // 去掉 节点 路径中最后可能出现的 _    eg：Application_YSD_XSM_————_————_————
                nodeName = nodeName.replaceAll("(_————)+$","");
                //
                nodeNameAndComment.put(nodeName,fieldComment);
            }

        }
        return nodeNameAndComment;


    }


}
