package cn.com.paic.read;

import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.excel.annotation.format.DateTimeFormat;
import com.alibaba.excel.annotation.format.NumberFormat;
import lombok.Data;

import java.util.Date;

/**
 * 定义需要被格式化的字段
 * 如果使用@NumberFormat("#.##")注解，建议数据类型采用String,如果使用double可能不能被格式化
 * @Author 夜泊
 * @BLOG https://hd1611756908.github.io/
 */
@Data
public class ConverterData {

	@NumberFormat("#.#")
	@ExcelProperty("数字标题")
	private String salary;
	@DateTimeFormat("yyyy-MM-dd HH:mm:ss")
	@ExcelProperty("日期标题")
	private Date hireDate;
}