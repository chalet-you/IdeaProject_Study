package cn.com.paic.write;

import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.excel.annotation.format.DateTimeFormat;
import com.alibaba.excel.annotation.format.NumberFormat;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * 创建User类,用于构建向Excel表格中写数据的类型;
 * @ExcelProperty:这个注解是EasyExcel提供,用于生成Excel表格头
 *
 * 对于日期和数字有时候我们需要对其展示的样式进行格式化, easyexcel提供了一下注解
 * @DateTimeFormat : 日期格式化
 * @NumberFormat   : 数字格式化(小数或者百分数)
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class User {
	@ExcelProperty("用户编号")
	private Integer userId;

	@ExcelProperty("姓名")
	private String userName;

	@ExcelProperty("性别")
	private String gender;

	@ExcelProperty("工资")
	//格式化小数类型，如果时百分比那么定义为： #.##% 比如：9.12%
	@NumberFormat("#.##")
	private Double salary;

	@ExcelProperty("入职时间")
	//格式化时间
	@DateTimeFormat("yyyy年MM月dd日 HH时mm分ss秒")
	private Date hireDate;
	

}