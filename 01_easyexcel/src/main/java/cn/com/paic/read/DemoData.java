package cn.com.paic.read;

import lombok.Data;

import java.util.Date;

/**
 * 承装Excel表格数据的类
 * 注意点: Java类中的属性的顺序和Excel中的列头的顺序是相同的
 * @Author 夜泊 
 * @BLOG   https://hd1611756908.github.io/
 */
@Data
public class DemoData {

	private String name;

	private Date hireDate;

	private Double salary;
}