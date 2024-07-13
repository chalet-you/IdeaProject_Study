详细用法会在POI，EasyExcel统一讲解。

    HSSF 是Horrible SpreadSheet Format的缩写，也即“讨厌的电子表格格式”。 通过HSSF，你可以用纯Java代码来读取、写入、修改Excel文件。
    HSSF 为读取操作提供了两类API：usermodel和eventusermodel，即“用户模型”和“事件-用户模型”。前者很好理解，后者比较抽象，但操作效率要高得多。
    Java中最常见就是HSSF，SXXF，SXSSF，三者区别在于：

HSSF：Excel97-2003版本，扩展名为.xls。一个sheet最大行数65536，最大列数256。

XSSF：Excel2007版本开始，扩展名为.xlsx。一个sheet最大行数1048576，最大列数16384。

SXSSF：是在XSSF基础上，POI3.8版本开始提供的支持低内存占用的操作方式，扩展名为.xlsx。

HSSF用于Excel03版本：

    缺点：最多只能处理65536行，否则会报异常，
    
    优点：过程中写入缓存，不操作磁盘，最后一次性写入磁盘，速度快

SXXF用于Excel07版本：

    缺点：写数据时速度非常慢，非常耗内存，也会发生内存溢出，如100万条数据
    优点：可以写较大的数据量，如20万条数据

SXSSF可以理解为SXXF超大量数据升级版：

优点：可以写非常大量的数据库，如100万条甚至更多条，写数据速度快，占用更少的内存
注意：

过程中会产生临时文件，需要清理临时文件
默认由100条记录被保存在内存中，如果超出这数量，则最前面的数据被写入临时文件
如果想自定义内存中数据的数量，可以使用new SXSSFWorkbook（数量）
其他常见名称含义：

```
1.XSSF (XML SpreadSheet Format) – Used to reading and writting Open Office XML (XLSX) format files.
2.HSSF (Horrible SpreadSheet Format) – Use to read and write Microsoft Excel (XLS) format files.
3.HWPF (Horrible Word Processor Format) – to read and write Microsoft Word 97 (DOC) format files.
4.HSMF (Horrible Stupid Mail Format) – pure Java implementation for Microsoft Outlook MSG files
5.HDGF (Horrible DiaGram Format) – One of the first pure Java implementation for Microsoft Visio binary files.
6.HPSF (Horrible Property Set Format) – For reading “Document Summary” information from Microsoft Office files.
7.HSLF (Horrible Slide Layout Format) – a pure Java implementation for Microsoft PowerPoint files.
8.HPBF (Horrible PuBlisher Format) – Apache's pure Java implementation for Microsoft Publisher files.
9.DDF (Dreadful Drawing Format) – Apache POI package for decoding the Microsoft Office Drawing format.
```

