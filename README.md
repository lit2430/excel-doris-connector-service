###                        excel-doris-connector-service

#### 功能介绍：

该项目的主要功能是解析excel,将excel中的数据通过streamLoad的方式导入到Doris中。

#### 使用手册：

入口类`com.bigdata.executor.ExcelToDorisExecutor`。用户只需要修改配置文件路径和待上传excel文件路径

然后在`application.properties`中配置Doris的地址即可导入。

#### 注意事项：

1.使用时，需要提前在Doris中建好目标表。

2.excel中的字段不需要与Doris中的字段数量保持一致，比如Doris表中有a,b,c,d四个字段，但只想导入a,c两个字段的数据，那么Excel只需要有这a,c两个字段即可，顺序无所谓。

3.excel中的字段名必须是英文名，且与Doris中字段名保持一致。

#### 说明：

v1项目，目前没有完整的打包部署流程，有需要的自己打包部署。