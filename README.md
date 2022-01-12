## 基础配置
| options |	必填	| 说明 |
| ----------- | ----------- | ----------- |
| --hosts |	YES |	集群IP，例如：localhost:9200 |
| --http_auth | NO | 认证信息，例如：elastic:elastic,格式为：user:password |
| --level | NO | 脚本输出的日志级别，可选：DEBUG、WARN、ERROR |
| --ssl | NO | 是否使用 ssl 加密连接 |
| --help | NO | 帮助，可以在每个Commands命令后面加上--help，获取每个模块帮助信息 |

## 命令模块
| Options | 说明 |
| ----------- | ----------- |
| lifecycle | 索引从hot节点到warm节点，从cold到删除，数据索引迁移模块 |
| ls | 列出由erotate管理的索引名称 |
| rm | 从erotate管理的索引名称中删除出去，不再管理 |
| rollover | ES rollover功能模块 |
| set |	设置一个由erotate管理的索引 |
| update | 更新erotate管理的索引，只更新指定字段 |
| inspect |	用来查看日均数据量，当前数据留存情况这些信息 |

## 模块使用

### set模块(重要)

>作用：新增索引管理，这个模块比较重要，如果需要管理索引的话必须先set新增进.rollover_indices索引里面才能使用 lifecycle 和 rollover 进行管理索引

#### 查看帮助

```shell
python erotate.py --hosts localhost:9200 --http_auth user:pass set  --help
```


#### 配置说明
| Options | 必带 | 说明 | 默认值 |
| ----------- | ----------- | ----------- | ----------- |
| --shards_nodes_ratio | NO | 分片与节点的比例，主分片数/节点数，单位：%  <br>1、单分片设置：0<br>2、分片与节点相同：100<br>3、分片为节点数的一半：50 | 无 |
| --number_of_replicas | NO | 副本数 | 1 |
| --rollover_size | NO | 分片大小，分片达到这个阀值的时候rollover | 25g |
| --pattern| NO | 后缀 | YYYY.MM.dd |
| --policy | NO | 冷热节点迁移规则，见下方说明 | 无 |
| --force-rollover | NO | 强制rollover，所输入实时生效 | 无 |
| --timezone | NO | 时区 | +08:00 |
| --help | NO | 该模块帮助说明 | 无 |

> --policy配置说明  
三个时间段分别为 `hot-days`,`warm-days`,`cold-days`，用逗号隔开，如  
例子一：  
`3,4,7`  
数据在hot节点**3**天，warm节点**4**天，cold节点**7**天 后删除  
例子二：  
`3,7`  
数据在hot节点**3**天，warm节点**7**天后删除   
注意：如果想不删除索引则可以使用**lifecycle模块的preserved_indices**配置进行

#### 使用方式

测试跑
```shell
python erotate.py  --hosts localhost:9200 --http_auth user:pass set --shards_nodes_ratio 100 --number_of_replicas 0 --rollover_size 50g  --policy 3,7 xxx
```
以上的意思为当旧索引分片为50G的时候rollover，rollover的时候分片数与节点数相同，副本为1，索引生命周期为热节点3天，冷节点7天然后删除,xxx为索引名

Response

![image-20191231010819824](../../../Library/Application Support/typora-user-images/image-20191231010819824.png)

这个时候去查看我们的数据

使用下面命令

GET test/_alias

![image-20191231012508013](../../../Library/Application Support/typora-user-images/image-20191231012508013.png)

二、update模块(重要)

     作用：可以根据名称对单配置进行更新

     		   如果需要对目前现有的配置进行修改，优先选择这个，可以指定单一修改，

                set也可以更新，但每次使用set更新的话必须带上所有配置，否则会被覆盖。

3、使用方式

测试跑看看

python erotate.py --hosts localhost:9200 --http_auth user:pass update --rollover_size 25g 别名

        以上的意思是修改rollover的分片触发阀值为25G

Response

![image-20191231011504770](../../../Library/Application Support/typora-user-images/image-20191231011504770.png)

三、lifecycle模块

定时任务

作用：做冷热迁移使用，需要定时调用，比如我们凌晨1点钟调用一次，一调用就会马上执行

1、查看帮助

python erotate.py --hosts localhost:9200 --http_auth user:pass lifecycle  --help

Response

![image-20191230235621626](../../../Library/Application Support/typora-user-images/image-20191230235621626.png)

2、配置说明
Optiones	必带	说明
--preserved_indices	NO	不能删除的索引，用英文逗号分隔
--only-steps	NO	有时候手动执行的时候希望只执行某个阶段，可选relocate 或 delete
--dry-run	NO	测试，会去执行一次脚本信息，但不会生效，类似DEBUG
--help	NO	该模块帮助说明

3、使用方式

测试跑看看，如果只需要看在哪些索引生效而不需要去集群里执行，可以使用--dry-run

python erotate.py --hosts localhost:9200 --http_auth user:pass lifecycle

四、rollover模块

定时任务

作用：ES rollover功能模块

1、查看帮助

python erotate.py --hosts localhost:9200 --http_auth user:pass rollover  --help

Response

![image-20191231002729633](../../../Library/Application Support/typora-user-images/image-20191231002729633.png)

2、配置说明
Optiones	必带	说明
--dry-run	NO	测试，会去执行一次脚本信息，但不会生效，类似DEBUG
--force-rollover-on-red	NO	索引状态为 red 时强制 rollover
--help	NO	该模块帮助说明

3、使用方式

测试跑看看，如果只需要看在哪些索引生效而不需要去集群里执行，可以使用--dry-run

python erotate.py --hosts localhost:9200 --http_auth user:pass rollover

五、rollover:force模块

作用：和rollover功能一样，但是是强制rollover，不管分片大小是否到达这个数

使用场景：

在某些时候需要对新进来的数据做改变而老的索引数据不做改变时可以使用这个，比如新数据进来没有副本，不去改变已存在的索引副本，这个时候就可以强制rollover一个新索引出来

1、查看帮助

python erotate.py --hosts localhost:9200 --http_auth user:pass rollover:force  --help

Response

![image-20191231003739905](../../../Library/Application Support/typora-user-images/image-20191231003739905.png)

2、配置说明
Optiones	必带	说明
--number_of_shards	NO	强制rollover自定义分片数
--number_of_replicas	NO	强制rollover自定义副本
--dry-run	NO	测试，会去执行一次脚本信息，但不会生效，类似DEBUG
--help	NO	该模块帮助说明

3、使用方式

测试跑看看，如果只需要看在哪些索引生效而不需要去集群里执行，可以使用--dry-run

python erotate.py --hosts localhost:9200 --http_auth user:pass rollover:force  别名

六、inspect模块

作用:用来查看日均数据量，当前数据留存情况这些信息

1、查看帮助

python erotate.py --hosts localhost:9200 --http_auth user:pass inspect --help

Response

![image-20200102152936626](../../../Library/Application Support/typora-user-images/image-20200102152936626.png)

2、配置说明
Optiones	必带	说明
--columns	NO	需要显示的列名,可选:
name、number_of_replicas、shards_nodes_ratio、
rollover_size、pattern、policy、avg_daily_pri_store、
real_policy、shard_usage需要使用英文逗号隔开
--help	NO	该模块帮助说明

3、使用方式

python erotate.py --hosts localhost:9200 --http_auth user:pass inspect

七、ls模块

作用：列出归脚本管理的索引，脚本所管理的数据都存到集群的.rollover_indices索引里面

1、使用方式

python erotate.py --hosts localhost:9200 --http_auth user:pass ls

Response

![image-20191231011742439](../../../Library/Application Support/typora-user-images/image-20191231011742439.png)

我们可以从response中看到我们在set设置的

八、rm模块

作用：从erotate管理的索引名称中删除出去，不再管理

只会删除.rollover_indices索引中的数据，不会删除现有的别名以及索引

1、使用方式

python erotate.py --hosts localhost:9200 --http_auth user:pass rm  别名

九、扫描模块

       定时扫描索引中的某个字段，事宜terms聚合之后与现有的template中的alias做比对，如果新增则新增alias，如果缺少某个则减少alias

1、查看帮助

python erotate.py  --hosts localbost:9200 --http_auth user:pass scan:alias --help

Response

![image-20200106113048621](../../../Library/Application Support/typora-user-images/image-20200106113048621.png)

2、配置说明
Optiones	必带	说明
name	YES	需要扫描的别名
--template	NO	template名称，默认：template_alias
--field	NO	区分别名数据字段，默认：kafka.topic
--help	NO	该模块帮助说明

3、使用说明

python erotate.py --hosts localhost:9200 --http_auth user:pass scan:alias test --field kafka.topic

![image-20200108002036974](../../../Library/Application Support/typora-user-images/image-20200108002036974.png)

打印的日志包含

● topics：扫描出的所有topic列表
● template：扫描了哪个template
● old_template：旧的template，备份使用
● new_template：新的template

使用例子

1、先建立脚本管理的别名

比如我们这里以common_index别名为例子

rollover的规则是

● 新索引分片数与节点数相同
● 0副本
● 分片大小为50G的时候做rollover

索引生命周期规则为

● 热节点3天，冷节点7天，7天后删除 最终创建命令如下

命令如下

python erotate.py --hosts localhost:9200 --http_auth user:pass set --shards_nodes_ratio 100 --number_of_replicas 0 --rollover_size 50g --policy 3,7 common_index

Response

![image-20200108003002812](../../../Library/Application Support/typora-user-images/image-20200108003002812.png)

可以看到创建成功，流程如下

1. 搜索是否有该别名，如果没有的话创建一个新的别名以及索引common_index-2020.01.02-000001
   通过_cat/indices接口可以看到我们的索引已经创建成功
2. 把is_write_index在创建的common_index-2020.01.02-000001索引上设置为true
3. 把配置数据写入到.rollover_indices
4. 在logstash的output端的索引名配置为common_index即可
5. 在我们的定时rollover的下一次定时任务调度就能检测到我们新创建的这个别名以及对应的索引

2、建立任务调度

通过crontab进行管理我们的rollover和lifecycle，和扫描脚本

执行以下编辑定时任务

crontab -e

新建rollover定时任务，定时5分钟检查一次rollover

*/5 * * * * /bin/python erotate.py --http_auth user:pass --hosts localhost:9200 rollover >> /var/log/curator/erotate_rollover.log 2>&1

新建定时数据迁移任务，定时每日凌晨1点钟数据迁移

0 1 * * * /bin/python erotate.py --http_auth user:pass --hosts localhost:9200 lifecycle >> /var/log/curator/erotate_lifecycle.log 2>&1

新建定时数据扫描任务，定时5分钟扫描一次topic列表

*/5 * * * * /bin/python erotate.py --http_auth user:pass --hosts localhost:9200 scan:alias  common_index >> /var/log/curator/erotate_scan.log 2>&1

其中common_index修改为需要扫描的别名即可

3、修改脚本管理的别名

比如我们想common_index这个别名下的rollover的规则改为分片大小为30G的时候rollover

python erotate.py --hosts 127.0.0.1:9200 --http_auth user:pass update --rollover_size 30g common_index

Response

![image-20200108003555124](../../../Library/Application Support/typora-user-images/image-20200108003555124.png)

可以看到已经修改成功

4、强制rollover

有的时候我们可能因为索引延迟我们需要强制rollover，需要⻢上新建一个索引出来我们可以这样

python erotate.py --hosts localhost:9200 --http_auth user:pass rollover:force common_index

Response

![image-20200108003752307](../../../Library/Application Support/typora-user-images/image-20200108003752307.png)

可以看到新创建了一个索引common_index-2020.01.08-000002

5、查看脚本管理的别名

执行以下命令

python erotate.py --hosts 127.0.0.1:9200 --http_auth user:pass ls

Response

![image-20200108003934026](../../../Library/Application Support/typora-user-images/image-20200108003934026.png)

我们可以看到可以从输出看到我们刚才设置的common_index这个别名以及配置

6、删除脚本管理的别名

执行以下的命令

python erotate.py --hosts localhost:9200 --http_auth user:pass rm common_index

Response

![image-20200108004049704](../../../Library/Application Support/typora-user-images/image-20200108004049704.png)

响应删除成功

注意:这里删除只会删除.rollover_indices索引中的数据，不会删除现有的别名以及索引，只会在
下次rollover以及数据迁移的时候不会被检测到而已

注意事项

一、脚本里的hot_warm_attr字段必须与集群的冷热标签相同，如果不一样需要修改和集群的冷热标签 一样

![image-20200108004156973](../../../Library/Application Support/typora-user-images/image-20200108004156973.png)

二、rollover不能对现有已有的别名进行操作，除非现有的别名已经在使用rollover进行管理
