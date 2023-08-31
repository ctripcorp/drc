## 一、drc console 简介

### 提供DRC web界面方便管理操作
```txt
1. drc搭建
2. drc相关
3. 全量/增量的数据一致性校验
4. 冲突自动处理记录与手动处理功能
```

### DRC 指标的监控功能与信息同步

```txt
1. 双向复制延迟监控，gtidGap监控，ddl操作监控..
2. mysql主从信息同步......
```

## 二、drc console 本地部署

### 数据库初始化
- [init.sql](/console/src/test/resources/db/init.sql) 创建fxdrcmetadb 

- [init.sql](/console/src/test/resources/db/init.sql) 在需要进行复制的两个mysql实例下创建drcmonitordb 

- 作用：

  ```
  fxdrcmetadb作为console元信息数据库
  drcmonitordb 用于支持延迟监控和事务表模式复制
  ```

### 项目导入

#### 依赖的 maven 仓库 jar 包位置

1. 清空本地 maven 仓库的 com 和 org 目录
2. 所有携程相关的 maven 依赖放置在项目 mvn_repo 分支下
   git checkout mvn_repo
3. 在 drc 目录下, 运行 `sh install.sh`, 自动将非公共 maven 仓库的依赖装载在本地 maven 系统中

#### 某些类无法找到问题处理

- 在导入项目后，可能会发生某些类无法找到的现象,

- 问题原因:

  ```txt
  这些类是mvn编译时自动生成的
  请使用 mvn clean compile 在drc目录编译drc项目
  ```

#### SPI 加载实现类

- 携程内部drc_console 调用了dba,dalcluster等内部接口api

- 所有的console api调用都放在 /service/impl/api 目录下

- 在resources/META-INF/services/ 下替换实现类来加载默认的实现类或自己的实现类

- 包括：

  - Qconfig、ctrip.sso
  - beacon、dbcluster、mysqltool、ops等接口调用

- 例如

  ```txt
  com.ctrip.framework.drc.console.service.impl.api.BeaconApiServiceImpl
  #com.ctrip.framework.drc.console.service.impl.api.blankImpl.BlankBeaconApiServiceImpl
  ```


#### 项目启动

- console 使用springBoot 内置tomcat 启动，运行ConsoleApplication

- 前端使用vue ，在src/main/webapp/app/src 目录下运行 npm run serve 运行前端

  <img src="/docs/zh/images/drc_console_home.png" style="zoom: 80%;" />

#### DRC 搭建

- 目前开源版本暂时无法通过界面搭建drc集群，需要通过调用api搭建

- **一、新建Mha Group （DRC实例）**

  ```shell
  curl -H "Content-Type:application/json" -X POST -d '{
   "buName": "xxx",
   "dalClusterName":"xxx_dalcluster",
   "appid":xxx,
   "originalMha":"originalMhaName",
   "originalMhaDc":"sharb",
   "newBuiltMha": "newBuiltMhaName",
   "newBuiltMhaDc": "fraaws"
  }' 'http://127.0.0.1:8080/api/drc/v1/access/mha/standalone'
  
  ```

  **解释：**

  ```txt
  Mha: mysql高可用集群
  dalCluster: dalcluster集群名，逻辑上的数据库
  dc:数据中心
  uuid: mysql实例的server_uuid
  ```

  

- **二、配置MySQL实例信息**（注意：需要调两次接口，更新源端目标端的实例信息）
  
    ```shell
    curl -H "Content-Type:application/json" -X POST -d '{
     "mhaName": "originalMhaName",
     "zoneId":"sharb",
     "type":appid,
     "master":{
         "ip": "masterIp",
         "port": masterPort,
         "idc" : "sharb",
         "uuid": "master_server_uuid"
     },
     "slaves":[]
    }' 'http://127.0.0.1:8080/api/drc/v1/access/machine/standalone'
    
    ---------------------------------------------------------------------------
    
    curl -H "Content-Type:application/json" -X POST -d '{
     "mhaName": "newBuiltMha",
     "zoneId":"fraaws",
     "type":appid,
     "master":{
         "ip": "masterIp",
         "port": masterPort,
         "idc" : "fraaws",
         "uuid": "master_server_uuid"
     },
     "slaves":[]
    }' 'http://127.0.0.1:8080/api/drc/v1/access/machine/standalone'

- **三、配置Replicator和Applier**

  在console页面配置 DRC 集群，选择新建的drc实例 

  配置 Replicator 和Applier 如图

  <img src="/docs/zh/images/drc_console_AR.png" style="zoom:90%;" />

  **解释：**

  ```txt
  Replicator： drc复制模块实例，从源Mha_master mysql数据
  Applier: drc填充模块，从对端dc的Replicator拉取数据，复制到目标mha_master
  includedDbs: 需要双向复制的表
  同步对象：使用正则表达式的选择复制表
  Gtid：MySQL5.6的新特性，Global Transaction Identifier，用于在binlog中唯一标识一个事务,可以理解为位点
  executedGtid: 通过“show master status\G;” 等sql查询gtid,用于为Replicator拉取binlog的初始点定位
  applyMode:
  默认Set_gtid（设置mysql_gtid保存来自对端的复制位点，需要较高的mysql权限）
  transaction_table（将对象复制的gtid保存在事务表中）
  ```

  **注意：需要在先搭Applier 和Replicator 模块 以及cluster_manager模块**

  <img src="/docs/zh/images/drc_console_resource.png" alt="image-20211208142140296" style="zoom: 67%;" />

- **四、验证**

  在两侧的mysql实例下的drcmonitordb.delayMonitor表可以看到两条数据一直刷新

  **解释：**

  ```txt
  console 会每秒向同dc的mysql更新监控时间 通过drc复制到对端
  ```

  