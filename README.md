# Drill Storage Plugin for IPFS


## 编译

模块可以独立编译，也可以放入Drill源码树中与Drill一起编译。另外，由于依赖修改过的IPFS的API，需要先在本地编译安装
java-ipfs-api，否则在编译过程中会出现找不到符号的错误。

### 独立编译

在模块的根目录下运行： `mvn -T 2C clean install -DskipTests -DcheckStyle.skip=true`

### 在Drill源码树中编译

将仓库克隆到Drill代码树的`contrib/storage-ipfs`目录下：

```
cd drill/contrib/
git clone https://git.code.tencent.com/drill-over-ipfs/drill-ipfs-storage.git storage-ipfs
```

编辑storage plugin模块的Parent POM (contrib/pom.xml)，在`<modules>`下添加这个插件：

```
  <modules>
    <module>storage-hbase</module>
    <module>format-maprdb</module>
    .....
    <module>storage-ipfs</module>
  </modules>
```

然后到Drill的代码树根目录下执行Build：

```
mvn -T 2C clean install -DskipTests
```

生成的jar包在storage-ipfs/target目录下


## 安装

若与Drill一起编译的，生成的Drill可执行文件位于`distribution/target/apache-drill-1.14.0`目录下
将整个目录复制到代码树之外，以便后续运行和测试，例如复制为`drill-run`

若独立编译的，从Drill的网站上下载Drill的安装包，解压到`drill-run`里。

将生成的`drill-ipfs-storage-{version}.jar`复制到`drill-run/jars`中。

ipfs的几个依赖包：

```
cid.jar
junit-4.12.jar
multiaddr.jar
multibase.jar
multihash.jar
hamcrest-core-1.3.jar
```

以及IPFS Java API本身的jar包`java-api-ipfs-v1.2.2.jar` 
一起复制到`drill-run/jars/3rdparty`目录下。

## 运行

### 嵌入式模式

首先设置Drill的hostname为机器的IP地址：

编辑`conf/drill-env.sh`中的`DRILL_HOST_NAME`，改为机器的IP。
若要在私有集群上工作的，改为内网IP，准备在公网上工作的，改为公网IP。

启动drill-embedded：

```
drill-run/bin/drill-embedded
```

启动IPFS daemon：

```
ipfs daemon &>/dev/null &
```

设置IPFS节点的`drill-ready`标志：

```
ipfs name publish $(\
  ipfs object patch add-link $(ipfs object new) "drill-ready" $(\
    printf "1" | ipfs object patch set-data $(ipfs object new)\
  )\
)
```

然后到Drill的webui (![](http://localhost:8047))的Storage 标签页下注册IPFS storage plugin

页面最下方有 New Storage Pulugin 编辑框，Name 填 `ipfs`，点击Create;
然后输入ipfs storage plugin的配置，默认配置在`storage-ipfs/src/resources/bootstrap-storage-plugins.json`
复制中间的一段：

```
ipfs : {
      type:"ipfs",
      host: "127.0.0.1",
      port: 5001,
      max-nodes-per-leaf: 3,
      ipfs-timeout: 5,
      enabled: false
    }
```

其中`max-nodes-per-leaf`是控制每个IPFS数据块最多由多少个节点来提供，设置较大的值容易实现并行化，但在查询的内容在IPFS网络中分布
较少的情况下容易导致IPFS超时；相反，设置较低的值可能导致并行化程度较低，但不易超时。`ipfs-timeout`就是控制IPFS的超时时间，单位为秒。

在私有集群的环境中，内容在每台节点上均匀分布的情况下，可以将`max-nodes-per-leaf`设置得大一些，`ipfs-timeout`则可以小一些；公网环境则相反。

编辑完成后，点Update，然后回到Storage主页面就可以看到ipfs插件已经注册到Drill里了。
点Enable就可以启用这个插件，在query里用ipfs的前缀可以指定使用ipfs存储引擎。

### 分布式模式

TODO