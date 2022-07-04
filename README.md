# RBAC Test Framework

About rbac's testing framework


这是一个测试 rbac 的简单测试框架。 主要通过hive jdbc连接thriftserver，然后将需要的sql传递给thriftserver，配合testng框架判断并执行结果。

#### 背景

RBAC 是为 Spark SQL 提供的库表级权限控制服务。通过将 Spark SQL 中的数据库和表抽象为资源，分组抽象为角色，各种 SQL 语句抽象为行为，从而建立起了基于角色的访问控制模型，并以此进行权限控制，提供了用户对于 Spark SQL 特定功能进行授权及管理操作的能力， 目前我们的rbac和spark thriftserver交互，所以我们的自动化测试是通过hive的jdbc连接thriftserver，输入我们需要的sql，最后验证结果。



#### 依赖

spark需要关闭鉴权 才能够通过jdbc连接到thriftserver

        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.4</version>
        </dependency>
    
        <dependency>
            <groupId>org.testng</groupId>
            <artifactId>testng</artifactId>
            <version>6.13.1</version>
        </dependency>
    
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-catalyst</artifactId>
            <version>3.1.2</version>
        </dependency>
    
        <dependencies>
        <dependency>
            <groupId>org.apache.hive</groupId>
            <artifactId>hive-jdbc</artifactId>
            <version>2.3.7</version>
        </dependency>
    
        <dependency>
            <groupId>org.apache.hadoop</groupId>
            <artifactId>hadoop-common</artifactId>
            <version>2.7.4</version>
        </dependency>
    
        <dependency>
            <groupId>org.testng</groupId>
            <artifactId>testng</artifactId>
            <version>6.13.1</version>
        </dependency>
    
        <dependency>
            <groupId>org.apache.spark</groupId>
            <artifactId>spark-catalyst_2.13</artifactId>
            <version>3.2.1</version>
        </dependency>

#### 结构和功能

HiveConnection类：简单封装了测试用例需要的sql拼接和通过hive jdbc连接spark-thriftserver，

SDPTestBase：测试的基础类，主要做了testng的参数传递，和测试环境的准备问题

@BeforeSuite：参数的传递工作

@BeforeTest:初始化了测试用例所使用的database（先删除可能会发生影响的database和group）

@BeforeClass:初始化每一条用例的测试环境，包括table,usage权限，group的创建

@AfterClass:删除测试用例用的table,group等操作

@AfterTest:删除测试用的databse



#### 编写规范

1. 自动化测试用例一个类对应一条手工测试用例
2. 通过用例类名可以明显的看出对应testlink哪一条手工用例，可以使用特性名+testlink编号(用例id)的方式来命名
3. 自动化测试用例对表进行隔离，如果是对database进行操作的用例，无需继承SDPTestNase，其他的用例直接继承SDPTestNase类，通过自己的构造器来传入tablename和环境是否grant usage权限和创建group，tableName(tableName命名主要为testlink的编号，防止tablename重复)。
4. 当预期结果是出现异常的时候，可以使用@Test(expectedExceptions)来定义预期异常
5. @Test主测试用例的方法下需要实现两个用户的连接，通过用例的内容，来通过管理员grant,测试用户来进行校验
6. 配置文件目前内容为：hostname,port,rootuser,rootpwd,testuser,testpwd,dbname,group
7. 测试报告路径：target\test-output

