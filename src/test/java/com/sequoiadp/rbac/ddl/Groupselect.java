package com.sequoiadp.rbac.ddl;/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/2 10:52
 * @LastEditTime  : 2022/6/2 10:52
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class Groupselect extends SDPTestBase {
    public Groupselect() {
        super.setTableName("xd5r9rg");
        super.hasGroup();
    }
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();


            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add",getConfig("testUser"));
            st1.executeQuery(addgpusersql);

            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);

            String grantsql = HiveConnection.getInstance().grantSql("select","table",tableName,"group",getConfig("testGroup"));
            st1.executeQuery(grantsql);
            //测试用户连接到thriftserver
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            st2.executeQuery(selectsql);


        } catch (  SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            st2.close();
            conn1.close();
            conn2.close();
        }
    }
}
