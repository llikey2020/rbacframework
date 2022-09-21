package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : non Database Owner needs read_metadata privilege only to describe database
 * @Author        : Lena
 */

public class NonOwnerneedReadMetadataToDescDB extends SDPTestBase {

    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null,conn3 = null;
        Statement st1 = null,st2 = null,st3 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","database",getConfig("dbName"),"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            String descsql = "desc database " + getConfig("dbName");
            st2.executeQuery(descsql);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("nonowner"));
            st1.executeQuery(addgpusersql);
            String grantsql2 = HiveConnection.getInstance().grantSql("read_metadata","database",getConfig("dbName"),"user",getConfig("testGroup"));
            st1.executeQuery(grantsql2);
            
            //测试用户test来验证管理员的语句
            conn3 = HiveConnection.getInstance().getTestConnect();
            st3 = conn3.createStatement();
            String descsql2 = "desc database " + getConfig("dbName");
            st3.executeQuery(descsql2);
            
        } catch ( SQLException e) {
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

