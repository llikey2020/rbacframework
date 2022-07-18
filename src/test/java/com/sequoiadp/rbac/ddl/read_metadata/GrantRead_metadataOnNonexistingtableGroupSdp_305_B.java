package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT READ_METADATA ON < non-existing table >to GROUP
 * @Author        : Lena
 */

public class GrantRead_metadataOnNonexistingtableGroupSdp_305_B extends SDPTestBase {
    public GrantRead_metadataOnNonexistingtableGroupSdp_305_B() {
        super.setTableName("tablea");
        super.hasGroup();
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadata","table","noexistingtable","group",getConfig("testGroup"));
            st1.executeQuery(grantsql);
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            
            String descsql = "desc table " + "noexistingtable";
            st2.executeQuery(descsql);
            

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            if(st2 != null) st2.close();
            conn1.close();
            if(conn2 != null) conn2.close();
        }
    }
}


