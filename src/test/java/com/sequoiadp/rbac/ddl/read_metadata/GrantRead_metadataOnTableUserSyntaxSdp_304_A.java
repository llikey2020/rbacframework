package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : READ_METADATA ON TABLE privilege syntax validation to user
 * @Author        : Lena
 */

public class GrantRead_metadataOnTableUserSyntaxSdp_304_A extends SDPTestBase {
    public GrantRead_metadataOnTableUserSyntaxSdp_304_A() {
        super.setTableName("tablea");
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Operation not allowed.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("read_metadat","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            
            String descsql = "desc table " + tableName;
            st2.executeQuery(descsql);
            
            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            String explainsql = "explain" + selectsql;
            st2.executeQuery(explainsql);

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
