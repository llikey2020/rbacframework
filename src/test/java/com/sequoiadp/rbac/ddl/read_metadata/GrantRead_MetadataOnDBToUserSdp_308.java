package com.sequoiadp.rbac.ddl.read_metadata;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT READ_METADATA ON DATABASE to USER
 * @Author        : Lena
 */

public class GrantRead_MetadataOnDBToUserSdp_308 extends SDPTestBase {
    public GrantRead_MetadataOnDBToUserSdp_308() {
        super.setTableName("tablea");
    }
    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;
        Statement st1 = null,st2 = null;
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
            st2.executeQuery(usagesql);
            String descsql = "desc table " + tableName;
            st2.executeQuery(descsql);
            
            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
            String explainsql = "explain " + selectsql;
            st2.executeQuery(explainsql);

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
