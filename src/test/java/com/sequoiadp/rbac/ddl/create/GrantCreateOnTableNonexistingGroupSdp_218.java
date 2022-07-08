package com.sequoiadp.rbac.ddl.create;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT CREATE ON TABLE TO GROUP <non existing group name>
 * @Author        : Lena
 */

public class GrantCreateOnTableNonexistingGroupSdp_218 extends SDPTestBase {
    public GrantCreateOnTableNonexistingGroupSdp_218() {
        super.hasGroup();
    }
   
    public static final String TABLENAME = "newtablename";
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Operation not allowed.*")
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("create","table",TABLENAME,"group","nonexistinggroup");
            st1.executeQuery(grantsql);
        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();
        }
    }
}

