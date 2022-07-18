package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON VIEW TO GROUP <non existing groupname> 
 * @Author        : Lena
 */

public class GrantAllOnViewToNonexistingGroupSdp_282 extends SDPViewTestBase {
    public GrantAllOnViewToNonexistingGroupSdp_282() {
        super.setTableName("tablea");
        super.setViewName(this.getTableName() + "_vIew");
        super.hasGroup();
    }
    //测试点
    @Test(expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*Operation not allowed.*")
    public void test() throws SQLException {
        Connection conn1 = null;
        Statement st1 = null;
        Connection conn2 = null;
        Statement st2 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);

            String grantsqlview = HiveConnection.getInstance().grantSql("all","view",viewName,"group","nonexistinggroup");
            st1.executeQuery(grantsqlview);
            
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", "nonexistinggroup");
            st1.executeQuery(addgpusersql);
            

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

