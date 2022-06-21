package com.sequoiadp.rbac.ddl;


/*
 * @Description   :
 * @Author        :  Fangjun
 * @CreateTime    : 2022/6/6 16:45
 * @LastEditTime  : 2022/6/6 16:45
 * @LastEditors   :
 */

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class GrantModify extends SDPTestBase {

    public GrantModify(){
        super.setTableName("fgasdfga");
    }
    @Test
    public void test() throws  SQLException {
        Connection conn1=null,conn2=null;
        Statement st1 = null,st2 = null;

        try{
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1 = conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantmodifysql = HiveConnection.getInstance().grantSql("modify","table",tableName,"user",getConfig("testUser"));
            st1.executeQuery(grantmodifysql);

            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();

            String usage= HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st2.executeQuery(usage);

            String insertsql = "insert into " + getConfig("dbName")+"."+tableName + " values(1002,'safhhwrgbg');";
            st2.executeQuery(insertsql);

        }catch ( SQLException e){
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
