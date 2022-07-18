package com.sequoiadp.rbac.ddl.all;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/*
 * @Description   : GRANT ALL ON DATABASE TO USER <non existing username>
 * @Author        : Lena
 */

public class GrantAllOnDBToNonexistingUserSdp_291 extends SDPTestBase {

    public static final String TABLENAME = "newtablename";
    public GrantAllOnDBToNonexistingUserSdp_291() {
        super.setTableName("tablea");
    }
    //测试点
    @Test
    public void test() throws SQLException {
    	
        Connection conn1 = null;
        Statement st1 = null;
        try {
            //管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);
            String grantsql = HiveConnection.getInstance().grantSql("all","database",getConfig("dbName"),"user","nonexistinguser");
            st1.executeQuery(grantsql);
//            //测试用户test来验证管理员的语句
//            conn2 = HiveConnection.getInstance().getTestConnect();
//            st2 = conn2.createStatement();
//           
//            st2.executeQuery(usagesql);
//            
//            String descsql = "desc table " + tableName;
//            st2.executeQuery(descsql);
//            
//            String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),tableName);
//            st2.executeQuery(selectsql);
//            
//            String explainsql = "explain " + selectsql;
//            st2.executeQuery(explainsql);
//            
//            String insertsql = "insert into " + tableName + " values(1001);";
//            st2.executeQuery(insertsql);
//            
//            String updatesql = "update " + tableName + " set  id = 1002 where id = 1001;";
//            st2.executeQuery(updatesql);
//
//            String delsql = "delete from " + tableName + " where id = 1001;";
//            st2.executeQuery(delsql);
//            
//            String s3 = "s3a://sdbbucket2/" + TABLENAME;
//            String createtablsql = "create table  " + TABLENAME + "(id int)using delta location \"" + s3 + "\" " + ";" ;
//            st2.executeQuery(createtablsql);
//            
//            String droptablesql = HiveConnection.getInstance().dropSql("table",getConfig("dbName") + "." + TABLENAME );
//            st2.executeQuery(droptablesql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            conn1.close();

        }
    }
}
