package com.sequoiadp.rbac.ddl.owner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;


public class OwnerShipTransferUserSdp_357 extends SDPTestBase {
    public OwnerShipTransferUserSdp_357() {
        super.setTableName("fasghghjj");
    }
    @Test (expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have grant privilege on.*")
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null,conn3 =  null;
        Statement st1 = null,st2 = null, st3 = null;
        try {
        	//管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);           
            String grantsql = HiveConnection.getInstance().grantSql("all","database",getConfig("dbName"),"user",getConfig("nonowner"));
            st1.executeQuery(grantsql);
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3 = conn3.createStatement();
            
            st3.executeQuery(usagesql);   
            
            String s3 = "s3a://sdbbucket2/" + "testtable";
            
            String createtablsql = "create table if not exists " + "testtable" + "(id int)using delta location \"" + s3 + "\" " + ";" ;
            st3.executeQuery(createtablsql);
            //插入数据
            String insertsql = "insert into " + "testtable" + " values(1001);";
            st3.executeQuery(insertsql);
            
            String transferowner = "ALTER table " + "testtable" + " OWNER TO user " +  getConfig("testUser");
            st3.executeQuery(transferowner);  
            st3.close();
            conn3.close();
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            st2.executeQuery(usagesql);    
            String transfergrantsql = HiveConnection.getInstance().grantSql("select","table", "testtable" ,"user",getConfig("nonowner"));
            st2.executeQuery(transfergrantsql);  
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3 = conn3.createStatement();
            String transfergrantsql2 = HiveConnection.getInstance().grantSql("select","table","testtable","user",getConfig("testUser"));
            st3.executeQuery(transfergrantsql2);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
			String dropsql = HiveConnection.getInstance().dropSql("table", getConfig("dbName") + "." + "testtable");
			st1.executeQuery(dropsql);
            st1.close();
            if(st2 != null) st2.close();
            if(st3 != null) st3.close();  
            conn1.close();
            if(conn2 != null) conn2.close();
            if(conn3 != null) conn3.close();
        }
    }
}
