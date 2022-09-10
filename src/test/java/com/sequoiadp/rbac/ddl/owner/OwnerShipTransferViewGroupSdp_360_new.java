package com.sequoiadp.rbac.ddl.owner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPViewTestBase;

public class OwnerShipTransferViewGroupSdp_360_new extends SDPViewTestBase {
    public OwnerShipTransferViewGroupSdp_360_new() {
        super.setTableName("fasghghjj");
        super.hasGroup();
    }
    @Test (expectedExceptions =  { java.sql.SQLException.class },expectedExceptionsMessageRegExp = ".*does not have grant privilege on.*")
    public void test() throws SQLException, InterruptedException {
        Connection conn1 = null,conn2 = null,conn3 =  null;
        Statement st1 = null,st2 = null, st3 = null;
        try {
        	//管理员sequoiadb连接到thriftserver
        	viewName = this.getTableName() + "_view";
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql);           
            String grantsql = HiveConnection.getInstance().grantSql("create","view",viewName,"user",getConfig("nonowner"));
            st1.executeQuery(grantsql);
            grantsql = HiveConnection.getInstance().grantSql("usage","database",getConfig("dbName"),"user",getConfig("nonowner"));
            st1.executeQuery(grantsql);
            String grantsql2 = HiveConnection.getInstance().grantSql("select","table",tableName,"user",getConfig("nonowner"));
            st1.executeQuery(grantsql2);
            String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
            st1.executeQuery(addgpusersql);
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3 = conn3.createStatement();
            
            st3.executeQuery(usagesql);   
            String viewsql = "create view " + viewName + " as select * from " + tableName ;
            st3.executeQuery(viewsql);
            
            String transferowner = "ALTER VIEW " + viewName + " OWNER TO group " +  getConfig("testGroup");
            st3.executeQuery(transferowner);  
            
            st3.close();
            conn3.close();
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            st2.executeQuery(usagesql);    
            String transfergrantsql = HiveConnection.getInstance().grantSql("select","view",viewName,"user",getConfig("nonowner"));
            st2.executeQuery(transfergrantsql);  
            
            conn3 = HiveConnection.getInstance().getNonownerConnect();
            st3 = conn3.createStatement();
            st3.executeQuery(usagesql);  
            String transfergrantsql2 = HiveConnection.getInstance().grantSql("select","view",viewName,"user",getConfig("testUser"));
            Thread.sleep(10000); 
            st3.executeQuery(transfergrantsql2);
            
			//String dropsql = HiveConnection.getInstance().dropSql("view", getConfig("dbName") + "." + viewName);
			//st1.executeQuery(dropsql);

        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
			String dropsql = HiveConnection.getInstance().dropSql("view", getConfig("dbName") + "." + viewName);
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
