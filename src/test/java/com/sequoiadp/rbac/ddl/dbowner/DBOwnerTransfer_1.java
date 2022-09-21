package com.sequoiadp.rbac.ddl.dbowner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.testng.annotations.Test;
import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : database ownership transfer to user/group
 * @Author        : Lena
 */
public class DBOwnerTransfer_1 extends SDPTestBase {
//    public static final String DBNAME1 = "newdbname_a";
    public static final String TABLENAME = "newtablename_a";
    public static final String VIEWNAME = "newviewname";
    public DBOwnerTransfer_1() {
        super.setTableName("fasghghjj");
//        super.hasGroup();
    }
    //测试点
    @Test
    public void test() throws SQLException {
        Connection conn1 = null,conn2 = null;//,conn3=null;
        Statement st1 = null,st2 = null;//,st3 = null;
        try {
        	//管理员sequoiadb连接到thriftserver
            conn1 = HiveConnection.getInstance().getAdminConnect();
            st1= conn1.createStatement();
            String usagesql = HiveConnection.getInstance().usageSql(getConfig("dbName"));
            st1.executeQuery(usagesql); 
            
	        //database ownership transfer to user ben
	        String transferowner = "ALTER database " + getConfig("dbName") + " OWNER TO user " +  getConfig("nonowner");
	        st1.executeQuery(transferowner);  
            
//	        //测试用户test来验证管理员的语句	        
//	        conn3 = HiveConnection.getInstance().getTestConnect();
//	        st3 = conn3.createStatement();   
	          
	
	        //database ownership transfer to group has user lena
	        String addgpusersql = HiveConnection.getInstance().alterUserSql(getConfig("testGroup"),"add", getConfig("testUser"));
	        st1.executeQuery(addgpusersql);      
	          
	        String transferowner2= "ALTER database " + getConfig("dbName") + " OWNER TO group " + getConfig("testGroup");
	        st1.executeQuery(transferowner2);  
	          
	        conn2 = HiveConnection.getInstance().getTestConnect();
	        st2 = conn2.createStatement();    
	
	        //group member has the db ownership can use db
	        String usagesql2= HiveConnection.getInstance().usageSql(getConfig("dbName"));
	        st2.executeQuery(usagesql2); 
     
            String s3 = "s3a://sdbbucket2/" + TABLENAME;
            //group member has the db ownership 建表
            String createtablesql = "create table " + getConfig("dbName") + "." + TABLENAME + " (id int)using delta location \"" + s3 + "\" " + ";" ;
            st2.executeQuery(createtablesql);
  
            //group member has the db ownership create view
            String viewsql = "create view " + VIEWNAME + " as select * from " + TABLENAME;
			st2.executeQuery(viewsql);
            
			//group member has the db ownership insert 
	        String insertsql = "insert into " + TABLENAME + " values(1001);";
	        st2.executeQuery(insertsql);
       
	        //group member has the db ownership select table
	        String selectsql = HiveConnection.getInstance().selectTv(getConfig("dbName"),TABLENAME);
            st2.executeQuery(selectsql);
            
            //group member has the db ownership select view
	        String selectsql2 = HiveConnection.getInstance().selectTv(getConfig("dbName"),VIEWNAME);
            st2.executeQuery(selectsql2);                   
            
            //group member has the db ownership update
	        String updatesql = "update " + TABLENAME + " set  id = 1002 where id = 1001;";
	        st2.executeQuery(updatesql);
	        
	        //group member has the db ownership can grant
            String grantsql2 = HiveConnection.getInstance().grantSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st2.executeQuery(grantsql2);	        
	        
            //group member has the db ownership can revoke
            String revokesql = HiveConnection.getInstance().revokeSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st2.executeQuery(revokesql);	
            
            //group member has the db ownership can transfer table to ben
            String transferownera = "ALTER table " + TABLENAME + " OWNER TO user " + getConfig("nonowner");
            st2.executeQuery(transferownera);
            
            //group member has the db ownership can transfer view to ben
            String transferownerb = "ALTER VIEW " + VIEWNAME + " OWNER TO user " + getConfig("nonowner");
            st2.executeQuery(transferownerb);  
            
            //group member has the db ownership can delete
	        String delsql = "delete from " + getConfig("dbName") + "." + TABLENAME + " where id = 1001;";
	        st2.executeQuery(delsql);          
    
            //member of the group drop view
        	String dropsql = HiveConnection.getInstance().dropSql("view", getConfig("dbName") + "." + VIEWNAME);
			st2.executeQuery(dropsql);
            
	        //member of the group drop table
            String droptablesql = HiveConnection.getInstance().dropSql("table", getConfig("dbName") + "." + TABLENAME );
            st2.executeQuery(droptablesql);
            
            
            String droptablesql2 = HiveConnection.getInstance().dropSql("table", getConfig("dbName") + "." + tableName );
            st1.executeQuery(droptablesql2);
            
//          String droptablesql2 = HiveConnection.getInstance().dropSql("table", getConfig("dbName") + "." + "null" );
//          st1.executeQuery(droptablesql2);
            
            //owner of the db drop db
            String dropdbsql2 = HiveConnection.getInstance().dropSql("database",getConfig("dbName") );
            st2.executeQuery(dropdbsql2);
            
        } catch ( SQLException e) {
            e.printStackTrace();
            throw e;
        }finally {
            st1.close();
            st2.close();
//            st3.close();
            conn1.close();
            conn2.close();
//            conn3.close();
        }
    }
}
