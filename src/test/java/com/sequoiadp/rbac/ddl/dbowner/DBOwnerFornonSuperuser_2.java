
package com.sequoiadp.rbac.ddl.dbowner;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.testng.annotations.Test;

import com.sequoiadp.testcommon.HiveConnection;
import com.sequoiadp.testcommon.SDPTestBase;

/*
 * @Description   : Database Ownership Verification for non super user
 * @Author        : Lena
 */
public class DBOwnerFornonSuperuser_2 extends SDPTestBase {
    public static final String DBNAME1 = "newdbname_a";
    public static final String TABLENAME = "newtablename_a";
    public static final String VIEWNAME = "newviewname";
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

            String grantsql = HiveConnection.getInstance().grantSql("create","database",DBNAME1,"user",getConfig("testUser"));
            st1.executeQuery(grantsql);
                        
            //测试用户test来验证管理员的语句
            conn2 = HiveConnection.getInstance().getTestConnect();
            st2 = conn2.createStatement();
            
            // non super user can create db
            String createdbsql = "create database " + DBNAME1;
            st2.executeQuery(createdbsql);
            
            //non super user as owner can use db 
            String usagesql2 = "use " + DBNAME1;
			st2.executeQuery(usagesql2);
            
            String s3 = "s3a://sdbbucket2/" + TABLENAME;
            //non super user as owner 建表
            String createtablesql = "create table " + DBNAME1 + "." + TABLENAME + " (id int)using delta location \"" + s3 + "\" " + ";" ;
            st2.executeQuery(createtablesql);
            
            //non super user as owner create view
            String viewsql = "create view " + VIEWNAME + " as select * from " + TABLENAME;
			st2.executeQuery(viewsql);
            
			 //non super user as owner insert 
	        String insertsql = "insert into " + TABLENAME + " values(1001);";
	        st2.executeQuery(insertsql);
	        
            //non super user as owner select table
	        String selectsql = HiveConnection.getInstance().selectTv(DBNAME1,TABLENAME);
            st2.executeQuery(selectsql);
	        
            //non super user as owner select view
	        String selectsql2 = HiveConnection.getInstance().selectTv(DBNAME1,VIEWNAME);
            st2.executeQuery(selectsql2);                   
            
            //non super user as owner update
	        String updatesql = "update " + TABLENAME + " set  id = 1002 where id = 1001;";
	        st2.executeQuery(updatesql);
	        
	        //non super user as owner can grant
            String grantsql2 = HiveConnection.getInstance().grantSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st2.executeQuery(grantsql2);	        
	        
	        //non super user as owner can revoke
            String revokesql = HiveConnection.getInstance().revokeSql("select","table",TABLENAME,"user",getConfig("nonowner"));
            st2.executeQuery(revokesql);	
	        
            //non super user as owner can transfer table
            String transferowner = "ALTER table " + TABLENAME + " OWNER TO user " + getConfig("nonowner");
            st2.executeQuery(transferowner);
            
            //non super user as owner can transfer view
            String transferowner2 = "ALTER VIEW " + VIEWNAME + " OWNER TO user " + getConfig("nonowner");
            st2.executeQuery(transferowner2);  
            

	        //non super user as owner delete
	        String delsql = "delete from " + DBNAME1 + "." + TABLENAME + " where id = 1001;";
	        st2.executeQuery(delsql);          
    
            // owner of the table drop table
            String droptablesql = HiveConnection.getInstance().dropSql("table", DBNAME1 + "." + TABLENAME );
            st2.executeQuery(droptablesql);
            
            //owner of the db drop view
        	String dropsql = HiveConnection.getInstance().dropSql("view", DBNAME1 + "." + VIEWNAME);
			st2.executeQuery(dropsql);
            
            //owner of the db drop db
            String dropdbsql = HiveConnection.getInstance().dropSql("database",DBNAME1 );
            st2.executeQuery(dropdbsql);
            
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
