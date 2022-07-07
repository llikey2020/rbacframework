package com.sequoiadp.testcommon;

import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class GeneralComparison {
    /**
     * @param set
     * @param index start from 1
     * @param keyWord to find
     * @return
     * @throws SQLException
     */
    public static boolean findInResult(ResultSet set,int[] index,String[] keyWord) throws SQLException {
        if(index.length!= keyWord.length)
            throw new SQLException("index.length should equal with keyWord.length");

        while(set.next()){
            for (int i = 0; i < index.length; i++) {
                if(set.getString(index[i]).contains(keyWord[i]))
                    return true;
            }
        }
        return false;
    }
}
