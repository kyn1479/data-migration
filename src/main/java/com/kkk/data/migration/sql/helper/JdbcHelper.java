package com.kkk.data.migration.sql.helper;

import java.util.List;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public interface JdbcHelper {
    <T> List<T> selectList(Class c, String tableName, String whereClause) throws Exception;

    long selectTotalCount(String tableName, String whereCxcuse);

    long selectTotalCount(String tableName);

    long selectBeginId(String tableName, String whereCxcuse, long startId, int limitSize);

    long selectEndId(String tableName, String whereCxcuse, long startId, int limitSize);
}
