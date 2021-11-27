package com.kkk.data.migration.sql.helper;

import com.kkk.data.migration.sql.jdbc.JdbcSelectProvider;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
@Service
public class JdbcHelperImpl implements JdbcHelper {
    @Resource
    private JdbcSelectProvider jdbcSelectProvider;

    public <T> List<T> selectList(Class c, String tableName, String whereClause) throws Exception {
        StringBuilder stringBuilder = new StringBuilder("SELECT * FROM ");
        stringBuilder.append(tableName).append(" where ").append(whereClause);
        String sql = stringBuilder.toString();
        return this.jdbcSelectProvider.select(c, sql, new Object[0]);
    }

    @Transactional
    public long selectTotalCount(String tableName, String whereCxcuse) {
        StringBuilder stringBuilder = new StringBuilder("SELECT COUNT(1) FROM ");
        stringBuilder.append(tableName).append(" WHERE ").append(whereCxcuse);
        String sql = stringBuilder.toString();
        ResultSet resultSet = this.jdbcSelectProvider.select(sql);
        long totalCount = 0L;

        try {
            if (resultSet.next()) {
                totalCount = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return totalCount;
    }

    @Transactional
    public long selectTotalCount(String tableName) {
        StringBuilder stringBuilder = new StringBuilder("SELECT COUNT(1) FROM ");
        stringBuilder.append(tableName);
        String sql = stringBuilder.toString();
        ResultSet resultSet = this.jdbcSelectProvider.select(sql);
        long totalCount = 0L;

        try {
            if (resultSet.next()) {
                totalCount = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return totalCount;
    }

    @Transactional
    public long selectBeginId(String tableName, String whereCxcuse, long startId, int limitSize) {
        StringBuilder stringBuilder = new StringBuilder("select MIN(t.id) from ( select id from ");
        stringBuilder.append(tableName).append(" where ").append(" id > ").append(startId).append(" and ").append(whereCxcuse).append(" order by id asc limit 0, ").append(limitSize).append(" )t");
        String sql = stringBuilder.toString();
        ResultSet resultSet = this.jdbcSelectProvider.select(sql);
        long totalCount = 0L;

        try {
            if (resultSet.next()) {
                totalCount = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return totalCount;
    }

    @Transactional
    public long selectEndId(String tableName, String whereCxcuse, long startId, int limitSize) {
        StringBuilder stringBuilder = new StringBuilder("select MAX(t.id) from ( select id from ");
        stringBuilder.append(tableName).append(" where ").append(" id > ").append(startId).append(" and ").append(whereCxcuse).append(" order by id asc limit 0, ").append(limitSize).append(" )t");
        String sql = stringBuilder.toString();
        this.jdbcSelectProvider.select(sql);
        ResultSet resultSet = this.jdbcSelectProvider.select(sql);
        long totalCount = 0L;

        try {
            if (resultSet.next()) {
                totalCount = resultSet.getLong(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return totalCount;
    }
}
