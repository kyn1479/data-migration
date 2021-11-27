package com.kkk.data.migration.sql.jdbc;

import com.kkk.data.migration.utils.FieldMappingUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
@Component
public class JdbcDeleteProvider {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDeleteProvider.class);
    @Resource
    private DataSource dataSource;

    public JdbcDeleteProvider() {
    }

    @Transactional
    public int[] delete(List<?> list) throws SQLException, IllegalAccessException {
        int size = list.size();
        if (size < 1) {
            return null;
        } else {
            Object obj = list.get(0);
            int[] result = new int[size];
            Connection connection = null;
            PreparedStatement pst = null;
            Object var7 = null;

            try {
                connection = DataSourceUtils.getConnection(this.dataSource);
                List<Field> idFieldList = this.getIdFields(obj.getClass());
                String sql = this.getDeleteSqlById(obj.getClass(), idFieldList);
                pst = connection.prepareStatement(sql);

                for(int i = 0; i < list.size(); ++i) {
                    for(int j = 0; j < idFieldList.size(); ++j) {
                        Object value = ((Field)idFieldList.get(j)).get(list.get(i));
                        pst.setObject(j + 1, value);
                    }

                    pst.addBatch();
                }

                result = pst.executeBatch();
                return result;
            } catch (SQLException var13) {
                throw new SQLException(var13);
            }
        }
    }

    @Transactional
    public void delete(String sql, Object... param) throws SQLException {
        Connection connection = null;
        PreparedStatement pst = null;
        Object var5 = null;

        try {
            connection = DataSourceUtils.getConnection(this.dataSource);
            pst = connection.prepareStatement(sql);

            for(int i = 0; i < param.length; ++i) {
                pst.setObject(i + 1, param[i]);
            }

            pst.execute();
        } catch (SQLException var7) {
            throw new SQLException(var7);
        }
    }

    private List<Field> getIdFields(Class<?> c) throws SQLException {
        List<Field> idFieldList = new ArrayList();
        List<Field> fields = FieldMappingUtil.getFields(c);

        for(int i = 0; i < fields.size(); ++i) {
            Field field = (Field)fields.get(i);
            field.setAccessible(true);
            Id id = (Id)FieldMappingUtil.getAnnotation(Id.class, field, c);
            if (null == id) {
                id = (Id)field.getAnnotation(Id.class);
            }

            if (null != id) {
                idFieldList.add(field);
            }
        }

        if (idFieldList.size() == 0) {
            throw new SQLException("找不到主键id");
        } else {
            return idFieldList;
        }
    }

    private String getDeleteSqlById(Class<?> c, List<Field> idFieldList) {
        StringBuffer sb = new StringBuffer("DELETE FROM ");
        Table table = (Table)c.getAnnotation(Table.class);
        String tableName = table.name();
        sb.append(tableName).append(" WHERE ");

        for(int i = 0; i < idFieldList.size(); ++i) {
            Column column = (Column) FieldMappingUtil.getAnnotation(Column.class, (Field)idFieldList.get(i), c);
            String fieldName = "";
            if (null != column) {
                fieldName = column.name();
            } else {
                fieldName = ((Field)idFieldList.get(i)).getName();
            }

            sb.append(fieldName).append(" = ? ").append("AND ");
        }

        String deleteSql = sb.toString();
        deleteSql = deleteSql.substring(0, deleteSql.lastIndexOf("AND")).trim();
        return deleteSql;
    }
}

