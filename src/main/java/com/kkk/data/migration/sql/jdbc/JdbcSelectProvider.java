package com.kkk.data.migration.sql.jdbc;

import com.kkk.data.migration.utils.FieldMappingUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.persistence.Column;
import javax.persistence.Id;
import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
@Component
public class JdbcSelectProvider {
    private static final Logger logger = LoggerFactory.getLogger(JdbcSelectProvider.class);
    @Resource
    private DataSource dataSource;

    public JdbcSelectProvider() {
    }

    @Transactional(rollbackFor = {Throwable.class})
    public <T> List<T> select(Class<T> c, String sql, Object... params) throws SQLException, IllegalAccessException, InstantiationException, InvocationTargetException, NoSuchMethodException {
        List<T> resultList = new ArrayList();
        ResultSet res = this.selectExecute(sql, params);
        Map<Field, String> fieldMap = this.getTableFields(c);

        T modle;
        label34:
        for(HashMap enumMethodMap = new HashMap(); res.next(); resultList.add(modle)) {
            modle = c.newInstance();
            Set<Map.Entry<Field, String>> entrySet = fieldMap.entrySet();
            Iterator var10 = entrySet.iterator();

            while(true) {
                while(true) {
                    if (!var10.hasNext()) {
                        continue label34;
                    }

                    Map.Entry<Field, String> entry = (Map.Entry)var10.next();
                    Field field = (Field)entry.getKey();
                    field.setAccessible(true);
                    Class enumClazz = field.getType();
                    if (enumClazz.isEnum() && res.getObject((String)entry.getValue()) != null) {
                        String methodKey = enumClazz.getName();
                        Method method;
                        if (enumMethodMap.containsKey(methodKey)) {
                            method = (Method)enumMethodMap.get(methodKey);
                        } else {
                            method = enumClazz.getMethod("getByCode", String.class);
                            enumMethodMap.put(methodKey, method);
                        }

                        field.set(modle, method.invoke((Object)null, res.getString((String)entry.getValue())));
                    } else {
                        field.set(modle, res.getObject((String)entry.getValue()));
                    }
                }
            }
        }

        return resultList;
    }

    @Transactional
    public ResultSet select(String sql) {
        return this.selectExecute(sql);
    }

    private ResultSet selectExecute(String sql, Object... params) {
        Connection connection = null;
        PreparedStatement pst = null;
        ResultSet res = null;

        try {
            connection = DataSourceUtils.getConnection(this.dataSource);
            pst = connection.prepareStatement(sql);

            for(int i = 0; i < params.length; ++i) {
                pst.setObject(i + 1, params[i]);
            }

            res = pst.executeQuery();
        } catch (SQLException var7) {
            var7.printStackTrace();
        }

        return res;
    }

    private Map<Field, String> getTableFields(Class<?> c) {
        Map<Field, String> map = new HashMap();
        List<Field> fields = FieldMappingUtil.getFields(c);

        for(int i = 0; i < fields.size(); ++i) {
            Field field = (Field)fields.get(i);
            field.setAccessible(true);
            Id id = (Id)FieldMappingUtil.getAnnotation(Id.class, field, c);
            Column column;
            String name;
            if (null != id) {
                column = (Column)field.getAnnotation(Column.class);
                if (null != column) {
                    name = column.name();
                    if (StringUtils.isNotBlank(name)) {
                        map.put(field, name);
                    } else {
                        map.put(field, field.getName());
                    }
                } else {
                    map.put(field, field.getName());
                }
            } else {
                column = (Column)FieldMappingUtil.getAnnotation(Column.class, field, c);
                if (null != column) {
                    name = column.name();
                    if (StringUtils.isNotBlank(name)) {
                        map.put(field, name);
                    }
                }
            }
        }

        return map;
    }
}

