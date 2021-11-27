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
import javax.persistence.Table;
import javax.sql.DataSource;
import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
@Component
public class JdbcInsertProvider {
    private static final Logger logger = LoggerFactory.getLogger(JdbcInsertProvider.class);
    @Resource
    private DataSource dataSource;
    private final static int SLICE_SIZE = 1000;

    public JdbcInsertProvider() {
    }

    @Transactional
    public void bacthInsertSlice(List<?> dataList, boolean isIgnore, String tableName) throws Exception {
        int size = dataList.size();
        if (size < 1) {
            return;
        } else {
            //继续分片....
            if(size>1000){
                int currentIndex = 0;
                int totalSize = dataList.size();
                int presentSize = dataList.size() >= SLICE_SIZE ? SLICE_SIZE : dataList.size();//1000
                while (presentSize <= totalSize) {
                    List<?> sliceList = dataList.subList(currentIndex, presentSize);
                    this.bacthInsert(sliceList, isIgnore, tableName);
                    if (presentSize == totalSize) {
                        break;
                    }
                    currentIndex = presentSize;
                    presentSize=presentSize+SLICE_SIZE;
                    if (presentSize > totalSize) {
                        presentSize = totalSize;
                    }
                }
            }else {
                this.bacthInsert(dataList, isIgnore, tableName);
            }
        }
        return;
    }

    @Transactional(rollbackFor = {Throwable.class})
    public int[] bacthInsert(List<?> list, boolean isIgnore, String tableName) throws SQLException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        int size = list.size();
        if (size < 1) {
            return null;
        } else {
            int[] result = new int[size];
            Object t = list.get(0);
            Connection connection = null;
            PreparedStatement pst = null;
            HashMap enumMethodMap = new HashMap();

            try {
                connection = DataSourceUtils.getConnection(this.dataSource);
                String sql = this.getInsertSql(t, isIgnore, tableName);
                pst = connection.prepareStatement(sql);
                List<String> list1 = this.getFieds(t.getClass());

                for(int i = 0; i < list.size(); ++i) {
                    Object obj = list.get(i);
                    Class c = obj.getClass();

                    for(int j = 0; j < list1.size(); ++j) {
                        Field field = this.getDeclaredField(c, (String)list1.get(j));
                        field.setAccessible(true);
                        Class enumClazz = field.getType();
                        if (enumClazz.isEnum() && field.get(obj) != null) {
                            String methodKey = enumClazz.getName();
                            Method method;
                            if (enumMethodMap.containsKey(methodKey)) {
                                method = (Method)enumMethodMap.get(methodKey);
                            } else {
                                method = enumClazz.getMethod("getCode");
                                enumMethodMap.put(methodKey, method);
                            }

                            pst.setObject(j + 1, method.invoke(field.get(obj)));
                        } else {
                            pst.setObject(j + 1, field.get(obj));
                        }
                    }

                    pst.addBatch();
                }

                result = pst.executeBatch();
                return result;
            } catch (SQLException e) {
                throw new SQLException(e);
            }
        }
    }

    @Transactional
    public int[] bacthInsert(List<?> list, boolean isIgnore) throws Exception {
        return this.bacthInsert(list, isIgnore, "");
    }

    @Transactional
    public void bacthInsertSpecifiedTableName(List<?> list, String tableName) throws Exception {
        this.bacthInsertSlice(list, false, tableName);
    }

    private Field getDeclaredField(Class c, String fieldName) throws NoSuchFieldException {
        if (c == Object.class) {
            throw new NoSuchFieldException(fieldName);
        } else {
            Field field = null;

            try {
                field = c.getDeclaredField(fieldName);
                return field;
            } catch (NoSuchFieldException var5) {
                return this.getDeclaredField(c.getSuperclass(), fieldName);
            }
        }
    }

    private <T> String getInsertSql(T t, Boolean isIgnore, String tableName) {
        String handle = "INSERT INTO";
        if (isIgnore) {
            handle = "INSERT IGNORE INTO";
        }

        StringBuilder insertSql = new StringBuilder(handle);
        Class<?> aClass = t.getClass();
        List<String> fiedsList = this.getTableFieds(aClass);
        Table table = (Table)aClass.getAnnotation(Table.class);
        if (null == table) {
            return insertSql.toString();
        } else {
            if (StringUtils.isBlank(tableName)) {
                tableName = table.name();
            }

            insertSql.append(" `").append(tableName).append("` ").append("(");

            int i;
            for(i = 0; i < fiedsList.size(); ++i) {
                insertSql.append("`").append((String)fiedsList.get(i)).append("`").append(",");
            }

            if (insertSql.lastIndexOf(",") == insertSql.length() - 1) {
                insertSql.deleteCharAt(insertSql.length() - 1);
            }

            insertSql.append(")").append(" VALUE ").append("(");

            for(i = 0; i < fiedsList.size(); ++i) {
                insertSql.append("?").append(",");
            }

            if (insertSql.lastIndexOf(",") == insertSql.length() - 1) {
                insertSql.deleteCharAt(insertSql.length() - 1);
            }

            insertSql.append(");");
            return insertSql.toString();
        }
    }

    private Type[] getGenericType(Class<?> c) {
        Type genType = c.getGenericSuperclass();
        Type[] types = null;
        if (genType instanceof ParameterizedType) {
            types = ((ParameterizedType)genType).getActualTypeArguments();
        }

        return types;
    }

    private List<String> getTableFieds(Class<?> c) {
        List<String> list = new ArrayList();
        List<Field> fields = FieldMappingUtil.getFields(c);

        for(int i = 0; i < fields.size(); ++i) {
            Field field = (Field)fields.get(i);
            field.setAccessible(true);
            Id id = (Id) FieldMappingUtil.getAnnotation(Id.class, field, c);
            Column column;
            String name;
            if (null != id) {
                column = (Column)field.getAnnotation(Column.class);
                if (null != column) {
                    name = column.name();
                    list.add(name);
                } else {
                    list.add(field.getName());
                }
            } else {
                column = (Column)FieldMappingUtil.getAnnotation(Column.class, field, c);
                if (null != column) {
                    name = column.name();
                    if (StringUtils.isNotBlank(name)) {
                        list.add(name);
                    }
                }
            }
        }

        return list;
    }

    private List<String> getFieds(Class<?> c) {
        List<String> list = new ArrayList();
        List<Field> fields = FieldMappingUtil.getFields(c);

        for(int i = 0; i < fields.size(); ++i) {
            Field field = (Field)fields.get(i);
            field.setAccessible(true);
            Id id = (Id)FieldMappingUtil.getAnnotation(Id.class, field, c);
            if (null != id) {
                list.add(field.getName());
            } else {
                Column column = (Column)FieldMappingUtil.getAnnotation(Column.class, field, c);
                if (null != column) {
                    list.add(field.getName());
                }
            }
        }

        return list;
    }
}

