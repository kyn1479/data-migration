package com.kkk.data.migration.job;

import com.kkk.data.migration.exception.DataMigrationException;
import com.kkk.data.migration.sql.jdbc.JdbcDeleteProvider;
import com.kkk.data.migration.sql.jdbc.JdbcInsertProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author Kkk
 * @Description: 数据迁移处理器
 * @date 2021/11/27
 */
@Component
public class DataMigrationHandler {
    private Logger logger = LoggerFactory.getLogger(DataMigrationHandler.class);
    @Resource
    private JdbcInsertProvider jdbcInsertProvider;
    @Resource
    private JdbcDeleteProvider jdbcDeleteProvider;

    public DataMigrationHandler() {
    }

    @Transactional(rollbackFor = {Throwable.class})
    public void migration(List<?> dataList, String historyTableName) {
        try {
            this.jdbcInsertProvider.bacthInsertSpecifiedTableName(dataList, historyTableName);
        } catch (Exception e) {
            this.logger.error("表({})插入数据异常",historyTableName,e);
            throw new DataMigrationException( e);
        }

        try {
            this.jdbcDeleteProvider.delete(dataList);
        } catch (Exception  e) {
            this.logger.error("表({})删除数据异常",historyTableName,e);
            throw new DataMigrationException( e);
        }
    }
}

