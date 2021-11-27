package com.kkk.data.migration.job;

import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.sql.helper.JdbcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * @author Kkk
 * @Description: Task任务
 * @date 2021/11/27
 */
public class DataTask implements Callable<DataMigrationTaskResult> {
    private static Logger logger = LoggerFactory.getLogger(DataTask.class);
    private String whereClause;
    private Class<?> modelClass;
    private JdbcHelper jdbcHelperImpl;
    private DataMigrationHandler dataMigrationHandler;
    private String historyTableName;
    private String tableName;
    private SliceStrategyType sliceStrategyType;
    private long beginId;
    private long endId;
    private String beginTime;
    private String endTime;
    private long sliceCount;


    @Override
    public DataMigrationTaskResult call() throws Exception {
        DataMigrationTaskResult dataTaskResult = new DataMigrationTaskResult();
        List<?> dataList =new ArrayList<>();
        try {
            dataList = this.jdbcHelperImpl.selectList(this.modelClass, this.tableName, this.whereClause);
            if(SliceStrategyType.DEFAULT_ID.equals(this.sliceStrategyType)){
                logger.info("开始分片迁移,分片策略({}),Id({}-->{}),预读数据:({})条，实际读({})条",this.sliceStrategyType,this.beginId,this.endId,this.sliceCount,dataList.size());
            }else if(SliceStrategyType.TIME_SLICE.equals(this.sliceStrategyType)){
                logger.info("开始分片迁移,分片策略({}),Time({}-->{}),实际读({})条",this.sliceStrategyType,this.beginTime,this.endTime,dataList.size());
            }
            this.dataMigrationHandler.migration(dataList, this.historyTableName);
            dataTaskResult.setFailSize(0);
            dataTaskResult.setSuccessSize(dataList.size());

        } catch (Throwable e) {
            logger.error("数据表({})迁移异常", this.tableName, e);
            dataTaskResult.setFailSize(dataList.size());
            dataTaskResult.setSuccessSize(0);
        }
        return dataTaskResult;
    }

    public String getWhereClause() {
        return whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public Class<?> getModelClass() {
        return modelClass;
    }

    public void setModelClass(Class<?> modelClass) {
        this.modelClass = modelClass;
    }

    public JdbcHelper getJdbcHelperImpl() {
        return jdbcHelperImpl;
    }

    public void setJdbcHelperImpl(JdbcHelper jdbcHelperImpl) {
        this.jdbcHelperImpl = jdbcHelperImpl;
    }

    public DataMigrationHandler getDataMigrationHandler() {
        return dataMigrationHandler;
    }

    public void setDataMigrationHandler(DataMigrationHandler dataMigrationHandler) {
        this.dataMigrationHandler = dataMigrationHandler;
    }

    public String getHistoryTableName() {
        return historyTableName;
    }

    public void setHistoryTableName(String historyTableName) {
        this.historyTableName = historyTableName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getBeginId() {
        return beginId;
    }

    public void setBeginId(long beginId) {
        this.beginId = beginId;
    }

    public long getEndId() {
        return endId;
    }

    public void setEndId(long endId) {
        this.endId = endId;
    }

    public String getBeginTime() {
        return beginTime;
    }

    public void setBeginTime(String beginTime) {
        this.beginTime = beginTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public SliceStrategyType getSliceStrategyType() {
        return sliceStrategyType;
    }

    public void setSliceStrategyType(SliceStrategyType sliceStrategyType) {
        this.sliceStrategyType = sliceStrategyType;
    }

    public long getSliceCount() {
        return sliceCount;
    }

    public void setSliceCount(long sliceCount) {
        this.sliceCount = sliceCount;
    }
}

