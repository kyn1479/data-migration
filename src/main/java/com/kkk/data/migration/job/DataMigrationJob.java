package com.kkk.data.migration.job;

import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.enums.TimeSliceIntervalUnit;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Kkk
 * @Description: 数据迁移Job
 * @date 2021/11/27
 */
public class DataMigrationJob {
    /** 任务编号*/
    private String jobTaskNo;
    /** 源表名*/
    private String tableName;
    /** 目的表名*/
    private String historyTableName;
    /** 表对应实体类*/
    private Class<?> modleClazz;
    /** 分片策略-见枚举*/
    private SliceStrategyType sliceStrategyType;
    /** 时间分片字段 */
    private String timeSliceColumn;
    /** 时间间隔*/
    private int interval;
    /** 时间间隔单位-见枚举*/
    private TimeSliceIntervalUnit intervalUnit;
    /** 查询条件*/
    private String whereClause;
    /** id分片预分片每片数量*/
    private int idSliceSize;
    /** 实际迁移每片数量*/
    private long migrationSize;
    /** 开始时间*/
    private String beginTime;
    /** 结束时间*/
    private String endTime;

    /** 线程池*/
    private ThreadPoolExecutor threadPoolExecutor;

    public DataMigrationJob() {
    }

    public String getJobTaskNo() {
        return this.jobTaskNo;
    }

    public void setJobTaskNo(String jobTaskNo) {
        this.jobTaskNo = jobTaskNo;
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return this.threadPoolExecutor;
    }

    public void setThreadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public String getTableName() {
        return this.tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getHistoryTableName() {
        return this.historyTableName;
    }

    public void setHistoryTableName(String historyTableName) {
        this.historyTableName = historyTableName;
    }

    public Class<?> getModleClazz() {
        return this.modleClazz;
    }

    public void setModleClazz(Class<?> modleClazz) {
        this.modleClazz = modleClazz;
    }

    public String getWhereClause() {
        return this.whereClause;
    }

    public void setWhereClause(String whereClause) {
        this.whereClause = whereClause;
    }

    public int getIdSliceSize() {
        return idSliceSize;
    }

    public void setIdSliceSize(int idSliceSize) {
        this.idSliceSize = idSliceSize;
    }

    public long getMigrationSize() {
        return this.migrationSize;
    }

    public void setMigrationSize(long migrationSize) {
        this.migrationSize = migrationSize;
    }

    public void setIntervalUnit(TimeSliceIntervalUnit intervalUnit) {
        this.intervalUnit = intervalUnit;
    }

    public SliceStrategyType getSliceStrategyType() {
        return sliceStrategyType;
    }

    public void setSliceStrategyType(SliceStrategyType sliceStrategyType) {
        this.sliceStrategyType = sliceStrategyType;
    }

    public String getTimeSliceColumn() {
        return timeSliceColumn;
    }

    public void setTimeSliceColumn(String timeSliceColumn) {
        this.timeSliceColumn = timeSliceColumn;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public TimeSliceIntervalUnit getIntervalUnit() {
        return intervalUnit;
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
}

