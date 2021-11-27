package com.kkk.data.migration.job.builder;

import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.enums.TimeSliceIntervalUnit;
import com.kkk.data.migration.job.DataMigrationJob;
import org.junit.Assert;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public class DataMigrationJobBuilder {
    /** 任务编号*/
    private String jobTaskNo;
    /** 迁出表名*/
    private String tableName;
    /** 迁入表名*/
    private String historyTableName;
    /** 表对应实体类*/
    private Class<?> modleClazz;
    /** where条件*/
    private String whereClause;
    /** Id分片时每片数量*/
    private int sliceSize;
    /** 线程池*/
    private ThreadPoolExecutor threadPoolExecutor;
    /** 分片策略*/
    private SliceStrategyType sliceStrategyType;
    /** 时间分片时，分片字段*/
    private String timeSliceColumn;
    /** 时间分片时，时间间隔*/
    private int interval;
    /** 时间分片时，时间间隔单位*/
    private TimeSliceIntervalUnit intervalUnit;
    /** 开始时间*/
    private String beginTime;
    /** 结束时间*/
    private String endTime;


    public DataMigrationJobBuilder jobTaskNo(String jobTaskNo) {
        this.jobTaskNo = jobTaskNo;
        return this;
    }

    public DataMigrationJobBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public DataMigrationJobBuilder modleClazz(Class<?> modleClazz) {
        this.modleClazz = modleClazz;
        return this;
    }

    public DataMigrationJobBuilder historyTableName(String historyTableName) {
        this.historyTableName = historyTableName;
        return this;
    }

    public DataMigrationJobBuilder whereClause(String whereClause) {
        this.whereClause = whereClause;
        return this;
    }

    public DataMigrationJobBuilder sliceSize(int sliceSize) {
        this.sliceSize = sliceSize;
        return this;
    }

    public DataMigrationJobBuilder threadPoolExecutor(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
        return this;
    }
    public DataMigrationJobBuilder sliceStrategyType(SliceStrategyType sliceStrategyType) {
        this.sliceStrategyType = sliceStrategyType;
        return this;
    }
    public DataMigrationJobBuilder timeSliceColumn(String timeSliceColumn) {
        this.timeSliceColumn = timeSliceColumn;
        return this;
    }
    public DataMigrationJobBuilder intervalUnit(TimeSliceIntervalUnit intervalUnit) {
        this.intervalUnit = intervalUnit;
        return this;
    }
    public DataMigrationJobBuilder interval(int interval) {
        this.interval = interval;
        return this;
    }
    public DataMigrationJobBuilder beginTime(String beginTime) {
        this.beginTime = beginTime;
        return this;
    }
    public DataMigrationJobBuilder endTime(String endTime) {
        this.endTime = endTime;
        return this;
    }

    public DataMigrationJob build() {
        DataMigrationJob dataMigrationJob = new DataMigrationJob();
        Assert.assertNotNull("jobTaskNo is null", this.jobTaskNo);
        dataMigrationJob.setJobTaskNo(this.jobTaskNo);
        Assert.assertNotNull("historyTableName is null", this.historyTableName);
        dataMigrationJob.setHistoryTableName(this.historyTableName);
        Assert.assertNotNull("modleClazz is null", this.modleClazz);
        dataMigrationJob.setModleClazz(this.modleClazz);
        Assert.assertNotNull("whereClause is null", this.whereClause);
        dataMigrationJob.setWhereClause(this.whereClause);
        Assert.assertNotNull("tableName is null", this.tableName);
        dataMigrationJob.setTableName(this.tableName);
        dataMigrationJob.setThreadPoolExecutor(this.threadPoolExecutor);
        dataMigrationJob.setSliceStrategyType(this.sliceStrategyType);
        Assert.assertNotNull("sliceStrategyType is null", this.sliceStrategyType);
        if(SliceStrategyType.DEFAULT_ID.equals(this.sliceStrategyType)){
            Assert.assertNotNull("sliceSize is null", this.sliceSize);
            dataMigrationJob.setIdSliceSize(this.sliceSize);
        }else if (SliceStrategyType.TIME_SLICE.equals(this.sliceStrategyType)){
            Assert.assertNotNull("beginTime is null", this.beginTime);
            dataMigrationJob.setBeginTime(this.beginTime);
            Assert.assertNotNull("endTime is null", this.endTime);
            dataMigrationJob.setEndTime(this.endTime);
            Assert.assertNotNull("timeSliceColumn is null", this.timeSliceColumn);
            dataMigrationJob.setTimeSliceColumn(this.timeSliceColumn);
            Assert.assertNotNull("interval is null", this.interval);
            dataMigrationJob.setInterval(this.interval);
            Assert.assertNotNull("intervalUnit is null", this.intervalUnit);
            dataMigrationJob.setIntervalUnit(this.intervalUnit);
        }else {
            throw new AssertionError("sliceStrategyType is illegal");
        }
        return dataMigrationJob;
    }

}
