package com.kkk.data.migration.job;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public class DataMigrationResult {
    /** 待迁移量*/
    private long migrationSize;
    /** 迁出表迁移前数据量*/
    private long oldOriginalCount;
    /** 迁出表迁移后数据量*/
    private long newOriginalCount;
    /** 迁入表迁移前数量*/
    private long oldHistoryCount;
    /** 迁入表迁移后数量*/
    private long newHistoryCount;

    public DataMigrationResult() {
    }

    public long getMigrationSize() {
        return this.migrationSize;
    }

    public void setMigrationSize(long migrationSize) {
        this.migrationSize = migrationSize;
    }

    public long getOldOriginalCount() {
        return this.oldOriginalCount;
    }

    public void setOldOriginalCount(long oldOriginalCount) {
        this.oldOriginalCount = oldOriginalCount;
    }

    public long getNewOriginalCount() {
        return this.newOriginalCount;
    }

    public void setNewOriginalCount(long newOriginalCount) {
        this.newOriginalCount = newOriginalCount;
    }

    public long getOldHistoryCount() {
        return this.oldHistoryCount;
    }

    public void setOldHistoryCount(long oldHistoryCount) {
        this.oldHistoryCount = oldHistoryCount;
    }

    public long getNewHistoryCount() {
        return this.newHistoryCount;
    }

    public void setNewHistoryCount(long newHistoryCount) {
        this.newHistoryCount = newHistoryCount;
    }
}
