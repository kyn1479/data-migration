package com.kkk.data.migration.job;

/**
 * @author Kkk
 * @Description:
 * @date 2021/11/27
 */
public class DataMigrationTaskResult {
    private long successSize;
    private long failSize;

    public long getFailSize() {
        return this.failSize;
    }

    public void setFailSize(long failSize) {
        this.failSize = failSize;
    }

    public long getSuccessSize() {
        return this.successSize;
    }

    public void setSuccessSize(long successSize) {
        this.successSize = successSize;
    }
}
