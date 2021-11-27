package com.kkk.data.migration.job;

/**
 * @author Kkk
 * @Description: Job执行器
 * @date 2021/11/27
 */
public interface DataMigrationJobLauncher {
    DataMigrationResult run(DataMigrationJob dataMigrationJob);
}