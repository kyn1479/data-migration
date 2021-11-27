package com.kkk.data.migration.job;

import java.util.List;

/**
 * @author Kkk
 * @Description: Task执行器接口
 * @date 2021/11/27
 */
public interface DataTaskExecutor {
    void execute(List<DataTask> dataTasks, DataMigrationJob dataMigrationJob);
}
