package com.kkk.data.migration.job.partitioner;

import com.kkk.data.migration.job.DataMigrationJob;
import com.kkk.data.migration.job.DataTask;

import java.util.List;

/**
 * @author Kkk
 * @Description: 数据分片接口
 * @date 2021/11/27
 */
public interface DataPartitioner {
    List<DataTask> partitioner(DataMigrationJob var1);
}