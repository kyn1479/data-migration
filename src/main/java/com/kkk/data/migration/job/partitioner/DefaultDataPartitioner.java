package com.kkk.data.migration.job.partitioner;

import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.job.DataMigrationHandler;
import com.kkk.data.migration.job.DataMigrationJob;
import com.kkk.data.migration.job.DataTask;
import com.kkk.data.migration.sql.helper.JdbcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Kkk
 * @Description: 数据分片接口实现-默认分片Id
 * @date 2021/11/27
 */
@Service
public class DefaultDataPartitioner implements DataPartitioner {
    private static Logger logger = LoggerFactory.getLogger(DefaultDataPartitioner.class);
    @Resource
    private JdbcHelper jdbcHelperImpl;
    @Resource
    private DataMigrationHandler dataMigrationHandler;

    /**
     * 根据自增主键分片
     * @param dataMigrationJob
     * @return
     */
    @Override
    public List<DataTask> partitioner(DataMigrationJob dataMigrationJob) {
        List<DataTask> dataTaskList = new ArrayList();
        long totalCount = dataMigrationJob.getMigrationSize();
        logger.info("表({})待迁移总条数{}", dataMigrationJob.getTableName(),totalCount);
        logger.info("开始分片");
        int sliceSize = dataMigrationJob.getIdSliceSize();
        long indexId = 0L;
        int currentSliceSize = 0;

        for(int i = 0; (long)(i * sliceSize) < totalCount; ++i) {
            currentSliceSize = (long)((i + 1) * sliceSize) < totalCount ? sliceSize : (int)(totalCount - (long)(i * sliceSize));
            DataTask dataTask = new DataTask();
            long beginId = this.jdbcHelperImpl.selectBeginId(dataMigrationJob.getTableName(), dataMigrationJob.getWhereClause(), indexId, currentSliceSize);
            long endId = this.jdbcHelperImpl.selectEndId(dataMigrationJob.getTableName(), dataMigrationJob.getWhereClause(), indexId, currentSliceSize);
            indexId = endId;
            StringBuilder stringBuilder = new StringBuilder("id between ");
            stringBuilder.append(beginId).append(" and ").append(endId).append(" and ").append(dataMigrationJob.getWhereClause());
            dataTask.setWhereClause(stringBuilder.toString());
            dataTask.setHistoryTableName(dataMigrationJob.getHistoryTableName());
            dataTask.setModelClass(dataMigrationJob.getModleClazz());
            dataTask.setDataMigrationHandler(this.dataMigrationHandler);
            dataTask.setJdbcHelperImpl(this.jdbcHelperImpl);
            dataTask.setTableName(dataMigrationJob.getTableName());
            dataTask.setBeginId(beginId);
            dataTask.setEndId(endId);
            dataTask.setSliceCount(currentSliceSize);
            dataTask.setSliceStrategyType(SliceStrategyType.DEFAULT_ID);
            dataTaskList.add(dataTask);
            logger.info("第{}片完成，beginId({}),endId({}),currentSliceSize({})", new Object[]{i + 1, beginId, endId, currentSliceSize});
        }
        logger.info("分片结束");
        return dataTaskList;
    }
}

