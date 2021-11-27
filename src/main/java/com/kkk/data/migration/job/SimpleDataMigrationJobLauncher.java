package com.kkk.data.migration.job;

import com.alibaba.fastjson.JSON;
import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.job.partitioner.DataPartitioner;
import com.kkk.data.migration.sql.helper.JdbcHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author Kkk
 * @Description: Job执行器
 * @date 2021/11/27
 */
@Service("simpleDataMigrationJobLauncher")
public class SimpleDataMigrationJobLauncher implements DataMigrationJobLauncher {
    private Logger logger = LoggerFactory.getLogger(SimpleDataMigrationJobLauncher.class);
    @Resource
    private DataPartitioner defaultDataPartitioner;
    @Resource
    private DataPartitioner timeSlicePartitioner;
    @Resource
    private DataTaskExecutor concurrentDataTaskExecutor;
    @Resource
    private JdbcHelper jdbcHelperImpl;

    public DataMigrationResult run(DataMigrationJob dataMigrationJob) {
        logger.info("SimpleDataMigrationJobLauncher执行器入参:{}", JSON.toJSONString(dataMigrationJob));
        DataMigrationResult dataMigrationResult = new DataMigrationResult();
        this.preHandler(dataMigrationResult, dataMigrationJob);
        if (dataMigrationJob.getMigrationSize() == 0L) {
            logger.warn("表({})没有需要迁移的数据,退出!",dataMigrationJob.getTableName());
            return dataMigrationResult;
        } else {
            List<DataTask> dataTaskList =null;
            if(SliceStrategyType.DEFAULT_ID.equals(dataMigrationJob.getSliceStrategyType())){
                dataTaskList = this.defaultDataPartitioner.partitioner(dataMigrationJob);
            }else {
                dataTaskList = this.timeSlicePartitioner.partitioner(dataMigrationJob);
            }
            concurrentDataTaskExecutor.execute(dataTaskList, dataMigrationJob);
            postHandler(dataMigrationResult, dataMigrationJob);
            logger.info("({})-->({})数据迁移,预迁移量({}),迁出表迁移之前数据量({}),迁出表迁移之后数据量({})，迁入表迁移之前数据量({}),迁入表迁移之后数据量({})",
                    dataMigrationJob.getTableName(), dataMigrationJob.getHistoryTableName(),
                    dataMigrationResult.getMigrationSize(),dataMigrationResult.getOldOriginalCount(),dataMigrationResult.getNewOriginalCount(),
                    dataMigrationResult.getOldHistoryCount(),dataMigrationResult.getNewHistoryCount()
                    );
            return dataMigrationResult;
        }
    }

    /**
     * 前置处理器
     * @param dataMigrationResult
     * @param dataMigrationJob
     */
    private void preHandler(DataMigrationResult dataMigrationResult, DataMigrationJob dataMigrationJob) {
        long oldOriginalCount = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getTableName());
        long oldHistoryCount = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getHistoryTableName());
        long migrationSize =0;
        if(dataMigrationJob.getSliceStrategyType().equals(SliceStrategyType.DEFAULT_ID)){
            //id分片
            migrationSize = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getTableName(), dataMigrationJob.getWhereClause());
        }else {
            //time分片-将时间区间条件拼接进sql条件中
            String whereClause = dataMigrationJob.getWhereClause();
            String beginTime = dataMigrationJob.getBeginTime();
            String endTime = dataMigrationJob.getEndTime();
            String timeSliceColumn = dataMigrationJob.getTimeSliceColumn();
            StringBuilder stringBuilder = new StringBuilder(" ");
            stringBuilder.append("'").append(beginTime).append("'").append("<=").append(timeSliceColumn).append(" and ").append(timeSliceColumn).append("<=").append("'").append(endTime).append("'").append(" and ").append(whereClause);
            migrationSize = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getTableName(),stringBuilder.toString());
        }
        dataMigrationJob.setMigrationSize(migrationSize);
        dataMigrationResult.setOldOriginalCount(oldOriginalCount);
        dataMigrationResult.setOldHistoryCount(oldHistoryCount);
        dataMigrationResult.setMigrationSize(migrationSize);
    }

    /**
     * 后置处理器
     * @param dataMigrationResult
     * @param dataMigrationJob
     */
    private void postHandler(DataMigrationResult dataMigrationResult, DataMigrationJob dataMigrationJob) {
        long newOriginalCount = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getTableName());
        long newHistoryCOunt = jdbcHelperImpl.selectTotalCount(dataMigrationJob.getHistoryTableName());
        dataMigrationResult.setNewOriginalCount(newOriginalCount);
        dataMigrationResult.setNewHistoryCount(newHistoryCOunt);
    }
}
