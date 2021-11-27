package com.kkk.data.migration.job.partitioner;

import com.kkk.data.migration.enums.SliceStrategyType;
import com.kkk.data.migration.enums.TimeSliceIntervalUnit;
import com.kkk.data.migration.job.DataMigrationHandler;
import com.kkk.data.migration.job.DataMigrationJob;
import com.kkk.data.migration.job.DataTask;
import com.kkk.data.migration.sql.helper.JdbcHelper;
import com.kkk.data.migration.utils.DateSplitUtils;
import com.kkk.data.migration.utils.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.kkk.data.migration.utils.DateUtil.pattern3;

/**
 * @author Kkk
 * @Description: 数据分片接口-时间分片timeSliceColumn
 * @date 2021/11/27
 */
@Service
public class TimeSlicePartitioner implements DataPartitioner {
    private static Logger logger = LoggerFactory.getLogger(TimeSlicePartitioner.class);
    private static final String CHARACTER_LESSTHANSIGN="<=";
    @Resource
    private JdbcHelper jdbcHelperImpl;
    @Resource
    private DataMigrationHandler dataMigrationHandler;

    /**
     * 时间分片
     * @param dataMigrationJob
     * @return
     */
    @Override
    public List<DataTask> partitioner(DataMigrationJob dataMigrationJob) {
        logger.info("表({})待迁移总条数{}", dataMigrationJob.getTableName(),dataMigrationJob.getMigrationSize());
        logger.info("开始根据({})分片,分片单位({}),区间时长({})",dataMigrationJob.getTimeSliceColumn(),dataMigrationJob.getIntervalUnit(),dataMigrationJob.getInterval());
        //客户端传入的原始查询条件
        String origWhereClause = dataMigrationJob.getWhereClause();
        //客户端传入的分片字段
        String timeSliceColumn = dataMigrationJob.getTimeSliceColumn();
        //客户端传入区间开始时间
        String beginTimeStr =dataMigrationJob.getBeginTime();
        //客户端传入区间结束时间
        String endTimeStr =dataMigrationJob.getEndTime();
        //客户端传入区间段时长
        int interval = dataMigrationJob.getInterval();
        //客户端传入区间段时长单位
        TimeSliceIntervalUnit intervalUnit = dataMigrationJob.getIntervalUnit();

        Date startDate= DateUtil.changeFormat2Date(beginTimeStr,pattern3);
        Date endDate=DateUtil.changeFormat2Date(endTimeStr,pattern3);

        List<DateSplitUtils.DateSplit> result=DateSplitUtils.splitDate(startDate,endDate,intervalUnit,interval);
        List<DataTask> dataTaskList = new ArrayList();

        int i=0;
        int endNum=result.size();
        for (DateSplitUtils.DateSplit dateSplit : result) {
            logger.info("切割后的时间区间:{}-->{}",dateSplit.getStartDateTimeStr(),dateSplit.getEndDateTimeStr());
            DataTask dataTask=new DataTask();
            String startDateTimeStr = dateSplit.getStartDateTimeStr();
            String endDateTimeStr = dateSplit.getEndDateTimeStr();

            //重写sql
            StringBuilder stringBuilder = new StringBuilder(" ");
            if(i==0){
                stringBuilder.append("'").append(startDateTimeStr).append("'").append(CHARACTER_LESSTHANSIGN).append(timeSliceColumn).append(" and ").append(timeSliceColumn).append("<").append("'").append(endDateTimeStr).append("'").append(" and ").append(origWhereClause);
            }else if(i==endNum-1){
                stringBuilder.append("'").append(startDateTimeStr).append("'").append("<=").append(timeSliceColumn).append(" and ").append(timeSliceColumn).append(CHARACTER_LESSTHANSIGN).append("'").append(endDateTimeStr).append("'").append(" and ").append(origWhereClause);
            }else {
                stringBuilder.append("'").append(startDateTimeStr).append("'").append("<=").append(timeSliceColumn).append(" and ").append(timeSliceColumn).append("<").append("'").append(endDateTimeStr).append("'").append(" and ").append(origWhereClause);
            }
            dataTask.setWhereClause(stringBuilder.toString());
            dataTask.setTableName(dataMigrationJob.getTableName());
            dataTask.setHistoryTableName(dataMigrationJob.getHistoryTableName());
            dataTask.setModelClass(dataMigrationJob.getModleClazz());
            dataTask.setDataMigrationHandler(this.dataMigrationHandler);
            dataTask.setJdbcHelperImpl(this.jdbcHelperImpl);
            dataTask.setBeginTime(startDateTimeStr);
            dataTask.setEndTime(endDateTimeStr);
            dataTask.setSliceStrategyType(SliceStrategyType.TIME_SLICE);
            dataTaskList.add(dataTask);
            i++;
        }
        return dataTaskList;
    }
}

