package com.kkk.data.migration.job;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Kkk
 * @Description: Task并发执行器
 * @date 2021/11/27
 */
@Service
public class ConcurrentDataTaskExecutor implements DataTaskExecutor {
    private static Logger logger = LoggerFactory.getLogger(ConcurrentDataTaskExecutor.class);

    public void execute(List<DataTask> dataTaskList, DataMigrationJob dataMigrationJob) {
        ExecutorService executorService = dataMigrationJob.getThreadPoolExecutor();
        if (null == executorService) {
            executorService = new ThreadPoolExecutor(4, 8, 1L, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>(), new ThreadFactoryBuilder().setNameFormat("job-pool-"+dataMigrationJob.getJobTaskNo()).build(), new ThreadPoolExecutor.AbortPolicy());
        }

        ArrayList futureList = null;

        Iterator iterator;
        try {
            futureList = new ArrayList(dataTaskList.size());
            iterator = dataTaskList.iterator();

            while(iterator.hasNext()) {
                DataTask dataTask = (DataTask)iterator.next();
                Future<DataMigrationTaskResult> future = executorService.submit(dataTask);
                futureList.add(future);
            }
        } finally {
            executorService.shutdown();
        }

        iterator = futureList.iterator();

        while(iterator.hasNext()) {
            Future future = (Future)iterator.next();

            while(!future.isDone() || future.isCancelled()) {
                try {
                    long successSize=((DataMigrationTaskResult)future.get()).getSuccessSize();
                    long failSize=((DataMigrationTaskResult)future.get()).getFailSize();
                    logger.debug("获取迁移结果:successSize({}),failSize({})",successSize,failSize);
                } catch (Exception e) {
                    logger.error("迁移结果或者失败！",e);
                }
            }
        }

    }
}
