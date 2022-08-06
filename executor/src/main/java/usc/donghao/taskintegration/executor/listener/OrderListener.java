package usc.donghao.taskintegration.executor.listener;

import org.quartz.*;
import org.quartz.listeners.JobChainingJobListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import usc.donghao.taskintegration.Application;
import usc.donghao.taskintegration.model.scheduler.JobDescriptor;

import java.util.Queue;

public class OrderListener extends JobChainingJobListener {

    private static final Logger appLogger = LoggerFactory.getLogger(OrderListener.class);

    private Queue<JobDetail> jobDetailQueue;

    public Queue<JobDetail> getJobDetailQueue() {
        return jobDetailQueue;
    }

    public void setJobDetailQueue(Queue<JobDetail> jobDetailQueue) {
        this.jobDetailQueue = jobDetailQueue;
    }

    public OrderListener(String name) {
        super(name);
    }

    @Override
    public void jobWasExecuted(JobExecutionContext jobExecutionContext, JobExecutionException e) {
        JobDetail curJobDetail = jobExecutionContext.getJobDetail();
        appLogger.info(curJobDetail.getJobDataMap().getString("stepName") + " is done");
        try{
            if(!jobDetailQueue.isEmpty()){
                JobDetail nextJobDetail = jobDetailQueue.poll();
                appLogger.info(nextJobDetail.getJobDataMap().getString("stepName") + " is running");
                jobExecutionContext.getScheduler().addJob(nextJobDetail,true);
                jobExecutionContext.getScheduler().triggerJob(nextJobDetail.getKey());
            }else{
                appLogger.info("All jobs are done");
            }
        }catch (Exception ex)
        {
            appLogger.error("Error:",ex);
        }

    }
}
