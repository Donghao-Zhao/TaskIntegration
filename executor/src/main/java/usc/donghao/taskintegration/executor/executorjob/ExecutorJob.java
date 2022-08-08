package usc.donghao.taskintegration.executor.executorjob;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import usc.donghao.taskintegration.executor.job.HiveJob;
import usc.donghao.taskintegration.executor.job.ScriptJob;
import usc.donghao.taskintegration.executor.job.SparkJob;
import usc.donghao.taskintegration.executor.listener.OrderListener;
import usc.donghao.taskintegration.model.scheduler.JobDescriptor;
import usc.donghao.taskintegration.model.vo.StepVO;
import usc.donghao.taskintegration.model.vo.TaskVO;

import java.util.*;

public class ExecutorJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(ExecutorJob.class);
    @Autowired
    private Scheduler scheduler;
    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        JobDataMap paramMap = jobExecutionContext.getMergedJobDataMap();
        String taskName = paramMap.getString("taskName");
        String taskId = paramMap.getString("taskId");
        String group = paramMap.getString("group");
        TaskVO taskVO = (TaskVO) paramMap.get("taskVO");
        OrderListener orderListener = new OrderListener(taskName + "OrderListener");


        appLogger.info("Get task " + taskId + "'s Config");
        taskVO.getSteps().sort(new Comparator<StepVO>() {
            @Override
            public int compare(StepVO o1, StepVO o2) {
                return Integer.parseInt(o1.getOrder()) - Integer.parseInt(o2.getOrder());
            }
        });
        Queue<JobDetail> jobDetailQueue = new LinkedList<>();
        Map<String,String> statusMap = new HashMap<>();
        taskVO.getSteps().forEach(stepVO -> {
            appLogger.info("Get step " + stepVO.getOrder() + "'s Config");
            statusMap.put(stepVO.getStepName(),"new");
            String type = stepVO.getType();
            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(group);
            jobDescriptor.setName(taskName);
            paramMap.put("order",stepVO.getOrder());
            paramMap.put("stepName",stepVO.getStepName());
            jobDescriptor.setName(stepVO.getStepName());
            if (type.equals("hive")) {
                jobDescriptor.setJobClazz(HiveJob.class);
                paramMap.put("path",stepVO.getPath());
                paramMap.put("hiveParam",stepVO.getHiveParam());
            }else if(type.equalsIgnoreCase("script")){
                jobDescriptor.setJobClazz(ScriptJob.class);
                paramMap.put("path",stepVO.getPath());
                paramMap.put("param",stepVO.getParam());
                paramMap.put("mode",stepVO.getMode());
            }else if(type.equalsIgnoreCase("spark")){
                jobDescriptor.setJobClazz(SparkJob.class);
                paramMap.put("path",stepVO.getPath());
                paramMap.put("master",stepVO.getMaster());
                paramMap.put("deployMode",stepVO.getDeployMode());
                paramMap.put("className",stepVO.getClassName());
                paramMap.put("sparkLogPath",stepVO.getSparkLogPath());
            }
            jobDescriptor.setDataMap(paramMap);
            JobDetail jobDetail = jobDescriptor.buildJobDetail();
            jobDetailQueue.add(jobDetail);

        });
        List<String> runningJobList = new ArrayList<>();
        orderListener.setJobDetailQueue(jobDetailQueue);
        orderListener.setStatusMap(statusMap);
        orderListener.setRunningJobList(runningJobList);


        try {
            scheduler.getListenerManager().addJobListener(orderListener);
            if(jobDetailQueue.isEmpty()) {
                appLogger.warn(taskName + "'s Step List is empty");
                return;

            }
            while(!jobDetailQueue.isEmpty() && jobDetailQueue.peek().getJobDataMap().getString("order").equals("1")){
                JobDetail initJobDetail = jobDetailQueue.poll();
                String stepName = initJobDetail.getJobDataMap().getString("stepName");
                runningJobList.add(stepName);
                statusMap.put(stepName,"processing");
                appLogger.info(stepName + " is running");
                scheduler.addJob(initJobDetail,true);
                scheduler.triggerJob(initJobDetail.getKey());
            }


        } catch (Exception e) {
            appLogger.error("Error",e);
        }
    }
}
