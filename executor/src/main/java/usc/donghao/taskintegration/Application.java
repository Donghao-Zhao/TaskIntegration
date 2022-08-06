package usc.donghao.taskintegration;

import org.quartz.*;
import usc.donghao.taskintegration.common.utils.ConfigUtil;
import usc.donghao.taskintegration.executor.job.HiveJob;
import usc.donghao.taskintegration.executor.listener.OrderListener;
import usc.donghao.taskintegration.executor.service.HiveService;
import usc.donghao.taskintegration.model.scheduler.JobDescriptor;
import usc.donghao.taskintegration.model.vo.ConfigVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import usc.donghao.taskintegration.model.vo.StepVO;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.util.*;


@SpringBootApplication
public class Application {

    @Autowired
    private ConfigUtil configUtil;

    @Autowired
    private Scheduler scheduler;

    private static final Logger appLogger = LoggerFactory.getLogger(Application.class);
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @PostConstruct
    public void run() throws FileNotFoundException {
        appLogger.info("TaskIntegration begin to run");
        ConfigVO configVO = configUtil.getConfigVO();
        appLogger.info("Get ConfigVO");
        List<ConfigVO.DataSource> dataSources = configVO.getDataSources();
        appLogger.info("Get Data Source Config");
        configVO.getTasks().forEach(taskVO -> {
            String taskId = taskVO.getTaskId();
            String taskName = taskVO.getTaskName();
            String cron = taskVO.getCron();

            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(taskName);

            Map<String,Object> paramMap = new HashMap<>();
            paramMap.put("taskName",taskName);
            paramMap.put("taskId",taskId);
            paramMap.put("cron",cron);
            jobDescriptor.setDataMap(paramMap);
            OrderListener orderListener = new OrderListener(taskName + "OrderListener");


            appLogger.info("Get task " + taskId + "'s Config");
            taskVO.getSteps().sort(new Comparator<StepVO>() {
                @Override
                public int compare(StepVO o1, StepVO o2) {
                    return Integer.parseInt(o1.getOrder()) - Integer.parseInt(o2.getOrder());
                }
            });
            Queue<JobDetail> jobDetailQueue = new LinkedList<>();
                taskVO.getSteps().forEach(stepVO -> {
                appLogger.info("Get step " + stepVO.getOrder() + "'s Config");
                String type = stepVO.getType();
                jobDescriptor.setName(stepVO.getStepName());
                if (type.equals("hive")) {
                    jobDescriptor.setJobClazz(HiveJob.class);
                    paramMap.put("order",stepVO.getOrder());
                    paramMap.put("stepName",stepVO.getStepName());
                    paramMap.put("path",stepVO.getPath());
                    paramMap.put("hiveParam",stepVO.getHiveParam());
                }else if(type.equals("spark")) {
                    //jobDescriptor.setJobClazz(SparkJob.class);
                    //paramMap.put("path",stepVO.getPath());
                    //paramMap.put("hiveParam",stepVO.getHiveParam());
                }
                JobDetail jobDetail = jobDescriptor.buildJobDetail();
                jobDetailQueue.add(jobDetail);

            });
                orderListener.setJobDetailQueue(jobDetailQueue);

            Trigger jobTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(taskName)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();
            try {
                scheduler.getListenerManager().addJobListener(orderListener);
                if(!jobDetailQueue.isEmpty()){
                    JobDetail initJobDetail = jobDetailQueue.poll();
                    //scheduler.addJob(initJobDetail,true);
                    scheduler.scheduleJob(initJobDetail,jobTrigger);
                }else{
                    appLogger.warn(taskName + "'s Step List is empty");
                }

            } catch (SchedulerException e) {
                appLogger.error("Error",e);
            }
        });
    }
}
