package usc.donghao.taskintegration;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import usc.donghao.taskintegration.common.utils.ConfigUtil;
import usc.donghao.taskintegration.executor.executorjob.ExecutorJob;
import usc.donghao.taskintegration.model.scheduler.JobDescriptor;
import usc.donghao.taskintegration.model.vo.ConfigVO;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
            String group = taskVO.getGroup();
            JobDescriptor jobDescriptor = new JobDescriptor();
            jobDescriptor.setGroup(group);
            jobDescriptor.setName(taskName);
            jobDescriptor.setJobClazz(ExecutorJob.class);
            Map<String, Object> paramMap = new HashMap<>();
            paramMap.put("taskName", taskName);
            paramMap.put("taskId", taskId);
            paramMap.put("cron", cron);
            paramMap.put("taskVO", taskVO);
            paramMap.put("group", group);
            jobDescriptor.setDataMap(paramMap);
            JobDetail executorJobDetail = jobDescriptor.buildJobDetail();
            Trigger jobTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(taskName,group)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();

            try {
                scheduler.scheduleJob(executorJobDetail,jobTrigger);
            } catch (SchedulerException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
