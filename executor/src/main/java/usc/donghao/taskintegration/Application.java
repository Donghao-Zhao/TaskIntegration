package usc.donghao.taskintegration;

import org.quartz.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;
import usc.donghao.taskintegration.common.utils.ConfigUtil;
import usc.donghao.taskintegration.executor.executorjob.ExecutorJob;
import usc.donghao.taskintegration.model.scheduler.JobDescriptor;
import usc.donghao.taskintegration.model.vo.ConfigVO;
import usc.donghao.taskintegration.executor.job.HiveJob;
import usc.donghao.taskintegration.executor.listener.OrderListener;
import usc.donghao.taskintegration.executor.service.HiveService;
import usc.donghao.taskintegration.model.vo.StepVO;

import javax.annotation.PostConstruct;
import java.io.FileNotFoundException;
import java.sql.Driver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@SpringBootApplication
public class Application {

    @Autowired
    private ConfigUtil configUtil;

    @Autowired
    private Scheduler scheduler;

    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @Autowired
    private HiveTemplate hiveTemplate;

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
            paramMap.put("datasourceMap",getDataSourceMap(dataSources));

            jobDescriptor.setDataMap(paramMap);
            JobDetail executorJobDetail = jobDescriptor.buildJobDetail();

            Trigger jobTrigger = TriggerBuilder.newTrigger()
                    .withIdentity(taskName,group)
                    .withSchedule(CronScheduleBuilder.cronSchedule(cron))
                    .build();

            try {
                scheduler.scheduleJob(executorJobDetail,jobTrigger);
            } catch (SchedulerException e) {
                e.printStackTrace();
            }

        });
    }

    public Map<String,Object> getDataSourceMap(List<ConfigVO.DataSource> dataSources){
        Map<String,Object> dataSourceMap=new HashMap<>();
        dataSources.forEach(dataSource -> {
            if(dataSource.getName().equals("hive")){
                dataSourceMap.put("hive",hiveJdbcTemplate);
                dataSourceMap.put("hiveTemplate",hiveTemplate);
            }else{
                SimpleDriverDataSource ds=new SimpleDriverDataSource();
                Class<?> cls=null;
                try{
                    cls=Class.forName(dataSource.getDriver());
                    ds.setDriverClass((Class<? extends Driver>) cls);
                    ds.setUrl(dataSource.getUrl());
                    ds.setUsername(dataSource.getUserName());
                    ds.setPassword(dataSource.getPassword());
                    JdbcTemplate jtm=new JdbcTemplate(ds);
                    dataSourceMap.put(dataSource.getName(),jtm);
                }catch (Exception e){
                    appLogger.error("Error",e);
                }
            }
        });
        return dataSourceMap;
    }
}
