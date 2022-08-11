package usc.donghao.taskintegration.executor.job;

import usc.donghao.taskintegration.executor.handler.DataTransferHandler;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataloaderJob implements Job {
    private static final Logger appLogger = LoggerFactory.getLogger(DataloaderJob.class);

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        appLogger.info("Begin to run Dataloader job");
        JobDataMap paramMap=jobExecutionContext.getMergedJobDataMap();
        Map<String,Object> dataSourceMap= (Map<String, Object>) paramMap.get("datasourceMap");
        try {
            JdbcTemplate sourceJtm= (JdbcTemplate) dataSourceMap.get(paramMap.getString("sourceDataSource"));
            JdbcTemplate destJtm= (JdbcTemplate) dataSourceMap.get(paramMap.getString("destDataSource"));
            List<String> sourceTables= (List<String>) paramMap.get("sourceTables");
            List<String> destTables= (List<String>) paramMap.get("sourceTables");
            if(sourceTables==null || destTables==null || sourceTables.isEmpty() || destTables.isEmpty() || sourceTables.size()!=destTables.size()){
                throw new Exception("source table list and dest table list must have value and same size");
            }
            for(int i=0;i<sourceTables.size();i++){
                String sourceTable=sourceTables.get(i);
                String destTable=destTables.get(i);
                appLogger.info("Being to load data from "+sourceTable+" to "+destTable);
                /**
                 * id name age
                 * 1  John  10
                 * 2  Paul  5
                 * */
                appLogger.info("select * from "+sourceTable);
                DataTransferHandler dataTransferHandler=new DataTransferHandler(destJtm,destTable);
                sourceJtm.query("select * from "+sourceTable,dataTransferHandler);
                dataTransferHandler.saveRest();
                appLogger.info("Load data from "+sourceTable+" to "+destTable+" complete");
            }
            appLogger.info("Dataloader job is done");
        } catch (Exception e) {
            appLogger.error("Error:",e);
        }
    }
}

