package usc.donghao.taskintegration.executor.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.PathResource;
import org.springframework.data.hadoop.hive.HiveScript;
import org.springframework.data.hadoop.hive.HiveTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import usc.donghao.taskintegration.executor.service.HiveService;

import java.util.List;
import java.util.Map;

@Service
public class HiveServiceImpl implements HiveService {
    private static final Logger appLogger = LoggerFactory.getLogger(HiveServiceImpl.class);
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @Autowired
    private HiveTemplate hiveTemplate;

    @Override
    public void execute(String sql) {
        appLogger.info("Run hive execute");
        appLogger.info(sql);
        hiveJdbcTemplate.execute(sql);
        appLogger.info("Hive sql executed");
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        appLogger.info("Run hive queryForList");
        appLogger.info(sql);
        List<Map<String, Object>> resMap = hiveJdbcTemplate.queryForList(sql);
        appLogger.info("Hive sql executed");
        return resMap;
    }

    @Override
    public Map<String, Object> queryForMap(String sql) {
        appLogger.info("Run hive queryForMap");
        appLogger.info(sql);
        Map<String, Object> resMap = hiveJdbcTemplate.queryForMap(sql);
        appLogger.info("Hive sql executed");
        return resMap;
    }

    @Override
    public void runHql(String filePath, Map<String, Object> paramMap){
        appLogger.info("Run hive runHql");
        HiveScript hiveScript = new HiveScript(new PathResource(filePath),paramMap);
        hiveTemplate.executeScript(hiveScript);
        appLogger.info("HQL run success");
    }
}
