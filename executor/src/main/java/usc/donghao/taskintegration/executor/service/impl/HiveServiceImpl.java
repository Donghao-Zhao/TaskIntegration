package usc.donghao.taskintegration.executor.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import usc.donghao.taskintegration.executor.service.HiveService;

@Service
public class HiveServiceImpl implements HiveService {
    private static final Logger appLogger = LoggerFactory.getLogger(HiveServiceImpl.class);
    @Autowired
    private JdbcTemplate hiveJdbcTemplate;
    @Override
    public void execute(String sql) {
        appLogger.info(sql);
        hiveJdbcTemplate.execute(sql);
        appLogger.info("Hive sql executed");
    }
}
