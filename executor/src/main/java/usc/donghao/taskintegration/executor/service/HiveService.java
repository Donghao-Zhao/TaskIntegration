package usc.donghao.taskintegration.executor.service;

import java.util.List;
import java.util.Map;

public interface HiveService {

    void execute(String sql);

    List<Map<String, Object>> queryForList(String sql);

    Map<String, Object> queryForMap(String sql);

    void runHql(String filePath, Map<String, Object> paramMap);
}
