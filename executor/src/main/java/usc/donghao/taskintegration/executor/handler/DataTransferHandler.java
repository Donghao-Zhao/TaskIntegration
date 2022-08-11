package usc.donghao.taskintegration.executor.handler;

import usc.donghao.taskintegration.executor.job.DataloaderJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

public class DataTransferHandler implements RowCallbackHandler {
    private static final Logger appLogger = LoggerFactory.getLogger(DataTransferHandler.class);

    private int cache=3;
    private List<Map<String,Object>> rowList=new ArrayList<>();
    private JdbcTemplate destJtm;
    private String destTable;
    public DataTransferHandler(JdbcTemplate destJtm,String destTable){
        this.destJtm=destJtm;
        this.destTable=destTable;
    }
    @Override
    public void processRow(ResultSet rs) throws SQLException {
        ResultSetMetaData metaData=rs.getMetaData();
        int columnCount=metaData.getColumnCount();
        Map<String,Object> row=new HashMap<>();
        for(int i=1;i<=columnCount;i++){
            String[] strArr=metaData.getColumnName(i).split("\\.");
            row.put(strArr[1],rs.getObject(i));
        }
        rowList.add(row);
        if(rowList.size()==cache){
            batchInsert(rowList);
            rowList.clear();
        }
    }

    public void saveRest(){
        if(!rowList.isEmpty()){
            appLogger.info("Begin to add "+rowList.size()+" records to "+destTable);
            batchInsert(rowList);
            appLogger.info("Done");

        }
    }

    private void batchInsert(List<Map<String,Object>> res){
        appLogger.info("Begin to insert "+res.size()+" records to "+destTable);
        Set<String> keySet= res.get(0).keySet();
        List<String> keyList=new ArrayList(keySet);

        List<String> valueStrList=new ArrayList<>();
        res.forEach(rowMap->{
            List<String> valueList=new ArrayList<>();
            keyList.forEach(key->{
                valueList.add("'"+String.valueOf(rowMap.get(key))+"'");
            });
            String valueStr="("+String.join(",",valueList)+")";
            valueStrList.add(valueStr);
        });
        appLogger.info("insert into "+destTable+"("+String.join(",",keyList)+") values "+String.join(",",valueStrList));
        destJtm.execute("insert into "+destTable+"("+String.join(",",keyList)+") values "+String.join(",",valueStrList));
        appLogger.info("Batch Insert is done");
    }
}

