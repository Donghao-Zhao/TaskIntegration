package usc.donghao.taskintegration.model.vo;

import java.util.List;
import java.util.Map;

public class StepVO {
    private String stepName;
    private String type;
    private String order;

    private String path;    //Some have, some doesn't
    private String mode;

    //hive
    private Map<String, Object> hiveParam;

    //spark
    private String master;
    private String deployMode;
    private String className;
    private String sparkLogPath;

    //hdfs
    private String source;
    private String destination;

    //script
    private String param;

    //dataLoader
    private String sourceDataSource;
    private String destDataSource;
    private List<String> sourceTables;
    private List<String> destTables;
    private String sourcePath;
    private List<String> sourceCSV;

    //custom
    private String methodName;
    private List<String> funcParamList;

    public String getStepName() {
        return stepName;
    }

    public void setStepName(String stepName) {
        this.stepName = stepName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Map<String, Object> getHiveParam() {
        return hiveParam;
    }

    public void setHiveParam(Map<String, Object> hiveParam) {
        this.hiveParam = hiveParam;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getParam() {
        return param;
    }

    public void setParam(String param) {
        this.param = param;
    }

    public String getSourceDataSource() {
        return sourceDataSource;
    }

    public void setSourceDataSource(String sourceDataSource) {
        this.sourceDataSource = sourceDataSource;
    }

    public String getDestDataSource() {
        return destDataSource;
    }

    public void setDestDataSource(String destDataSource) {
        this.destDataSource = destDataSource;
    }

    public List<String> getSourceTables() {
        return sourceTables;
    }

    public void setSourceTables(List<String> sourceTables) {
        this.sourceTables = sourceTables;
    }

    public List<String> getDestTables() {
        return destTables;
    }

    public void setDestTables(List<String> destTables) {
        this.destTables = destTables;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public List<String> getSourceCSV() {
        return sourceCSV;
    }

    public void setSourceCSV(List<String> sourceCSV) {
        this.sourceCSV = sourceCSV;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String getSparkLogPath() {
        return sparkLogPath;
    }

    public void setSparkLogPath(String sparkLogPath) {
        this.sparkLogPath = sparkLogPath;
    }

    public List<String> getFuncParamList() {
        return funcParamList;
    }

    public void setFuncParamList(List<String> funcParamList) {
        this.funcParamList = funcParamList;
    }
}
