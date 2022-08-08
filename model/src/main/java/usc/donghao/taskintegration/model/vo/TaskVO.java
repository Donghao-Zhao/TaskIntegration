package usc.donghao.taskintegration.model.vo;

import java.util.List;

public class TaskVO {
    private String taskId;
    private String taskName;
    private String cron;
    private List<StepVO> steps;
    private String group;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public List<StepVO> getSteps() {
        return steps;
    }

    public void setSteps(List<StepVO> steps) {
        this.steps = steps;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
