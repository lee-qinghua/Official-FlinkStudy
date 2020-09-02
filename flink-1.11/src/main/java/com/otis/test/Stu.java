package com.otis.test;

public class Stu {
    public String action;
    public Integer age;
    public long ts;

    public Stu() {
    }

    public Stu(String action, Integer age, long ts) {
        this.action = action;
        this.age = age;
        this.ts = ts;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }
}
