package com.otis.udfs;



public class Pojo1 {
    public String ttime;
    public String status;

    public String getTtime() {
        return ttime;
    }

    public void setTtime(String ttime) {
        this.ttime = ttime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Pojo1() {
    }

    public Pojo1(String ttime, String status) {
        this.ttime = ttime;
        this.status = status;
    }
}
