package com.otis.udfs;

import java.util.List;
import java.util.Map;

public class AccValue {
    public Map<String, List<Pojo1>> map;
    public int target;

    public Map<String, List<Pojo1>> getMap() {
        return map;
    }

    public void setMap(Map<String, List<Pojo1>> map) {
        this.map = map;
    }

    public int getTarget() {
        return target;
    }

    public void setTarget(int target) {
        this.target = target;
    }

    public AccValue() {
    }

    public AccValue(Map<String, List<Pojo1>> map, int target) {
        this.map = map;
        this.target = target;
    }
}
