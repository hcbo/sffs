package com.hcb.structure;

public class AAttribute {
    private long atime;

    public AAttribute(){}

    public AAttribute(long atime) {
        this.atime = atime;
    }

    public long getAtime() {
        return atime;
    }

    public void setAtime(long atime) {
        this.atime = atime;
    }

    @Override
    public String toString() {
        return "AAttribute{" +
                "atime=" + atime +
                '}';
    }
}
