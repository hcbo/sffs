package com.hcb.structure;

public class WAttribute {

    private long ctime;
    private long mtime;
    private long size;

    public WAttribute(){}

    public WAttribute(long ctime, long mtime, long size) {
        this.ctime = ctime;
        this.mtime = mtime;
        this.size = size;
    }

    public long getCtime() {
        return ctime;
    }

    public void setCtime(long ctime) {
        this.ctime = ctime;
    }

    public long getMtime() {
        return mtime;
    }

    public void setMtime(long mtime) {
        this.mtime = mtime;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    @Override
    public String toString() {
        return "WAttribute{" +
                "ctime=" + ctime +
                ", mtime=" + mtime +
                ", size=" + size +
                '}';
    }
}
