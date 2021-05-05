package com.hcb.structure;

public class CAttribute {

    static final String GROUP_NAME = "staff";
    static final short DEFAULT_MODE = (short)493;
    private String owner = "";
    private String group = GROUP_NAME;
    private short mode = DEFAULT_MODE;
    private String xattr = "";

    public CAttribute(){}

    public CAttribute(String owner) {
        this.owner = owner;
    }

    public CAttribute(String owner, String group, short mode, String xattr) {
        this.owner = owner;
        this.group = group;
        this.mode = mode;
        this.xattr = xattr;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public short getMode() {
        return mode;
    }

    public void setMode(short mode) {
        this.mode = mode;
    }

    public String getXattr() {
        return xattr;
    }

    public void setXattr(String xattr) {
        this.xattr = xattr;
    }

    @Override
    public String toString() {
        return "CAttribute{" +
                "owner='" + owner + '\'' +
                ", group='" + group + '\'' +
                ", mode=" + mode +
                ", xattr='" + xattr + '\'' +
                '}';
    }
}
