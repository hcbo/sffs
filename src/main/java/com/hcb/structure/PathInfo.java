package com.hcb.structure;

public class PathInfo {

    private String pathName = "";
    private boolean isDirectory;

    public CAttribute cAttr;
    public AAttribute aAttr;
    public WAttribute wAttr;

    public PathInfo(String pathName, boolean isDirectory) {
        this.pathName = pathName;
        this.isDirectory = isDirectory;
        cAttr = new CAttribute();
        aAttr = new AAttribute();
        wAttr = new WAttribute();
    }

    public String getPathName() {
        return pathName;
    }

    public void setPathName(String pathName) {
        this.pathName = pathName;
    }

    public boolean isDirectory() {
        return isDirectory;
    }

    public void setDirectory(boolean directory) {
        isDirectory = directory;
    }

    @Override
    public String toString() {
        return "PathInfo{" +
                "pathName='" + pathName + '\'' +
                ", isDirectory=" + isDirectory +
                ", cAttr=" + cAttr +
                ", aAttr=" + aAttr +
                ", wAttr=" + wAttr +
                '}';
    }
}
