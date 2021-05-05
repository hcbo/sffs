package com.hcb;

import com.google.gson.Gson;
import com.hcb.SConHash.SConHashClient;
import com.hcb.SConHash.SConUtils;
import com.hcb.connection.SSDBConnection;
import com.hcb.structure.AAttribute;
import com.hcb.structure.CAttribute;
import com.hcb.structure.PathInfo;
import com.hcb.structure.WAttribute;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSDBUnderFileSystem {

    public static final String SEPARATOR = "::";
    public static final String METADATA = "::attr";
    public static final String FILEDATA = "::data";
    public static final String INDEX = "::index";
    public static String uriString; // sffs://192.168.225.6:8888
    public static Map<String, SSDBConnection> clients = new HashMap<>();

    private Gson gson = new Gson();

    public SSDBUnderFileSystem(URI uri, Configuration conf) {
        SffsFileSystem.LOG.error("SSDBUnderFileSystem 构造方法开始");
        //uri.toString :   sffs://192.168.225.6:8888
        uriString = uri.toString();
        String masterIp = getMasterIp(uriString);
        String masterPort = getMasterPort(uriString);
        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://@"+ masterIp + ":" + masterPort));
        RedisConnection<String, String> connection = redisClient.connect();
        RedisConnection<String, byte[]> dataConnection = redisClient.connect(new StringByteCodec());
        SConHashClient.refreshSconNodes(connection);

        clients.put(SConHashClient.realNodes[0],new SSDBConnection(redisClient,
                connection, dataConnection));
        for (int i = 1; i < SConHashClient.realNodes.length; i++) {
            String realNodeStr = SConHashClient.realNodes[i];
            String ip = realNodeStr.split(":")[0];
            String port = realNodeStr.split(":")[1];
            clients.put(realNodeStr, new SSDBConnection(ip ,port));
        }

    }

    private String getMasterPort(String uriString) {
        return uriString.substring(uriString.lastIndexOf(':') + 1);
    }

    private String getMasterIp(String uriStr) {
        return uriStr.substring(uriStr.indexOf("//") + 2,
                uriStr.lastIndexOf(':') );
    }



    public InputStream open(String path) throws IOException {
        SffsFileSystem.LOG.error("open()方法执行 path="+path);
        path = trimPath(path);
        return new RfsFileInputStream(path);
    }


    public OutputStream create(String path, short mode) throws IOException {
        SffsFileSystem.LOG.error("create()方法执行 path=" + path);
        path = trimPath(path);
        return new RfsFileOutputStream(path , mode);
    }

    // only support in the same dir
    public boolean renameFile(String src, String dst) {
//        SffsFileSystem.LOG.error("renameFile()方法执行 src="+src+" dst"+dst);
//        dst = trimPath(dst);
//
//        String pathInfoJson = connection.get(SSDBUnderFileSystem.METADATA + dst);
//        PathInfo pathInfo = gson.fromJson(pathInfoJson, PathInfo.class);
//        connection.set(SSDBUnderFileSystem.METADATA + dst, gson.toJson(pathInfo));


        return true;
    }

    public FileStatus[] listStatus(String path) throws FileNotFoundException {
        SffsFileSystem.LOG.error("listStatus()方法执行 path="+path);
        path = trimPath(path);
        String uuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(path));
        String dirMetaKey = path + uuid + SSDBUnderFileSystem.METADATA;
        String pathAAttrKey = dirMetaKey + "::a";
        String server = SConHashClient.getRealServer(path);
        SSDBConnection ssdbConnection = clients.get(server);
        if (!ssdbConnection.connection.exists(pathAAttrKey)) {
            throw new FileNotFoundException();
        }

        String pathIndexKey = path + uuid + SSDBUnderFileSystem.INDEX + "::0";

        List<String> subPaths =
                ssdbConnection.connection.hkeys(pathIndexKey);

        String logFormat = String.join(",", subPaths);
        SffsFileSystem.LOG.error("index hashmap : "+ logFormat);

        FileStatus[] fileStatuses = new FileStatus[subPaths.size()];
        int i = 0;
        String childMetaKey;
        for (String subPath : subPaths) {
            String childUuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(subPath));
            childMetaKey = subPath + childUuid + SSDBUnderFileSystem.METADATA;
            RedisConnection<String, String> childConnection = getConnection(subPath);
            String jsonAAttr =
                    childConnection.get(childMetaKey + "::a");
            AAttribute childAAttr = gson.fromJson(jsonAAttr, AAttribute.class);
            String jsonCAttr =
                    childConnection.get(childMetaKey + "::c");
            CAttribute childCAttr = gson.fromJson(jsonCAttr, CAttribute.class);
            String jsonWAttr =
                    childConnection.get(childMetaKey + "::w");
            WAttribute childWAttr = gson.fromJson(jsonWAttr, WAttribute.class);

            int dirFlag = Integer.parseInt(ssdbConnection.connection.hget(pathIndexKey, subPath));
            boolean isDir = (dirFlag == 0);
            FileStatus fileStatus = new FileStatus(childWAttr.getSize(), isDir, 1,
                    512L, childWAttr.getMtime(), childAAttr.getAtime(),
                    new FsPermission(childCAttr.getMode()), childCAttr.getOwner(),
                    childCAttr.getOwner(),new Path(subPath));

            fileStatuses[i++] = fileStatus;
        }
        return fileStatuses;
    }

    public boolean mkdirs(String path) {
        SffsFileSystem.LOG.error("mkdirs()方法执行 path="+path);
        path = trimPath(path);
        String uuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(path));
        String dirKey = path + uuid + SSDBUnderFileSystem.METADATA;

        String pathAAttrKey = dirKey + "::a";
        String server = SConHashClient.getRealServer(path);
        SSDBConnection ssdbConnection = clients.get(server);
        if (ssdbConnection.connection.exists(pathAAttrKey)) {
            return false;
        } else {
            mkParentDirsRecv(path);
            return true;
        }
    }

    private void mkParentDirsRecv(String path) {
        String dirUuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(path));
        String metadataKey =  path + dirUuid + SSDBUnderFileSystem.METADATA;
        String parentPath = getParentPath(path);
        RedisConnection<String, String> connection = getConnection(path);
        if (parentPath == null || parentPath == "") {
            // path 为根目录
            if (!connection.exists(metadataKey + "::a")) {
                long timeStamp = System.currentTimeMillis();
                PathInfo pathInfo = new PathInfo(path, true);
                pathInfo.aAttr.setAtime(timeStamp);
                pathInfo.wAttr.setCtime(timeStamp);
                pathInfo.wAttr.setMtime(timeStamp);
                pathInfo.wAttr.setSize(0);
                connection.set(metadataKey + "::c", gson.toJson(pathInfo.cAttr));
                connection.set(metadataKey + "::a", gson.toJson(pathInfo.aAttr));
                connection.set(metadataKey + "::w", gson.toJson(pathInfo.wAttr));
            }
            return;
        } else {
            mkParentDirsRecv(parentPath);
            String parDirUuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(parentPath));
            String parentIndexKey = parentPath + parDirUuid +
                    SSDBUnderFileSystem.INDEX + "::0";
            RedisConnection<String, String> parConnection = getConnection(parentPath);
            parConnection.hset(parentIndexKey, path, "0");
            long timeStamp = System.currentTimeMillis();
            PathInfo pathInfo = new PathInfo(path, true);
            pathInfo.aAttr.setAtime(timeStamp);
            pathInfo.wAttr.setCtime(timeStamp);
            pathInfo.wAttr.setMtime(timeStamp);
            pathInfo.wAttr.setSize(0);
            connection.set(metadataKey + "::c", gson.toJson(pathInfo.cAttr));
            connection.set(metadataKey + "::a", gson.toJson(pathInfo.aAttr));
            connection.set(metadataKey + "::w", gson.toJson(pathInfo.wAttr));
        }
    }

    private RedisConnection<String, String> getConnection(String path) {
        String server = SConHashClient.getRealServer(path);
        return clients.get(server).connection;
    }

    // checkpointRoot/state/3/199
    private String getParentPath(String path) {
        if (!path.contains("/")) {
            return null;
        }
        String parentPath = path.substring(0,path.lastIndexOf('/'));
        return parentPath;
    }

    public FileStatus getFileStatus(String path) throws FileNotFoundException {
        SffsFileSystem.LOG.error("getFileStatus()方法执行 path="+path);
        path = trimPath(path);
        String fileUuid = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(path));
        String metadatKey = path + fileUuid +
                SSDBUnderFileSystem.METADATA;
        RedisConnection<String, String> connection = getConnection(path);

        if (!connection.exists(metadatKey + "::a")) {
            throw new FileNotFoundException();
        }
        String AAttrStr = connection.get(metadatKey + "::a" );
        String CAttrStr = connection.get(metadatKey + "::c" );
        String WAttrStr = connection.get(metadatKey + "::w" );
        PathInfo pathInfo = new PathInfo(path, false);
        pathInfo.aAttr = gson.fromJson(AAttrStr, AAttribute.class);
        pathInfo.cAttr = gson.fromJson(CAttrStr, CAttribute.class);
        pathInfo.wAttr = gson.fromJson(WAttrStr, WAttribute.class);
        return new FileStatus(pathInfo.wAttr.getSize(), false, 0,1024 ,
                pathInfo.wAttr.getMtime(), pathInfo.aAttr.getAtime(),
                new FsPermission(pathInfo.cAttr.getMode()), pathInfo.cAttr.getOwner(),
                pathInfo.cAttr.getGroup(), new Path(pathInfo.getPathName()));
    }
    // /root/3/e.txt -> root/3/e.txt
    private String trimPath(String path) {
        return path.substring(1);
    }
    

}
