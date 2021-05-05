package com.hcb;

import com.google.gson.Gson;
import com.hcb.SConHash.SConHashClient;
import com.hcb.SConHash.SConUtils;
import com.hcb.connection.SSDBConnection;
import com.hcb.structure.PathInfo;
import com.lambdaworks.redis.RedisConnection;
import java.io.OutputStream;
import java.util.Arrays;

public class RfsFileOutputStream extends OutputStream {

    private final int BYTE_BUFFER_SIZE;
    private byte[] byteBuffer;
    private int pointer;
    private PathInfo pathInfo;
    private static Gson gson = new Gson();
    private RedisConnection<String, String> connection;
    private RedisConnection<String, byte[]> dataConnection;
    private String UUID;




    public RfsFileOutputStream(String path, short mode) {
        SffsFileSystem.LOG.error("RfsFileOutputStream构造方法调用 " + path);
        this.BYTE_BUFFER_SIZE = SffsFileSystem.BUFFERSIZE;
        byteBuffer = new byte[BYTE_BUFFER_SIZE];
        pathInfo = new PathInfo(path, false);
        pathInfo.cAttr.setMode(mode);
        String realNode = SConHashClient.getRealServer(pathInfo.getPathName());
        SSDBConnection ssdbConnection = SSDBUnderFileSystem.clients.get(realNode);
        this.connection = ssdbConnection.connection;
        this.dataConnection = ssdbConnection.dataConnection;
        UUID = SSDBUnderFileSystem.SEPARATOR +
                String.valueOf(SConUtils.GetHash(pathInfo.getPathName()));
        SffsFileSystem.LOG.error("RfsFileOutputStream构造方法调用结束");
    }

    @Override
    public void write(int b) {
        byteBuffer[pointer] = (byte) b;
        pointer++;
    }

    @Override
    public void close() {
        SffsFileSystem.LOG.error("RfsFileOutputStream.close()调用:"+" pathInfo.name " + pathInfo.getPathName());

        String fileKey = pathInfo.getPathName() + UUID + SSDBUnderFileSystem.FILEDATA;
        if (connection.exists(fileKey)) {
            SffsFileSystem.LOG.error("RfsFileOutputStream.close()调用提前结束:"+" pathInfo.name " + fileKey + " pointer： " + pointer);
            return;
        }
        String parentPath = getParentPath(pathInfo.getPathName());
        String parUuid = SSDBUnderFileSystem.SEPARATOR +
                String.valueOf(SConUtils.GetHash(parentPath));
        String patIndexKey = parentPath + parUuid + SSDBUnderFileSystem.INDEX + "::0";
        RedisConnection<String, String> parConnection = getConnection(parentPath);
        // "1表示文件 0表示目录"
        parConnection.hset(patIndexKey, pathInfo.getPathName(), "1");
        //data
        dataConnection.set(fileKey, Arrays.copyOf(byteBuffer, pointer));
        //metadata
        String metadataKey = pathInfo.getPathName() + UUID + SSDBUnderFileSystem.METADATA;
        long timeStamp = System.currentTimeMillis();
        pathInfo.aAttr.setAtime(timeStamp);
        pathInfo.wAttr.setCtime(timeStamp);
        pathInfo.wAttr.setMtime(timeStamp);
        pathInfo.wAttr.setSize(pointer);
        connection.set(metadataKey + "::c", gson.toJson(pathInfo.cAttr));
        connection.set(metadataKey + "::a", gson.toJson(pathInfo.aAttr));
        connection.set(metadataKey + "::w", gson.toJson(pathInfo.wAttr));

        SffsFileSystem.LOG.error("RfsFileOutputStream.close()调用结束:"+" pathInfo.name " + fileKey + " pointer： " + pointer);
    }




    // checkpointRoot/state/3/199
    private String getParentPath(String path) {
        return path.substring(0,path.lastIndexOf("/"));
    }

    private RedisConnection<String, String> getConnection(String path) {
        String server = SConHashClient.getRealServer(path);
        return SSDBUnderFileSystem.clients.get(server).connection;
    }
}
