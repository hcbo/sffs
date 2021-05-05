package com.hcb;

import com.hcb.SConHash.SConHashClient;
import com.hcb.SConHash.SConUtils;
import com.hcb.connection.SSDBConnection;
import com.lambdaworks.redis.RedisConnection;
import org.apache.hadoop.fs.FSInputStream;

import java.io.IOException;

public class RfsFileInputStream extends FSInputStream {
    private byte[] byteBuffer ;
    private int pointer;
    private RedisConnection<String, byte[]> dataConnection;
    private String UUID;

    public RfsFileInputStream(String path) {
        SffsFileSystem.LOG.error("RfsFileInputStream.构造函数调用");
        String realNode = SConHashClient.getRealServer(path);
        SSDBConnection ssdbConnection = SSDBUnderFileSystem.clients.get(realNode);
        this.dataConnection = ssdbConnection.dataConnection;
        UUID = SSDBUnderFileSystem.SEPARATOR + String.valueOf(SConUtils.GetHash(path));
        String fileKey = path + UUID + SSDBUnderFileSystem.FILEDATA;
        byteBuffer = (byte[]) dataConnection.get(fileKey);
        SffsFileSystem.LOG.error("RfsFileInputStream.构造函数调用结束"+ " "+byteBuffer.length);
    }

    @Override
    public int read() throws IOException {
        if(pointer < byteBuffer.length){
            int res = (int)byteBuffer[pointer];
            pointer++;
            return res&(0xff);
        }
        return -1;
    }

    @Override
    public void seek(long pos) throws IOException {

    }

    @Override
    public long getPos() throws IOException {
        return 0;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        return false;
    }
}
