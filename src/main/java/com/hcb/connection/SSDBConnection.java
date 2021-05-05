package com.hcb.connection;

import com.hcb.StringByteCodec;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;

public class SSDBConnection {

    private String ip;
    private String port;

    public RedisClient redisClient;
    public RedisConnection<String, String> connection;
    public RedisConnection<String, byte[]> dataConnection;

    public SSDBConnection(String ip, String port) {
        this.ip = ip;
        this.port = port;
        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://@"+ ip + ":" + port));
        this.connection = redisClient.connect();
        this.dataConnection = redisClient.connect(new StringByteCodec());

    }

    public SSDBConnection(RedisClient redisClient,
                          RedisConnection<String, String> connection,
                          RedisConnection<String, byte[]> dataConnection) {
        this.redisClient = redisClient;
        this.connection = connection;
        this.dataConnection = dataConnection;
    }
}
