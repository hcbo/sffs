package com.hcb;

import static org.junit.Assert.assertTrue;

import com.hcb.SConHash.SConHashClient;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void sConHashTest(){
        RedisConnection connection = createConnection();
        SConHashClient.refreshSconNodes(connection);
        System.out.println(SConHashClient.virtualNodes);

    }

    private RedisConnection createConnection() {
        String materServer = "192.168.225.6:6380";
        String ip = materServer.split(":")[0];
        int port = Integer.parseInt(materServer.split(":")[1]);

        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://@"+ ip + ":" + port));

        RedisConnection<String, String> connection = redisClient.connect();
        return connection;
    }
    private String trimPath(String path) {
        int start = "sffs://192.168.225.6:8888".length() + 1;
        return path.substring(start);
    }
    @Test
    public void testTrimPath() {
        String res = trimPath("sffs://192.168.225.6:8888/root/3/e.txt");
        System.out.println(res);
    }


}
