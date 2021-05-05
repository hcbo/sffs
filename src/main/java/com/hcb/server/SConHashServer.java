package com.hcb.server;

import com.google.gson.Gson;
import com.hcb.SConHash.SConUtils;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.RedisURI;
import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class SConHashServer {

    public static final String VNMAPKEY = "vnMap";

    public static final String CLUSTERINFO = "::clusterInfo";

    private static Gson gson = new Gson();
    // 待添加入Hash环的服务器列表
    private static String[] servers = { "192.168.225.6:6380", "192.168.225.7:6380"};

    // 真实结点列表,考虑到服务器上线、下线的场景，即添加、删除的场景会比较频繁，这里使用LinkedList会更好
    private static List<String> realNodes = new LinkedList<String>();

    // 虚拟节点，key表示虚拟节点的hash值，value表示虚拟节点的名称
    private static SortedMap<Integer, String> virtualNodes = new TreeMap<Integer, String>();

    static {
        // 先把原始的服务器添加到真实结点列表中
        for (int i = 0; i < servers.length; i++) {
            realNodes.add(servers[i]);
        }
    }

    // 得到应当路由到的结点
    private static String getServer(String key) {
        // 得到该key的hash值
        int hash = SConUtils.GetHash(key);
        // 得到大于该Hash值的所有Map
        SortedMap<Integer, String> subMap = virtualNodes.tailMap(hash);
        String virtualNode;
        if (subMap.isEmpty()) {
            // 如果没有比该key的hash值大的，则从第一个node开始
            Integer i = virtualNodes.firstKey();
            // 返回对应的服务器
            virtualNode = virtualNodes.get(i);
        } else {
            // 第一个Key就是顺时针过去离node最近的那个结点
            Integer i = subMap.firstKey();
            // 返回对应的服务器
            virtualNode = subMap.get(i);
        }
        // virtualNode虚拟节点名称要截取一下
        if (StringUtils.isNotBlank(virtualNode)) {
            return virtualNode.substring(0, virtualNode.indexOf("&&"));
        }
        return null;
    }

    public static void main(String[] args) {

        // 对100万个key进行hash处理，然后分别放入10个节点，根据每个节点的key数量来看key的分布情况
        int keyNum = 1000; // 100万个kv

        // 实际服务器的数量
        int serversLength = servers.length;
        double dAve = keyNum / serversLength;

        //待计算数组
        double[] _serviceKeysNum ;

        int opVirtualNum = 0;
        double minOt = 0;
        // 虚拟节点模拟
        for(int virtualNum=50; virtualNum<=3000; virtualNum=virtualNum+10) {
            //重置
            _serviceKeysNum = new double[serversLength];
            virtualNodes = new TreeMap<Integer, String>();

            // 再添加虚拟节点，遍历LinkedList使用foreach循环效率会比较高
            for (String str : realNodes) {
                for (int i = 0; i < virtualNum; i++) {
                    String virtualNodeName = str + "&&VN" + String.valueOf(i);
                    int hash = SConUtils.GetHash(virtualNodeName);
                    // System.out.println("虚拟节点[" + virtualNodeName + "]被添加, hash值为" + hash);
                    virtualNodes.put(hash, virtualNodeName);
                }
            }

            for (int i = 0; i < keyNum; i++) {
                String _name = String.valueOf(i) + "key";
                String serverIdent = getServer(_name);
                int index = Arrays.binarySearch(servers, serverIdent);
                _serviceKeysNum[index]++;
            }

            System.out.println(String.valueOf(serversLength)+"个节点数据分布个数："+Arrays.toString(_serviceKeysNum));
            System.out.println("平均数=" + String.valueOf(dAve));

            double var = SConUtils.StandardDiviation(_serviceKeysNum, dAve);
            double ot = var * virtualNum;
            if (minOt == 0 || ot < minOt) {
                minOt = ot;
                opVirtualNum = virtualNum;
            }
            System.out.println(String.valueOf(serversLength) + "固定节点，每个固定节点的个虚拟节点数儿="+virtualNum+"，标准差=" + String.valueOf(ot));
            System.out.println();
        }


        virtualNodes.clear();

        String materServer = servers[0];
        String ip = materServer.split(":")[0];
        int port = Integer.parseInt(materServer.split(":")[1]);

        RedisClient redisClient = new RedisClient(
                RedisURI.create("redis://@"+ ip + ":" + port));

        RedisConnection<String, String> connection = redisClient.connect();

        connection.set("realNodes" + CLUSTERINFO, gson.toJson(servers));
        connection.set("opVirtualNum" + CLUSTERINFO, String.valueOf(opVirtualNum));



        for (String str : realNodes) {
            for (int i = 0; i < opVirtualNum; i++) {
                String virtualNodeName = str + "&&VN" + String.valueOf(i);
                int hash = SConUtils.GetHash(virtualNodeName);
                virtualNodes.put(hash, virtualNodeName);
                connection.hset(VNMAPKEY + CLUSTERINFO, String.valueOf(hash), virtualNodeName);
            }
        }
        connection.close();
        redisClient.shutdown();


        System.out.println(String.valueOf(serversLength) + "个固定节点，每个固定节点的虚拟节点数儿="+String.valueOf(opVirtualNum)+"，最小标准差=" + String.valueOf(minOt));
    }
}