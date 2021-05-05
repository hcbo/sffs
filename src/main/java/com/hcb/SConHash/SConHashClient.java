package com.hcb.SConHash;



import com.google.gson.Gson;
import com.hcb.server.SConHashServer;
import com.lambdaworks.redis.RedisConnection;

import org.apache.commons.lang3.StringUtils;

import java.util.*;

public class SConHashClient {
    private static Gson gson = new Gson();

    public static SortedMap<Integer, String> virtualNodes = new TreeMap<>();

    public static String[] realNodes;

    public static int opVirtualNum;


    // 虚拟节点，key表示虚拟节点的hash值，value表示虚拟节点的名称

    public static void refreshSconNodes(RedisConnection<String, String> connection){

        opVirtualNum = Integer.parseInt(connection.get("opVirtualNum" + SConHashServer.CLUSTERINFO));

        realNodes = gson.fromJson(connection.get("realNodes" + SConHashServer.CLUSTERINFO), String[].class);

        Map<String, String>  ssdbVnMap = connection.hgetall(SConHashServer.VNMAPKEY +
                SConHashServer.CLUSTERINFO);

        for(Map.Entry<String, String> entry : ssdbVnMap.entrySet()){
            virtualNodes.put(Integer.parseInt(entry.getKey()),entry.getValue());
        }

    }

    // 得到应当路由到的结点
    public static String getRealServer(String key) {
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


}