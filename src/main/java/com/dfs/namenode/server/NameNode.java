package com.dfs.namenode.server;

/**
 * NameNode核心启动类
 */
public class NameNode {

    // NameNode是否在运行
    private volatile Boolean shouldRun;

    // 负责管理元数据的核心组件
    private FSNamesystem namesystem;

    // NameNode对外提供rpc接口的server，可以响应请求
    private NameNodeRpcServer rpcServer;

    public NameNode() {
        this.shouldRun = true;
    }

    /**
     * 初始化NameNode
     */
    private void initialize(){
        this.namesystem = new FSNamesystem();
        this.rpcServer = new NameNodeRpcServer(this.namesystem);
        this.rpcServer.start();
    }

    private void run(){
        try {
            while (shouldRun) {
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        NameNode nameNode = new NameNode();
        nameNode.initialize();
        nameNode.run();
    }
}
