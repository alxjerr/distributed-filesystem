package com.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * 负责管理edits log 日志的核心组件
 */
public class FSEditlog {

    /**
     * 当前递增到的txid的序号
     */
    private long txidSeq = 0L;

    // 内存双缓冲区
    private DoubleBuffer editLogBuffer = new DoubleBuffer();

    // 当前是否在将内存缓冲刷入磁盘中
    private volatile Boolean isSyncRunning = false;

    // 当前是否有线程在等待刷新下一批edits log到磁盘里去
    private volatile Boolean isWaitSync = false;

    // 在同步到磁盘中的最大的一个txid
    private volatile Long syncMaxTxid = 0L;

    // 每个线程自己本地的txid副本
    private ThreadLocal<Long> localTxid = new ThreadLocal<Long>();

    /**
     * 记录edits log日志
     * @param content
     */
    public void logEdit(String content){
        // 这里必须得直接加锁
        synchronized (this) {
            txidSeq++;
            long txid = txidSeq;
            localTxid.set(txid);

            // 构造一条Edits log对象
            EditLog log = new EditLog(txid,content);
            // 将edits log写入内存缓冲中，不是直接刷入磁盘文件
            editLogBuffer.write(log);
        }
    }

    /**
     * 将内存缓冲中的数据刷入磁盘文件中
     */
    private void logSync(){
        // 再次尝试加锁
        synchronized (this) {
            // 如果说当前整好有人在刷内存缓冲到磁盘中去
            if (isSyncRunning) {
                long txid = localTxid.get();
                if (txid < syncMaxTxid) {
                    return;
                }

                if (isWaitSync) {
                    return;
                }
                isWaitSync = true;
                while (isSyncRunning) {
                    try {
                        wait(2000);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                isWaitSync = false;
            }

            // 交换两块缓冲区
            editLogBuffer.setReadyToSync();
            syncMaxTxid = editLogBuffer.getSyncMaxTxid();
            // 设置当前正在同步到磁盘的标志位
            isSyncRunning = true;
        }

        editLogBuffer.flush();

        synchronized (this) {
            isSyncRunning = false;
            notifyAll();
        }
    }

    /**
     * 代表了一条edits log
     */
    class EditLog{
        Long txid;
        String content;

        public EditLog(Long txid, String content) {
            this.txid = txid;
            this.content = content;
        }
    }

    /**
     * 内存双缓冲
     */
    class DoubleBuffer{
        /**
         * 是专门用来承载线程写入edits log
         */
        LinkedList<EditLog> currentBuffer = new LinkedList<EditLog>();

        /**
         * 专门用来将数据同步到磁盘中去的一块缓冲
         */
        LinkedList<EditLog> syncBuffer = new LinkedList<EditLog>();

        /**
         * 将edits log写到内存缓冲里去
         * @param log
         */
        public void write(EditLog log){
            currentBuffer.add(log);
        }

        /**
         * 交换两块缓冲区，为了同步内存数据到磁盘做准备
         */
        public void setReadyToSync(){
            LinkedList<EditLog> tmp = currentBuffer;
            currentBuffer = syncBuffer;
            syncBuffer = tmp;
        }

        public Long getSyncMaxTxid(){
            return syncBuffer.getLast().txid;
        }

        /**
         * 将 syncBuffer 缓冲区中的数据刷入磁盘中
         */
        public void flush(){
            for (EditLog log : syncBuffer) {
                System.out.println("将edit log写入磁盘文件中：" + log);
                // 正常来说，就是用文件输出流将数据写入磁盘文件中
            }
            syncBuffer.clear();
        }

    }

}
