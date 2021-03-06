package com.dfs.namenode.server;

import java.util.LinkedList;
import java.util.List;

/**
 * 负责管理内存中的文件目录树的核心组件
 */
public class FSDirectory {

    /**
     * 内存中的文件目录树
     */
    private INodeDirectory dirTree;

    public FSDirectory() {
        this.dirTree = new INodeDirectory("/");
    }

    /**
     * 创建目录
     * @param path  目录路径
     */
    public void mkdir(String path){
        // path = /usr/warehouse/hive

        synchronized (dirTree) {
            String[] pathes = path.split("/");
            INodeDirectory parent = null;
            for (String splitedPath : pathes) {
                if (splitedPath.trim().equals("")) {
                    continue;
                }

                INodeDirectory dir = findDirectory(parent == null ? dirTree : parent,
                                                    splitedPath);
                if (dir != null) {
                    parent = dir;
                    continue;
                }
                INodeDirectory child = new INodeDirectory(splitedPath);
                parent.addChild(child);
            }
        }

    }

    /**
     * 对文件目录树递归查找目录
     * @param dir
     * @param path
     * @return
     */
    private INodeDirectory findDirectory(INodeDirectory dir, String path){
        if (dir.getChildren().size() == 0) {
            return null;
        }
        INodeDirectory resultDir = null;
        for(INode child : dir.getChildren()) {
            if (child instanceof INodeDirectory) {
                INodeDirectory childDir = (INodeDirectory) child;
                if (childDir.getPath().equals(path)) {
                    return childDir;
                }
                resultDir = findDirectory(childDir,path);
                if (resultDir != null) {
                    return resultDir;
                }
            }
        }
        return null;
    }

    /**
     * 代表的是文件目录树中的一个节点
     */
    private interface INode{

    }

    /**
     * 代表文件目录树中的一个目录
     */
    public static class INodeDirectory implements INode{
        private String path;
        private List<INode> children;

        public INodeDirectory(String path) {
            this.path = path;
            this.children = new LinkedList<INode>();
        }

        public void addChild(INode inode){
            this.children.add(inode);
        }

        public String getPath() {
            return path;
        }
        public void setPath(String path) {
            this.path = path;
        }
        public List<INode> getChildren() {
            return children;
        }
        public void setChildren(List<INode> children) {
            this.children = children;
        }
    }

    /**
     * 代表文件目录树中的一个文件
     */
    public static class INodeFile implements INode{
        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
