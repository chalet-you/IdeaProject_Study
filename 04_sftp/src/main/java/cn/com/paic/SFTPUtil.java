package cn.com.paic;

import com.jcraft.jsch.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * sftp 工具 用来传输文件
 * 目的：不用先落地磁盘，直接在程序运行时将文件写过去
 */
public class SFTPUtil {

    private transient Logger log = LoggerFactory.getLogger(this.getClass());

    private ChannelSftp sftp;
    private Session session;
    private String username;
    protected String password;
    private String host;

    /**
     * 用来免密
     *
     * @param username
     * @param host
     */
    public SFTPUtil(String username,String host){
        this.username = username;
        this.host = host;
    }

    public SFTPUtil(String username, String password, String host){
        this.username = username;
        this.password = password;
        this.host = host;
    }

    /**
     * 开启连接
     *
     * @return
     */
    public boolean getSession(){
        JSch jSch = new JSch();
//        String user = System.getProperty("user", "home");
//        jSch.setKnownHosts();

        try{
            if (password == null){ // 如果没有密码，也就是免密
                File file = new File(System.getProperty("user.home") + "/.ssh/id_rsa");
                jSch.addIdentity(file.getPath());
            }
            session = jSch.getSession(username, host);
            session.setConfig("StrictHostKeyChecking","no");
            // 如果有密码
            if (password != null){
                session.setConfig("PreferredAuthentications","password"); // 优先使用密码
                session.setPassword(password);
            }
            session.connect(); // 建立 session 连接
            log.info("Session is connected");
            Channel channel = session.openChannel("sftp");
            channel.connect();
            log.info("channel is connected");
            sftp = (ChannelSftp) channel;
            log.info(String.format("sftp server host:[%s] is connect successfull",host));
            return true;
        }catch (JSchException e){
            e.printStackTrace();
            log.error("Cannot connect to specified sftp server : {}\n Exception message is: {}",new Object[]{host,e.getMessage()});
        }
        return false;
    }

    /**
     * 关闭连接
     */
    public void close(){
        if (sftp != null){
            if (sftp.isConnected()){
                sftp.disconnect();
                log.info("sftp is closed already");
            }
        }
        if (session !=null){
            if (session.isConnected()){
                session.disconnect();
                log.info("sshSession is closed already");
            }
        }
    }

    /**
     * 传输文件
     *
     * @param dir
     * @param filename
     * @param content
     */
    public void sendFile(String dir, String filename, String content){
        try {
            sftp.cd(dir);
        }catch (Exception e){
            // 目录不存在，则创建文件夹
            String[] dirs = dir.split("/");
            String tempPath = "";
            int index = 0;
            mkdirDir(dirs, tempPath, dirs.length, index);
        }

        try {
            OutputStream out = sftp.put(dir + "/" + filename, ChannelSftp.OVERWRITE);
            OutputStreamWriter ow = new OutputStreamWriter(out);
            BufferedWriter bw = new BufferedWriter(ow);
            bw.write(content);
            bw.close();
            ow.close();
            out.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    /**
     * 递归根据路径创建文件夹
     * @param dirs        根据 "/" 分隔后的数组文件夹名称
     * @param tempPath    拼接路径
     * @param length      文件夹的格式
     * @param index       数组下标
     */
    private void mkdirDir(String[] dirs, String tempPath, int length, int index) {
        // 以 "/a/b/c/d" 为例按 "/" 分割后，第 0 位是 ""; 故下标从1开始
        index ++;
        if (index < length){
            //目录不存在，则创建文件夹
            tempPath += "/" + dirs[index];
        }
        try {
//            log.info("检查目录[" + tempPath +"]");
            sftp.cd(tempPath);
            if (index < length){
                mkdirDir(dirs,tempPath,length,index);
            }
        }catch (Exception ex){
//            log.warn("创建目录[" + tempPath +"]");
            try {
                sftp.mkdir(tempPath);
                sftp.cd(tempPath);
            }catch (Exception e){
//              e.printStackTrace();
                System.out.println("error: happens on tempPath " + tempPath +"; error msg:"+e);
                log.error("error happens on tempPath dir:"+ tempPath);
//                log.error("创建目录[" + tempPath + "]失败,异常信息[" +e.getMessage() +"]");
            }
//            log.info("进入目录[" + tempPath + "]");
            if (index <length){
                mkdirDir(dirs,tempPath,length,index);
            }
        }
    }

}
