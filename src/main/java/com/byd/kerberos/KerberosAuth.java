package com.byd.kerberos;

import com.byd.task.Task;
import com.byd.task.TaskExecutors;
import com.byd.utils.DateUtils;
import com.byd.utils.PathTools;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * @author wang.yonggui1
 * @date 2021-11-10 12:03
 * @note
 */
public class KerberosAuth {

    private static Logger log = LoggerFactory.getLogger(KerberosAuth.class);

    private KerberosAuth(){

    }

    private static UserGroupInformation LOGIN;

    public static String SECURITY_KRB5_CONF = "java.security.krb5.conf";

    /**
     * 强制刷新登录
     */
    public static void forcedFlush(){
        try {
            LOGIN.forceReloginFromKeytab();
            log.info("forcedFlush kerberos:{} at:{}",LOGIN.getUserName(),
                    DateUtils.formatTimestampMs(System.currentTimeMillis()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查刷新登录
     */
    public static void checkAndFlush(){
        try {
            LOGIN.checkTGTAndReloginFromKeytab();
            log.info("checkAndFlush kerberos:{} at:{}",LOGIN.getUserName(),
                    DateUtils.formatTimestampMs(System.currentTimeMillis()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static UserGroupInformation login() {
        if (null==LOGIN){
            synchronized (KerberosAuth.class){
                if (null==LOGIN){
                    try {
                        LOGIN=doLogin();
                    } catch (Exception e) {
                        log.error("login error:",e);
                    }
                }
            }
        }

        return LOGIN;
    }

    private static UserGroupInformation doLogin() throws Exception {
        if (StringUtils.isEmpty(CommonConfig.KERBEROS_PRINCIPAL.getValue())){
            throw new RuntimeException("principal can not be null!!!");
        }

        String kerb5Path= PathTools.getPath(CommonConfig.KERBEROS_KRB5_CONF_PATH.getValue());
        String keytabPath= PathTools.getPath(CommonConfig.KERBEROS_KEYTAB_PATH.getValue());
        log.info("krb5 path:"+ kerb5Path);
        log.info("keytab path:"+keytabPath);

        System.setProperty(SECURITY_KRB5_CONF, kerb5Path);
        UserGroupInformation.setConfiguration(HadoopKbsConfiguration.getHadoopKbsConfiguration());

        UserGroupInformation.loginUserFromKeytab(
                CommonConfig.KERBEROS_PRINCIPAL.getValue(), keytabPath);

        UserGroupInformation logUser = UserGroupInformation.getLoginUser();
        if (null == logUser) {
            throw new Exception("login user can not be empty!");
        }
        log.info(UserGroupInformation.getCurrentUser()+" login success at:"
                +DateUtils.formatTimestampMs(System.currentTimeMillis()));
        return logUser;
    }

    public static Object dealWithKbs(Task task){
        Object dealRes = null;
        dealRes=login()
                .doAs((PrivilegedAction<Object>) () -> task.execute());
        return dealRes;
    }

    /**
     * start thread to flush kerberos
     */
    public static void startFlushKeberos(){
        Integer valueInt = CommonConfig.KERBEROS_FLUSH_INTERVAL.getValueInt();
        login();

        if (valueInt>60*60*1000){
            TaskExecutors.addTask(()->{
                while (true){
                    try {
                        Thread.sleep(valueInt);
                    } catch (InterruptedException e) {
                        log.error("startFlushKeberos error:",e);
                    }
                    KerberosAuth.checkAndFlush();
                }
            });
            log.info("regist timer to flush kerberos login,interval:{} ms",valueInt);
        }else {
            log.error("flush kerberos interval must uppper 60min");
        }
    }



    public static void main (String[] args) throws IOException {

//        CommonConfig.reload("dms/prod/config-dms_dea.properties");
//        KerberosAuth.login();
        UserGroupInformation ugiFromTicketCache = UserGroupInformation
                .getUGIFromTicketCache(
                        "/home/BYDP0001/kerberos/mycache_1001",
                        "ic.bigdatamgr@BYD.COM");

        System.out.println(ugiFromTicketCache);

        UserGroupInformation.setLoginUser(ugiFromTicketCache);

        System.out.println(UserGroupInformation.getLoginUser());

    }
}
