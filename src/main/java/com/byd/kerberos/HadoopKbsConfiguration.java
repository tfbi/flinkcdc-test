package com.byd.kerberos;


import com.byd.utils.PathTools;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author wang.yonggui1
 * @date 2021-11-10 9:36
 */
public class HadoopKbsConfiguration extends Configuration{

    private static Logger log = LoggerFactory.getLogger(KerberosAuth.class);

    public static String HADOOP_SECURITY_AUTH = "hadoop.security.authentication";
    public static final String KERBEROS="Kerberos";

    private static HadoopKbsConfiguration INSTANCE;

    private HadoopKbsConfiguration(){}

    public static HadoopKbsConfiguration getHadoopKbsConfiguration(){
        if (null==INSTANCE){
            synchronized (HadoopKbsConfiguration.class){
                if (null==INSTANCE){
                    INSTANCE=new HadoopKbsConfiguration();
                    INSTANCE.set(HADOOP_SECURITY_AUTH, KERBEROS);
                    INSTANCE.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
                    INSTANCE.setBoolean("dfs.support.append",true);
                    String confDir = CommonConfig.HADOOP_CONF_DIR.getValue();
                    confDir = PathTools.getPath(confDir);
                    log.info("hadoop conf dir:"+confDir);
                    INSTANCE.addResource(confDir+"/core-site.xml");
                    INSTANCE.addResource(confDir+"/hdfs-site.xml");
                    INSTANCE.addResource(confDir+"/hive-site.xml");
                }
            }
        }
        return INSTANCE;
    }


}
