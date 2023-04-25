package com.byd.kerberos;

/**
 * @author wang.yonggui1
 * @date 2021-12-30 12:10
 */
public interface Config {

    /**
     * @return 名称
     */
    String getKey();

    /**
     * @return 值
     */
    String getValue();

    /**
     * @return 备注
     */
    String getDesc();

}
