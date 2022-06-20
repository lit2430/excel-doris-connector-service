package com.bigdata.exception;

/**
 * Created by:
 *
 * @Author: lit
 * @Date: 2022/06/17/14:51
 * @Description:
 */
public class ReadPropertiesException extends RuntimeException {

    private String lable;


    public ReadPropertiesException(String message, String lable) {
        super(message);
        this.lable = lable;

    }

    public String getLable() {
        return lable;
    }
}
