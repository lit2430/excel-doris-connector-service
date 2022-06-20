package com.bigdata.exception;


import com.bigdata.constants.RespContent;

/**
 * @author lit
 * @email
 * @date 2021/6/2 17:02
 * @description: 导入doris异常
 **/
public class DorisImportException extends RuntimeException {

    private RespContent respContent;

    public DorisImportException(String message, RespContent respContent) {
        super(message);
        this.respContent = respContent;
    }

    public RespContent getRespContent() {
        return respContent;
    }
}
