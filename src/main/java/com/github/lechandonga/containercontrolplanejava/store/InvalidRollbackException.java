package com.github.lechandonga.containercontrolplanejava.store;

/**
 * 回滚请求本身不合法：没有任何历史、指定的修订号不存在、目标修订没有可恢复的声明等。
 */
public class InvalidRollbackException extends RuntimeException {
    public InvalidRollbackException(String message) {
        super(message);
    }
}
