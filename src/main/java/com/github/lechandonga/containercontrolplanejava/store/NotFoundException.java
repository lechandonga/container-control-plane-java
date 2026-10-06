package com.github.lechandonga.containercontrolplanejava.store;

public class NotFoundException extends RuntimeException {
    public NotFoundException(String uid) {
        super("资源不存在或已被删除: " + uid);
    }

    public NotFoundException(String message, boolean custom) {
        super(message);
    }
}
