package com.github.lechandonga.containercontrolplanejava.revision;

/**
 * 修订是由什么操作产生的。回滚本身也是一条新修订，不覆盖历史。
 */
public enum RevisionTrigger {
    /** 首次提交创建工作负载。 */
    CREATE,
    /** 普通的期望状态更新（改副本数和/或版本）。 */
    UPDATE,
    /** 用户显式按某个历史修订（或上一个修订）发起的回滚。 */
    ROLLBACK
}
