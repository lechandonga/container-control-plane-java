package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 修订的来源类型。
 */
public enum RevisionKind {
    /** 首次提交期望状态。 */
    CREATE,
    /** 普通的期望状态变更提交（扩缩容/版本更新）。 */
    UPDATE,
    /** 显式按修订号回滚（包含“退回上一个修订”）。 */
    ROLLBACK
}
