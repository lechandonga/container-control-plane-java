package com.github.lechandonga.containercontrolplanejava.revision;

/**
 * 一条修订记录的最终结果。修订一旦进入终态即为不可变事实。
 */
public enum RevisionStatus {
    /** 修订已提交，对应的期望变更仍在收敛中（滚动更新/扩缩容/回滚进行中）。 */
    IN_PROGRESS,
    /** 变更成功收敛：实际副本数与版本精确等于该修订声明。 */
    SUCCEEDED,
    /** 变更明确告停：容量/健康节点不足、目标版本起不来等，条件中保留原因。 */
    FAILED,
    /** 该修订声明的版本被后续失败更新自动回退，或被一次成功的“按修订回滚”取代。 */
    ROLLED_BACK
}
