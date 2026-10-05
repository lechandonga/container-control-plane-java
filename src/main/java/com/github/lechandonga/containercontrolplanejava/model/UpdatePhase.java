package com.github.lechandonga.containercontrolplanejava.model;

/**
 * 一次期望版本变更（spec.generation 递增）所对应的滚动更新阶段。
 */
public enum UpdatePhase {
    /** 无进行中的更新。 */
    IDLE,
    /** 新版本正在按 surge / 可用性下限逐步替换旧版本。 */
    ROLLING,
    /** 新版本全部就绪，旧版本已彻底清理。 */
    SUCCEEDED,
    /** 更新失败，已回收全部新版本实例并回退到旧版本。 */
    FAILED
}
