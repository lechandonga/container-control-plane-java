package com.github.lechandonga.containercontrolplanejava.store;

/**
 * 并发修改冲突：资源版本不匹配（乐观锁），调用方必须重试或放弃，禁止静默覆盖。
 */
public class ConflictException extends RuntimeException {

    private final String workloadUid;
    private final String suppliedResourceVersion;
    private final String currentResourceVersion;

    public ConflictException(String workloadUid, String supplied, String current) {
        super("工作负载 " + workloadUid + " 发生并发修改冲突: 提交的 resourceVersion="
                + supplied + ", 当前 resourceVersion=" + current
                + "，本次操作已拒绝，请重新读取后基于最新版本重试");
        this.workloadUid = workloadUid;
        this.suppliedResourceVersion = supplied;
        this.currentResourceVersion = current;
    }

    public String getWorkloadUid() { return workloadUid; }
    public String getSuppliedResourceVersion() { return suppliedResourceVersion; }
    public String getCurrentResourceVersion() { return currentResourceVersion; }
}
