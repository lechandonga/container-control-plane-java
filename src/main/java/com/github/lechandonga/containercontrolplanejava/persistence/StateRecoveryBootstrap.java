package com.github.lechandonga.containercontrolplanejava.persistence;

import com.github.lechandonga.containercontrolplanejava.reconcile.Reconciler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * 进程启动顺序：Spring 容器就绪后（控制循环已创建但测试配置下不会自动跑），
 * 先从本地状态文件恢复权威状态，再从恢复出的期望状态继续调谐，
 * 从中断处接着朝目标收敛——与全程不中断跑完的结果一致。
 */
@Component
public class StateRecoveryBootstrap {

    private static final Logger log = LoggerFactory.getLogger(StateRecoveryBootstrap.class);

    private final LocalStateStore stateStore;
    private final Reconciler reconciler;

    public StateRecoveryBootstrap(LocalStateStore stateStore, Reconciler reconciler) {
        this.stateStore = stateStore;
        this.reconciler = reconciler;
    }

    @Order(0)
    @EventListener(ApplicationReadyEvent.class)
    public void recover() {
        stateStore.loadOnStartup();
        log.info("[PERSISTENCE] 状态恢复完成，从期望状态继续全量调谐");
        reconciler.reconcileAll();
    }
}
