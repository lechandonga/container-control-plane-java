package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.api.ControlPlaneService;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.web.dto.EventView;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 演示/测试用运维接口：手动推进控制循环、模拟控制面重启、按业务域查询事件。
 */
@RestController
@RequestMapping("/api/control-plane")
public class AdminController {

    private final ControlPlaneService service;
    private final EventLog eventLog;

    public AdminController(ControlPlaneService service, EventLog eventLog) {
        this.service = service;
        this.eventLog = eventLog;
    }

    @PostMapping("/tick")
    public Map<String, String> tick(@RequestParam(defaultValue = "1") int times) {
        for (int i = 0; i < times; i++) {
            service.drive();
        }
        return Map.of("result", "TICKED", "times", String.valueOf(times));
    }

    @PostMapping("/restart")
    public Map<String, String> restart() {
        service.restartControlPlane();
        return Map.of("result", "RECONCILED_FROM_STORE");
    }

    /** 演示/测试用：模拟真实进程重启——内存状态全丢，仅从本地磁盘文件恢复并继续收敛。 */
    @PostMapping("/restart-process")
    public Map<String, String> restartProcess() {
        service.restartProcessFromDisk();
        return Map.of("result", "RESTORED_FROM_LOCAL_DISK_AND_RECONCILED");
    }

    @GetMapping("/events")
    public List<EventView> events(@RequestParam(required = false) String business) {
        return eventLog.all().stream()
                .filter(e -> business == null || business.equals(e.business()))
                .map(EventView::from)
                .toList();
    }
}
