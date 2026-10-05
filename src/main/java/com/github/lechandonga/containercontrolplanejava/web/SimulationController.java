package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.core.ControlPlane;
import com.github.lechandonga.containercontrolplanejava.sim.NodeSimulator;
import java.util.Map;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 模拟控制：推进虚拟时钟、注入/清除“某节点上某镜像启动失败”、测试复位。
 */
@RestController
@RequestMapping("/api/sim")
public class SimulationController {
    private final ControlPlane controlPlane;
    private final NodeSimulator simulator;

    public SimulationController(ControlPlane controlPlane, NodeSimulator simulator) {
        this.controlPlane = controlPlane;
        this.simulator = simulator;
    }

    public record TickRequest(Long steps) {
    }

    @PostMapping("/tick")
    public Map<String, Object> tick(@RequestBody(required = false) TickRequest req) {
        long steps = req == null || req.steps() == null ? 1 : req.steps();
        return Map.of("tick", controlPlane.tick(steps));
    }

    @PostMapping("/failures")
    public Map<String, Object> inject(@RequestParam String nodeId,
                                      @RequestParam String image,
                                      @RequestParam String version) {
        simulator.injectImageFailure(nodeId, image, version);
        controlPlane.reconcile();
        return Map.of("injected", Map.of("nodeId", nodeId, "image", image, "version", version));
    }

    @DeleteMapping("/failures")
    public Map<String, Object> clear(@RequestParam String nodeId,
                                     @RequestParam String image,
                                     @RequestParam String version) {
        simulator.clearImageFailure(nodeId, image, version);
        controlPlane.reconcile();
        return Map.of("cleared", Map.of("nodeId", nodeId, "image", image, "version", version));
    }

    @PostMapping("/reset")
    public Map<String, Object> reset() {
        controlPlane.reset();
        return Map.of("reset", true);
    }
}
