package com.github.lechandonga.containercontrolplanejava.web;

import com.github.lechandonga.containercontrolplanejava.api.ControlPlaneService;
import com.github.lechandonga.containercontrolplanejava.model.Workload;
import com.github.lechandonga.containercontrolplanejava.model.WorkloadSpec;
import com.github.lechandonga.containercontrolplanejava.store.NotFoundException;
import com.github.lechandonga.containercontrolplanejava.web.dto.EventView;
import com.github.lechandonga.containercontrolplanejava.web.dto.InstanceView;
import com.github.lechandonga.containercontrolplanejava.web.dto.WorkloadRequest;
import com.github.lechandonga.containercontrolplanejava.web.dto.WorkloadView;
import com.github.lechandonga.containercontrolplanejava.event.EventLog;
import com.github.lechandonga.containercontrolplanejava.store.WorkloadStore;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/workloads")
public class WorkloadController {

    private final ControlPlaneService service;
    private final WorkloadStore store;
    private final EventLog eventLog;

    public WorkloadController(ControlPlaneService service, WorkloadStore store, EventLog eventLog) {
        this.service = service;
        this.store = store;
        this.eventLog = eventLog;
    }

    @PostMapping
    public WorkloadView create(@RequestBody WorkloadRequest request) {
        if (request.uid() == null || request.uid().isBlank()
                || request.replicas() == null || request.image() == null) {
            throw new IllegalArgumentException("uid / replicas / image 为必填项");
        }
        WorkloadSpec spec = new WorkloadSpec(
                request.replicas(), request.image(),
                request.maxSurge() != null ? request.maxSurge() : 1,
                request.minAvailable() != null ? request.minAvailable() : request.replicas(),
                request.maxStartRetries() != null ? request.maxStartRetries() : 5,
                request.rolloutDeadlineSeconds() != null ? request.rolloutDeadlineSeconds() : 120);
        return WorkloadView.from(service.createWorkload(request.uid(), spec));
    }

    @PutMapping("/{uid}")
    public WorkloadView update(@PathVariable String uid, @RequestBody WorkloadRequest request) {
        Workload current = store.get(uid);
        WorkloadSpec spec = request.toSpec(current.getSpec());
        return WorkloadView.from(service.submit(uid, spec, request.resourceVersion()));
    }

    @GetMapping
    public List<WorkloadView> list() {
        return service.list().stream().map(WorkloadView::from).toList();
    }

    @GetMapping("/{uid}")
    public WorkloadView get(@PathVariable String uid) {
        return WorkloadView.from(service.get(uid));
    }

    @DeleteMapping("/{uid}")
    public void delete(@PathVariable String uid,
                       @org.springframework.web.bind.annotation.RequestParam(required = false)
                       String resourceVersion) {
        service.delete(uid, resourceVersion);
    }

    @GetMapping("/{uid}/instances")
    public List<InstanceView> instances(@PathVariable String uid) {
        if (store.find(uid).isEmpty()) {
            throw new NotFoundException(uid);
        }
        return store.instancesOf(uid).stream().map(InstanceView::from).toList();
    }

    @GetMapping("/{uid}/events")
    public List<EventView> events(@PathVariable String uid) {
        return eventLog.byWorkload(uid).stream().map(EventView::from).toList();
    }
}
