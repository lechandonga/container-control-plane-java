# 单机容器编排控制面（本地模拟节点）

一个不依赖任何外部服务（无数据库、无消息队列、无真实容器运行时）的 Spring Boot 工程，
在单机内模拟“控制面 + 节点 + 容器实例”，实现声明式期望状态、持续调谐（reconcile）、
节点故障自愈、版本滚动更新与并发一致性。时间由**虚拟时钟**驱动，默认通过
`POST /api/sim/tick` 显式推进，因此所有行为（启动、超时、故障）都是确定性、可复现的。

## 运行与验证

```bash
mvn test                 # 运行业务测试，控制台打印每个业务/用例的覆盖与通过情况
mvn spring-boot:run      # 启动服务（默认随机端口，可用 -Dserver.port=8080 固定）
mvn test -Dccp.auto-tick=true   # 仅演示用：打开自动调谐（每秒 1 tick）
```

状态持久化在 `ccp.store-file`（默认 `target/ccp-state.json`），采用“临时文件 + 原子 move”，
控制面在任意时刻崩溃重启后都从上一个完整快照恢复，不会出现半截写或重复实例。

## 快速体验（HTTP）

```bash
# 1) 注册两个容量为 5 的模拟节点
curl -XPOST localhost:8080/api/nodes -H 'Content-Type: application/json' \
  -d '{"id":"n1","capacity":5}'
curl -XPOST localhost:8080/api/nodes -H 'Content-Type: application/json' \
  -d '{"id":"n2","capacity":5}'

# 2) 声明工作负载：3 副本，滚动期间可用下限 2（PUT 是幂等的 create-or-update）
curl -XPUT localhost:8080/api/workloads/app -H 'Content-Type: application/json' \
  -d '{"image":"demo/img","version":"v1","replicas":3,"minAvailable":2}'

# 3) 推进虚拟时钟，观察收敛
curl -XPOST localhost:8080/api/sim/tick -H 'Content-Type: application/json' -d '{"steps":2}'
curl localhost:8080/api/workloads/app

# 4) 滚动更新到 v2
curl -XPUT localhost:8080/api/workloads/app -H 'Content-Type: application/json' \
  -d '{"image":"demo/img","version":"v2","replicas":3,"minAvailable":2}'
curl -XPOST localhost:8080/api/sim/tick -d '{"steps":5}'

# 5) 节点故障 / 注入坏版本 / 查看可解释事件
curl -XPOST localhost:8080/api/nodes/n2/lost
curl -XPOST 'localhost:8080/api/sim/failures?nodeId=n1&image=demo/img&version=v3'
curl 'localhost:8080/api/events?workloadId=app'
```

## 调谐语义（Reconciliation）

- **声明式、持续调谐**：控制面只认“期望状态”（image/version/replicas/minAvailable/超时）。
  每次提交、节点事件、时钟推进都会触发一轮全量调谐：先观察实际状态，再向期望状态收敛。
  调谐是“水平触发”而非“边沿触发”：重复提交、重复事件、重复 tick 都是无害的。
- **幂等保证**：
  - 相同期望状态重复提交被去重（事件 `SubmitDedup`），不推进 `resourceVersion`、不创建实例；
  - 实例 ID 为 `<workload>-<单调递增 seq>`，seq 持久化且删除后不复用，重复观察不会重建同一实例；
  - 每轮调谐都是“观察 → 计算差集 → 动作”，扩缩容/驱逐/创建只依据当前实际状态，
    因此重复执行结果相同，不会出现计数漂移或状态回退。
- **崩溃恢复**：每次状态变更后原子落盘；重启后加载快照、继续调谐。
  用例 1.3 验证重启后实例不重复、seq 延续、最终继续收敛。
- **可解释结果**：所有关键动作写入只追加事件流（`GET /api/events`），
  每个工作负载的 `phase` + `reason` 用中文说明当前状态与阻塞原因。

工作负载相位：`PENDING`（尚未就绪/容量等待）、`RUNNING`（已收敛）、
`UPDATING`（滚动更新中）、`UPDATE_FAILED`（更新失败已告停回滚）、`TERMINATING`（删除中）。

## 滚动更新策略

采用 **maxSurge = 0**（实例总数恒等于 `replicas`，绝不超卖）的确定性替换：

1. 优先使用集群**空闲槽位**建立 `minAvailable` 个新版本实例（旧版本保持不动）；
2. 没有空闲槽位时，仅当“终止一个旧实例后当前可用实例数仍 ≥ `minAvailable`”
   才终止旧实例腾出槽位，再创建新版本；
3. 之后逐轮“新就绪后杀旧”地替换，直到旧版本实例数为 0 且新版本全部就绪，提交版本进入 `RUNNING`；
4. 过渡期间允许短暂的两版本并存（这是滚动更新本身），**收敛后旧版本被彻底清理，绝不长期混跑**；
5. 失败与超时一律**明确告停**（`UPDATE_FAILED` + 原因），并**确定性回滚**：
   移除全部新版本实例、目标版本恢复为旧版本、按容量补齐旧副本；
   - 新版本实例 `FAILED`（坏版本/故障注入）→ 立即回滚；
   - 新版本 PENDING 超过 `startTimeoutTicks` → 回滚；
   - 整轮更新超过 `rolloutTimeoutTicks`（如无法在不跌破下限的前提下推进）→ 回滚；
6. 回滚后提交修复后的新版本即重新发起更新（事件 `RolloutRetried`，用例 3.4）。

用例 3.1 逐 tick 校验滚动期间最低可用数 ≥ minAvailable 且结束后无旧版本残留。

## 容量规则

- 节点有硬容量 `capacity`，按**集群所有工作负载**汇总占用；任何健康节点上实例数 ≤ 容量。
- 调度器在“健康 + 有空闲槽位”的节点中确定性选择（负载最低，再按节点 ID 字典序），
  放置结果可复现、可解释。
- 节点 `NOT_READY`（失联）后：其上实例立即视为丢失并回收（事件 `InstanceEvicted`），
  在剩余健康节点重建；若健康容量不足，保持副本缺口并把原因写入 `reason`，
  **绝不超卖**；节点恢复或新节点注册后自动补齐。
- 节点恢复时旧实例不会“复活”，只能经调谐重新分配（防止幽灵副本）。

## 并发与冲突规则

- 所有状态变更在控制面单把全局锁内完成并原子落盘，杜绝脑裂与丢失更新。
- 每个工作负载带单调递增的 `resourceVersion`（乐观锁）：
  - 提交时**携带** `resourceVersion`：与当前值不一致返回 `409 CONFLICT`
    （`reason=RESOURCE_VERSION_CONFLICT`），冲突操作被**明确拒绝**，不静默覆盖；
  - **不携带** `resourceVersion`：表示显式强制覆盖，按到达顺序串行生效（确定规则）；
  - 相同期望重复提交：幂等去重。
- **删除优先**：删除会立即清空实例并移除对象；并发的更新若基于旧状态则 409；
  对已删除对象携带 `resourceVersion` 的更新同样被拒绝，不会让对象“脑裂复活”。
- 用例 4.1 用 20 个线程基于同一版本并发写，验证恰好 1 个成功、19 个 409、版本只 +1、
  实际副本精确收敛到获胜写入。

## 测试覆盖（`mvn test`）

| 测试类 | 业务 | 用例 |
| --- | --- | --- |
| `ReconcileIdempotencyTest` | 调谐幂等与重启恢复 | 重复提交去重、重复 tick 无漂移、调谐中途重启恢复、扩缩容幂等 |
| `NodeFailureTest` | 节点故障自愈与容量 | 失联重建、容量不足不超卖、恢复节点不复活旧实例、多负载共享容量 |
| `RollingUpdateTest` | 滚动更新与失败回滚 | 可用下限+旧版本清理、坏版本告停回滚、总超时告停、失败后重试成功 |
| `ConcurrencyConflictTest` | 并发冲突与一致性 | 20 线程乐观锁冲突、并发改版本拒绝、更新中删除优先、强制覆盖串行收敛 |

测试日志在控制台以中文输出：`▶ 业务覆盖` 标明业务，`┌─ 用例`/`└─ 结果` 标明每个用例及
✅/❌ 结果，`■ 业务[...] 用例数/通过/失败` 给出汇总。
