# 单机容器编排控制面（Java / Spring Boot 模拟实现）

一套**不依赖任何外部服务**（无数据库、无消息队列、无真实容器运行时）即可单机运行的容器编排
控制面：运行期以内存中的权威存储驱动调谐，并把全部权威状态**原子落盘到本机状态文件**，
用本地模拟节点（`SimulatedNode`）承载工作负载。
控制面实现了声明式期望状态、持续调谐（reconcile）、节点故障自愈、版本滚动更新与乐观并发控制、
**发布修订历史与按修订回滚**，以及**基于本地磁盘原子落盘的进程重启 / 崩溃恢复**，
并保证在**任意提交顺序、重复事件、调谐过程中重启、写状态时被强杀、异常时序**下最终收敛且结果可解释。

## 快速开始

```bash
# 运行全部单元测试（日志带 [BUSINESS-TESTS] 前缀，可直接看出业务覆盖与用例结果）
mvn test

# 启动服务（默认 8080 端口）
mvn spring-boot:run
```

### 一分钟演示（REST）

```bash
# 1) 注册 3 个容量为 2 的模拟节点
for i in 1 2 3; do
  curl -s -X POST localhost:8080/api/nodes \
    -H 'Content-Type: application/json' \
    -d '{"host":"host-'$i'","capacity":2}'
done

# 2) 声明工作负载：4 个副本，版本 app:v1
curl -s -X POST localhost:8080/api/workloads \
  -H 'Content-Type: application/json' \
  -d '{"uid":"demo","replicas":4,"image":"app:v1","maxSurge":1,"minAvailable":3}'

# 3) 观察收敛
curl -s localhost:8080/api/workloads/demo
curl -s localhost:8080/api/workloads/demo/instances

# 4) 滚动更新到 v2（必须带返回的 resourceVersion，否则冲突拒绝）
RV=$(curl -s localhost:8080/api/workloads/demo | python3 -c 'import sys,json;print(json.load(sys.stdin)["resourceVersion"])')
curl -s -X PUT localhost:8080/api/workloads/demo \
  -H 'Content-Type: application/json' \
  -d '{"replicas":4,"image":"app:v2","maxSurge":1,"minAvailable":3,"resourceVersion":"'$RV'"}'

# 5) 节点故障注入与恢复
curl -s -X POST localhost:8080/api/nodes/node-1/partition
curl -s -X POST localhost:8080/api/nodes/node-1/recover

# 6) 按业务域查询可解释事件
curl -s 'localhost:8080/api/control-plane/events?business=NODE_FAILURE'
curl -s localhost:8080/api/workloads/demo/events

# 7) 查询发布历史（修订序号/时间/前后声明/结果与原因）
curl -s localhost:8080/api/workloads/demo/revisions
curl -s localhost:8080/api/workloads/demo/revisions/2

# 8) 按修订回滚（退回上一个修订可省略 targetRevision；同样必带 resourceVersion）
curl -s -X POST localhost:8080/api/workloads/demo/rollback \
  -H 'Content-Type: application/json' \
  -d '{"targetRevision":2,"resourceVersion":"'$RV'"}'
```

## 架构与模块

| 包 / 类 | 职责 |
| --- | --- |
| `model` | 领域模型：`Workload`（期望 spec + 实际 status）、`InstanceRecord`、节点/阶段枚举、`Condition` |
| `store.WorkloadStore` | 权威内存存储 + **乐观并发控制** + 每工作负载调谐互斥锁 |
| `node.SimulatedNode` / `NodeRegistry` | 模拟节点（幂等起停容器、分区/启动失败注入、心跳）与故障检测 |
| `scheduler.Scheduler` | 容量感知装箱调度（most-available，平局按 uid 字典序，确定性） |
| `reconcile.Reconciler` | 核心调谐循环体：fence、自愈、扩缩容、滚动更新、失败回退 |
| `control.ControlLoop` / `HeartbeatSimulator` | 周期性故障检测 + level-triggered 全量调谐、节点心跳模拟 |
| `event.EventLog` | 业务事件审计（创建/驱逐/重建/版本切换/冲突/告停全部留痕） |
| `revision.RevisionLog` / `Revision` | 发布修订历史：序号连续、前后声明、成功/失败/被回退与原因，只增不改 |
| `persistence.LocalStateStore` | 本地 JSON 状态文件：原子写、格式标识、SHA-256 校验和、崩溃恢复 |
| `web` | REST API 与结构化错误（409 冲突 / 404 不存在 / 400 非法规格） |
| `time.ClockService` | 可替换时钟，测试中精确控制“心跳超时 / 滚动更新超时” |

## 发布历史与按修订回滚

### 修订历史

- **什么才算一条修订**：只有**真正改变期望状态**的提交（创建、内容不同的更新、回滚）才追加一条
  修订；完全相同的重复提交是幂等空操作，乐观锁冲突被明确拒绝的提交也不落历史，因此修订号
  **从 1 开始严格连续、绝不跳号**；
- **修订内容**：修订序号、对应 generation、来源（CREATE/UPDATE/ROLLBACK）、回滚目标修订号、
  变更前/变更后的完整声明（副本数、版本、maxSurge、minAvailable 等）、产生时间、
  最终结果（IN_PROGRESS / SUCCEEDED / FAILED / ROLLED_BACK）与原因、终态时间；
- **不可变**：序号与前后声明永久固定，结果只允许从 IN_PROGRESS 迁移到终态一次；一次成功的回滚会把
  被它取代的、此前已成功的修订标记为 ROLLED_BACK（此外不允许任何改写）；**工作负载删除是历史唯一的
  抹除路径**（随 purge 一并删除）；
- 查询：`GET /api/workloads/{uid}/revisions` 列出全部（按序号升序），
  `GET /api/workloads/{uid}/revisions/{n}` 查看单条；不存在的修订号回滚返回 `400 INVALID_ROLLBACK`。

### 按修订回滚

- `POST /api/workloads/{uid}/rollback`，请求体 `{"targetRevision":2,"resourceVersion":"rv-x"}`；
  **省略 `targetRevision` 即“退回上一个修订”**（最近一条修订的前一条；只有一条时回到其自身声明）；
- 回滚取目标修订声明中的**副本数与版本**作为新的期望，**复用正常滚动更新过程**：同样受
  `maxSurge` / `minAvailable` 可用数下限与节点容量上限约束，逐轮“先建新、后删旧”，失败/超时明确告停；
- 回滚**本身是一条新的 ROLLBACK 修订**（携带 targetRevision），不覆盖、不抹掉任何旧历史；
- 回滚与更新使用**完全相同的乐观并发保护**：必须携带当前 resourceVersion，过期版本 / 盲写一律
  `409` 明确拒绝，被拒不产生修订；与正在进行的升级、扩缩容撞车时恰一个胜出，其余冲突失败；
- **回滚的硬保证**：成功后只运行目标版本，收敛实例数**精确等于期望副本数**，无任何多余实例；
  若目标版本起不来、容量或健康节点确实不够，控制面**只减不增地清掉全部非目标版本实例后告停**
  （修订 FAILED + 明确原因码），绝不允许两个版本同时继续对外服务；告停后不自动反复重试，
  只有重新提交期望才会再次推进。

## 进程重启与崩溃恢复（本地持久化）

全部权威状态保存在本机目录 `controlplane.state-dir`（默认 `./data/control-plane`）下的单个
JSON 状态文件 `control-plane-state.json`，**不依赖任何数据库 / 消息队列 / 外部服务**。

- **落盘时机与范围**：每次提交、调谐动作、节点注册/故障注入之后都原子落盘。内容覆盖：
  工作负载期望 spec 与全部调谐进度（generation、observedGeneration、更新阶段、回退目标、
  deadline/预算、条件与原因、活动修订号）、全部实例权威记录、**节点登记信息及其容器视图**、
  **完整发布修订历史**、resourceVersion 序列；
- **重启恢复**：进程启动时先整体装载状态文件，再从恢复出的期望状态继续 level-triggered 调谐，
  从**中断处**接着收敛，最终结果与全程不中断跑完一致——已经完成的替换不会重做，没做完的不会被
  当成做完，实例 id 稳定沿用所以不重复、计数不漂移；进行中的更新/回滚、已经告停的失败现场
  （含原始原因，且不自动重试）都原样恢复；
- **写到一半被强杀**：落盘采用“写同目录临时文件 → `rename` 原子替换正式文件”，
  强杀只可能发生在替换之前或之后，磁盘上永远是**变更前或变更后两个完整一致状态之一**，
  不会出现半截/自相矛盾状态；启动时会清理遗留临时文件。

### 本地数据兼容与容错范围

| 情形 | 行为 |
| --- | --- |
| 状态文件不存在（首次启动） | 以空状态正常启动 |
| 临时文件残留（上次强杀点之前） | 启动时自动清理临时文件，正式文件不受影响 |
| 文件被截断 / JSON 损坏 | **拒绝启动**并明确报错（不会用半截状态凑一个进程） |
| `formatId` 不认识 | **拒绝启动**，不做猜测性解析 |
| `formatVersion` 高于当前支持版本 | **拒绝启动**（不支持向前兼容猜测）；同版本允许存在未知字段（忽略），旧版本文件可被新版本读取 |
| SHA-256 校验和不匹配（被改写/磁盘损坏） | **拒绝启动**并报告期望/实际校验和 |

外层文件结构：`{formatId:"CCP-STATE", formatVersion:1, savedAt, checksum, payload}`，
其中 `checksum` 为 `payload` 的 SHA-256。需要人工处置时，可先备份再删除状态文件，
    进程将以空状态重新启动（此时历史权威状态视为放弃，不会尝试自动合并）。

## 一、调谐语义（Reconciliation）

控制面是 **level-triggered（水平触发）** 而非 edge-triggered：

- 控制循环（默认每 100ms）先做节点故障检测，再对**所有**工作负载调谐；
- 每次写操作（提交/删除）之后也会同步驱动一次该工作负载的调谐；
- 调谐器只信任两类输入：存储中的权威 `InstanceRecord` + 节点上的真实容器视图，
  不依赖任何“上次发生了什么”的内存事件。

因此：

1. **重复提交**：期望规格逐字段相等时，`submit` 是幂等空操作——`generation` 与
   `resourceVersion` 均不变，不会产生任何新实例；
2. **重复事件 / 空转**：对同一状态反复调谐是零副作用的，节点对同一实例 id 的 `start`
   也是幂等的，计数不会漂移、RUNNING 不会回退；
3. **调谐过程中重启**：重启后 `reconcileAll` 只从存储出发即可恢复全部动作，
   “漏事件 / 重放事件”不影响最终状态（见 `ReconcileIdempotencyTest`）；
4. **每轮至多一个变更动作**（建/删/驱逐），调谐是一连串小而确定的步骤，过程可逐步解释。

## 二、节点故障与自愈

- `NodeRegistry` 依据心跳超时或将节点注入 `partition` 判定失联，一旦失联立即把节点
  标记为 `UNREACHABLE` 并 **fence**：其上实例记录置为 `LOST`，随后在剩余健康节点重建；
- 重建由 `Scheduler` 选节点，**严格不超容量**；剩余容量不足时不做超额调度，
  而是置 `FAILED` 并记录 `Unschedulable` 原因（新节点加入后下一轮自动恢复）；
- 失联节点若恢复，控制面先 **wipe 其本地残留容器**，再允许它重新承载负载，
  从机制上杜绝“旧机器没真死、两边都在跑”的脑裂双实例；
- 健康节点上的容器若意外消失，同样按 `LOST` 自愈。

## 三、滚动更新策略

每次期望规格变化都会让 `generation + 1`；**仅当镜像（版本）变化时才触发滚动更新**，
纯副本数变化走普通扩缩容，不做无谓的实例替换。

更新参数（在 spec 中声明）：

- `maxSurge`：允许超过期望副本数的峰值（默认 1）——提供“先建新、后删旧”的空间；
- `minAvailable`：全过程可用实例数（健康节点上 RUNNING）的硬下限（默认等于副本数）；
- `maxStartRetries`：新版本启动失败的最大尝试次数（默认 5）；
- `rolloutDeadlineSeconds`：更新超时（默认 120s）。

执行保证：

1. 任何一个中间步骤之后，可用实例数都 **≥ `minAvailable`**（测试逐步采样验证）；
2. 有 surge 空间时先创建新版本实例；空间不足且可用性允许时才删除一个旧实例腾位；
3. 新版本就绪数达标后，旧版本实例被**彻底清理**，不会长期混跑；
4. **失败明确告停**：新版本连续启动失败超过预算，或超过 deadline 仍无法推进
   （例如容量与可用性约束形成死锁、无健康节点），控制面会：
   - 只减不增地回收全部新版本实例；
   - 在旧版本上把副本数补齐（首轮部署无旧版本可回退时直接告停）；
   - 置 `updatePhase=FAILED / phase=FAILED`，条件中给出
     `RolloutFailed` 或 `RolloutDeadlineExceeded` 的明确原因；
   - 之后停止推进新版本（仅保留节点故障自愈），必须由人工重新提交期望状态才能继续，
     因此不会出现两个版本长期混跑。

## 四、容量规则

- 只有 `HEALTHY` 且未被分区的节点可以承载新实例；
- 节点槽位 = 节点上的全部实例记录数（失联节点上的 `LOST` 记录仍占位，
  在健康节点完成重建并摘除旧记录前，不会把槽位再次分配，避免恢复瞬间超卖）；
- 选点策略：剩余容量最多优先，平局按节点 uid 字典序，结果确定、可预测；
- 无健康节点 / 全部满载时返回带原因的不可调度决策，而不是无限静默重试。

## 五、并发冲突规则

对同一工作负载的修改采用**乐观并发控制（OCC）**：

- 对象上带单调递增的 `resourceVersion`；`PUT`/`DELETE` 必须携带上次读到的版本；
- 规格相同 → 幂等空操作；规格不同且版本匹配 → 接受并换发新版本、`generation+1`；
- 版本不匹配 / 不带版本的盲写 → 返回 **HTTP 409** `RESOURCE_VERSION_CONFLICT`
  （响应体同时给出提交版本与当前版本），本次操作被明确拒绝，**绝不静默覆盖**；
- 删除后到达的更新返回 **404**，工作负载不会被“复活”；
- 同一工作负载的调谐临界区由每 uid 互斥锁串行化，不同工作负载互不阻塞；
  并发扩缩容/改版本压测下“恰有一个提交胜出，其余全部冲突失败”，最终状态确定。
- 调用方的标准恢复方式是 **read-modify-write**：重新 GET 最新版本后合并重试。

## 六、验证方法

`mvn test` 运行 33 个用例，按 6 个业务域组织（均带中文业务场景描述，
日志中搜索 `[BUSINESS-TESTS]` 可见覆盖了哪些业务、每个用例的通过情况）：

| 测试类 | 覆盖业务 |
| --- | --- |
| `ReconcileIdempotencyTest` | 收敛正确性、重复提交幂等、重复事件零漂移、调谐中途重启不重复、节点起停幂等 |
| `NodeFailureTest` | 失联 fence + 健康节点重建、容量约束与明确失败原因、节点恢复 wipe 防脑裂 |
| `RollingUpdateTest` | 更新成功且旧版本清零、全过程不低于 minAvailable（逐步采样）、新版本失败回退告停、超时回退告停 |
| `ConcurrentConflictTest` | 过期版本拒绝、8 线程并发恰一胜出、删除后更新 404、读-改-写重试成功、盲写拒绝 |
| `RevisionHistoryTest` | 修订生成/连续序号/前后声明/终态查询、重复与冲突不入历史、按修订回滚单版本精确收敛、“上一个修订”、回滚目标起不来告清零残留、回滚过期版本/盲写/不存在修订号、回滚与更新并发恰一胜出 |
| `PersistenceRecoveryTest` | 重启后期望与历史不丢并继续收敛、滚到一半重启断点续跑且实例不重复、已收敛不重做、强杀落在原子替换前后的一致性、损坏文件/未知 formatId/高版本拒绝启动、回滚中途重启续跑、失败现场重启保持原样不自动重试 |

测试通过可替换的虚拟时钟确定性地制造“心跳超时”“更新 deadline 超时”，
通过故障注入接口制造网络分区与镜像启动失败，不依赖真实时间与真实故障。

## 配置项

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `controlplane.reconcile-interval-ms` | 100 | 控制循环调谐周期 |
| `controlplane.heartbeat-interval-ms` | 50 | 模拟节点心跳周期 |
| `controlplane.heartbeat-timeout-seconds` | 8 | 心跳超时判定阈值 |
| `controlplane.state-dir` | `./data/control-plane` | 本地权威状态目录（原子落盘 + 崩溃恢复） |
