# 单机容器编排控制面（Java / Spring Boot 模拟实现）

一套**不依赖任何外部服务**（无数据库、无消息队列、无真实容器运行时）即可单机运行的容器编排
控制面：用内存存储承载权威状态，用本地模拟节点（`SimulatedNode`）承载工作负载。
控制面实现了声明式期望状态、持续调谐（reconcile）、节点故障自愈、版本滚动更新与乐观并发控制，
并保证在**任意提交顺序、重复事件、调谐过程中重启、异常时序**下最终收敛且结果可解释。
所有权威状态（期望状态、发布历史、进行中的更新/回滚进度、节点容器投影）都**持久化在本地文件**，
进程停掉再拉起来可以从中断处继续，不依赖任何数据库或外部服务。

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

# 7) 查询发布历史（每次真正改变期望状态的提交一条修订，修订号连续不跳号）
curl -s localhost:8080/api/workloads/demo/revisions
curl -s localhost:8080/api/workloads/demo/revisions/2

# 8) 按修订回滚（省略 revision 即“退回上一个修订”；resourceVersion 规则与更新一致）
RV=$(curl -s localhost:8080/api/workloads/demo | python3 -c 'import sys,json;print(json.load(sys.stdin)["resourceVersion"])')
curl -s -X POST localhost:8080/api/workloads/demo/rollback \
  -H 'Content-Type: application/json' \
  -d '{"revision":1,"resourceVersion":"'$RV'"}'

# 9) 模拟“进程停掉再拉起”：内存清空，仅从本地文件恢复并继续收敛
curl -s -X POST localhost:8080/api/control-plane/restart-process
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
| `persist.LocalStateStore` | 本地文件权威状态：原子写快照、启动恢复、半写/损坏容错（格式标识 + 版本号） |
| `web` | REST API 与结构化错误（409 冲突 / 404 不存在 / 400 非法规格） |
| `time.ClockService` | 可替换时钟，测试中精确控制“心跳超时 / 滚动更新超时” |

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

`mvn test` 运行 37 个用例，按 7 个业务域组织（均带中文业务场景描述，
日志中搜索 `[BUSINESS-TESTS]` 可见覆盖了哪些业务、每个用例的通过情况）：

| 测试类 | 覆盖业务 |
| --- | --- |
| `ReconcileIdempotencyTest` | 收敛正确性、重复提交幂等、重复事件零漂移、调谐中途重启不重复、节点起停幂等 |
| `NodeFailureTest` | 失联 fence + 健康节点重建、容量约束与明确失败原因、节点恢复 wipe 防脑裂 |
| `RollingUpdateTest` | 更新成功且旧版本清零、全过程不低于 minAvailable（逐步采样）、新版本失败回退告停、超时回退告停 |
| `ConcurrentConflictTest` | 过期版本拒绝、8 线程并发恰一胜出、删除后更新 404、读-改-写重试成功、盲写拒绝 |
| `ReleaseHistoryTest` | 修订序号连续与前后声明、终态（成功/失败/被取代）与原因、重复与冲突不入史不跳号、历史仅随删除消失 |
| `RollbackTest` | 指定修订/退回上一个、回滚后单版本精确收敛、目标版本起不来零非目标残留告停、容量不足告停、回滚并发冲突、重复回滚幂等 |
| `CrashRecoveryTest` | 收敛后重启、滚动/回滚中途重启续跑、历史跨重启保留、写盘半杀前后一致、失败现场重启不重试、损坏文件隔离启动 |

测试通过可替换的虚拟时钟确定性地制造“心跳超时”“更新 deadline 超时”，
通过故障注入接口制造网络分区与镜像启动失败，不依赖真实时间与真实故障。

## 七、发布历史（修订）与按修订回滚

### 7.1 修订历史语义

- 每次**真正改变期望状态**的提交（首次创建、期望变更、回滚）都产生一条修订记录，
  修订序号等于提交生效后的 `spec.generation`，严格连续、**只增不跳号**；
- 修订内容：修订号、创建时间、来源（`CREATE` / `UPDATE` / `ROLLBACK`）、
  **变更前与变更后的完整声明**（副本数、版本、maxSurge、minAvailable 等）、
  最终状态与原因、终态时间；
- 修订终态有四种，均如实反映这次发布的结局：
  - `SUCCEEDED`：该修订声明的期望已成功收敛；
  - `FAILED`：发布失败告停（普通更新失败时服务自动回退到旧版本，修订仍记为 FAILED 并带原因）；
  - `ROLLED_BACK`：该修订曾成功/在途，但被一次**显式回滚**撤销（原因写明被哪条回滚修订撤销）；
  - `SUPERSEDED`：发布未完成就被更新的人工提交取代，不再作为期望被追求；
- **完全相同的重复提交**是幂等空操作，**版本号冲突被拒的提交**不会进入历史——
  二者都不占用修订号；被拒的回滚请求同样不入史；
- 历史一旦形成即不可变：终态只写一次，任何后续提交、回滚都不能抹掉或改写已有事实，
  **唯一的消失途径是工作负载本身被删除**（修订是工作负载聚合的一部分）。
- 查询：`GET /api/workloads/{uid}/revisions` 列出全部修订（按号升序），
  `GET /api/workloads/{uid}/revisions/{n}` 查看单条。

### 7.2 按修订回滚语义

- `POST /api/workloads/{uid}/rollback`，请求体 `{"revision": 2, "resourceVersion": "rv-.."}`；
  `revision` 省略为 null 即**“退回上一个修订”**（当前修订号 − 1 那条）；
- 回滚把期望状态换成目标修订声明的**副本数与版本**，走的就是**正常滚动更新通道**：
  先建后删受 `maxSurge` 约束，全过程可用实例数不低于该修订声明的 `minAvailable`，
  调度严格不超节点容量；
- 回滚本身是一条 `kind=ROLLBACK` 的**新修订**（`targetRevision` 指向目标），
  不覆盖、不抹掉原有历史；被撤销的中间修订（目标之后的）如实标记为 `ROLLED_BACK`
  并注明撤销来源，已是 FAILED/SUPERSEDED 的修订保持其原有事实；
- **失败/超时明确告停**，且回滚的失败语义比普通更新更严格：不允许回退到任何
  “非目标版本”继续提供服务。目标版本确认起不来（启动失败超预算）或容量/健康节点
  不足以承载目标副本数时：
  1. 非目标版本实例**只减不增**地被彻底清退（可用性下限不再保护“错误版本”）；
  2. 目标版本能就绪几个就保留几个，一个都起不来就**零实例**，
     收敛实例数只少不多，绝不允许两个版本同时对外服务；
  3. 置 `FAILED` 终态，条件中写明 `RolloutFailed` / `RolloutDeadlineExceeded`
     与容量/健康节点不足的具体原因，**不会自行反复重试**，须重新提交期望才能继续；
- 并发保护与更新完全一致：必须携带当前 `resourceVersion`，过期/缺失版本返回 409；
  回滚与正在进行的升级、扩缩容相撞时，同一临界区内**恰好一个操作胜出**，其余冲突拒绝；
  目标声明与当前期望完全相同的重复回滚请求是幂等空操作，不产生修订、不跳号；
  目标修订号不存在返回 404。

## 八、进程重启与崩溃恢复

### 8.1 持久化覆盖的状态

权威状态以单个 JSON 快照保存在本地目录（默认 `.controlplane-state/state.json`，
可用 `controlplane.state-dir` 配置；测试使用独立临时目录）。快照包含：

- 全部工作负载聚合：期望 spec、generation、resourceVersion 序列、阶段、
  更新阶段、观察世代、就绪计数、实例序号、滚动预算/截止时间、回退目标、
  失败原因、条件列表，以及**完整修订历史**（修订号、时间、前后声明、终态、原因）；
- 全部权威实例记录（id、所属工作负载/节点、镜像、世代、状态、重启计数、创建时间）；
- 节点登记信息（uid、host、容量、健康状态、最后心跳）与节点本地容器投影。

因此同一台机器上把进程停掉再拉起（`POST /api/control-plane/restart-process` 可演示）：
期望状态、发布历史、以及**进行到一半的更新/回滚进度**都在。启动后 level-triggered
全量调谐只从权威记录 + 节点真实视图出发，即可从中断处继续朝目标收敛，
最终结果与全程不中断跑完一致：不会重建已完成的替换、不会把没做完的当成做完、
实例 id 与落点稳定，不出现重复实例或计数漂移。

### 8.2 写盘方式与“写到一半被强杀”

- 每一轮调谐临界区结束（以及每次提交）后做一次**全量快照**；
- 写入流程：全量字节写入 `state.tmp`（`SYNC` 落盘）→ **原子 rename** 覆盖 `state.json`；
- 因此任意时刻强杀，磁盘上只可能是两份**完整**快照之一：
  - 杀在 rename 前：`state.tmp` 是半份、`state.json` 仍是上一份完整快照 → **恢复到变更前**；
  - 杀在 rename 后：`state.json` 已是新快照 → **恢复到变更后**；
  启动时残留的半份 `state.tmp` 直接删除，永不参与恢复。不会出现自相矛盾的中间状态。

### 8.3 本地数据的兼容与容错

- 快照带**格式标识**（`container-control-plane-local-state`）与**格式版本号**（当前 v1）：
  - 文件不存在：首次启动，空状态正常运行；
  - 标识不认识、或版本号**高于本进程支持的版本**、或 JSON 损坏无法解析：
    问题文件被**隔离备份**为 `state.corrupt-<时间戳>.json`，记录一条 `RECOVERY`
    （`CorruptStateQuarantined`）告警事件，控制面**以空状态启动**——
    拒绝猜测、拒绝带着半个/自相矛盾的状态运行；之后的新提交会正常重新落盘；
  - 同版本内的字段以后向兼容方式演进；旧版本进程看到更高版本文件按上面“不认识”处理。
- 注意范围：网络分区、镜像启动失败等**故障注入**是测试/演示用的临时手段，不属权威状态；
  重启后节点默认健康（这与真实机器重启后重新上报心跳一致），随后由调谐重新对账。

## 配置项

| 键 | 默认值 | 含义 |
| --- | --- | --- |
| `controlplane.reconcile-interval-ms` | 100 | 控制循环调谐周期 |
| `controlplane.heartbeat-interval-ms` | 50 | 模拟节点心跳周期 |
| `controlplane.heartbeat-timeout-seconds` | 8 | 心跳超时判定阈值 |
| `controlplane.state-dir` | `.controlplane-state` | 本地权威状态快照目录（期望状态/历史/进度的持久化位置） |
