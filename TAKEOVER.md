# 接管与恢复说明（a → b 单次主备切换）

本轮在既有批次入仓服务上新增一次性 a→b 接管。业务核（pipeline.py）、输入接纳回执、
既有结果与回执格式均未改变；接管设施全部在于 journal.py / ops.py / runtime.py 的
协调层。以下入口均经安装产物 `ROOT/app.pyz` 调用。

## 对外接口

- `promote ROOT REQUEST_ID`：发起或重试接管。REQUEST_ID 为非空 ASCII 字母/数字/`_`/`-`。
  同步返回接管回执 `{"request_id":…,"frontier":冻结序号,"active":"b"}`；进程退出时允许
  尚未返回（由协调进程继续完成）。同一 REQUEST_ID 重试（含完成后）返回原回执；
  另一请求号返回 `{"error":"FAILOVER_CONFLICT"}` 并退出 2，不发起第二次切换。
- `failover-status ROOT`：`{"request_id","frontier","active","accepted_seq","replicas","phase"}`。
  未请求时 request_id/frontier 为 null。phase 三态：`serving`（正常，a 服务）、
  `takeover`（接管未完成）、`serving_b`（完成，b 服务）。`status` 的 phase 同源。
- 接管未完成时：`submit` 新批次返回 `{"error":"MAINTENANCE","retryable":true}` 退出 75
  且不新增日志项；重复提交已接纳批次仍返回原接纳回执（异内容仍 BATCH_CONFLICT 退出 2）；
  `read` 同样返回 75 维护态。任何时刻至多一个实例对外服务，绝不拼接两个实例的结果。

## 一致性与 fencing 约定

- 冻结在 journal.sqlite 单个写事务内完成，与并发 submit 明确先后：成功确认的输入
  要么落在冻结前沿内，要么属于切换完成后的 b；不会确认后遗漏。
- 备用库 b 必须逐条真实入账整个冻结前沿（重放游标 ≥ 前沿）后才切换路由；
  不只改路由、不复制旧库文件。
- 全局协调锁（journal 写事务）只覆盖冻结/切换/标记这类短事务，不跨业务计算或
  载荷文件读写；b 的追赶只持 b 自己的 worker.lock，且逐条释放。
- 旧主停在 stage/result/ledger 公开钩子时，接管不等待、不终止它。切换前接管方先把
  a 的 fence 置位（seal）；该标记与旧执行器的 ledger 提交同库同事务序列化，恢复后的
  旧执行器提交时被 FENCED 拒绝，不会晚到入账；切换前已完成的入账不撤销。
- 切换后新批次由 b 完成，即使旧主仍停在公开钩子中。

## 崩溃恢复

接管状态机持久化在 journal.sqlite 的 failover 行（request_id、frontier、phase、
原回执）。恢复步骤幂等：seal a → b 追平前沿 → 切路由 → 标记 done。协调进程在
前沿持久化后、路由切换前后任意点被 SIGKILL，自启动设施重开后自动续跑同一请求，
不依赖人工重发 promote；旧执行器残留不阻塞恢复。macOS 上自启动为用户级
LaunchAgent（KeepAlive）；无 launchd 的开发机上由安装产物内 supervisor 命令提供
同样的 start/stop/自动重开语义（CONTROL/supervisor.pid、CONTROL/worker.pid）。

## 实验钩子（仅用于暂停边界，不代替恢复实现）

- 既有按实例钩子：`CONTROL/<a|b>/hook.json` 写 `{"phase":"stage|result|ledger"}`，
  `.ready`（执行器 PID）→ 写 `.go` 放行 → 生成 `.spent`，单次实验触发一次。
- 新增接管边界钩子：`CONTROL/hook.json` 写
  `{"phase":["failover-frontier","failover-preswitch","failover-postswitch"]}`，
  分别在前沿持久化并 seal 后、b 追平前沿后切路由前、切路由后标记完成前暂停；
  协议同上（`.ready/.go/.spent`，按名各自单次）。协调进程被杀重开后会重新停在
  未 `.spent` 的同一边界，便于逐边界注入故障。

## 上线回归与证据

运行清单（.pointwise-runtime.json，version=1、kind=cli）：

- setup：`python3 ops.py build release.zip` —— 构建可复现发布包（同源码逐字节相同）。
- verify：`python3 lab.py takeover release.zip evidence/takeover` —— 经安装产物的
  上线回归：解包重建并核对逐字节一致、移走开发副本；场景一旧主停 ledger 钩子时
  完成接管、新批次由 b 入账、放行后旧执行器被 FENCED 且无晚到入账；场景二在三个
  接管边界 SIGKILL 协调进程（并杀掉 promote CLI），由真实自启动重开并完成同一请求，
  核对维护态退出 75、原回执幂等、异请求冲突、唯一当前实例与后续批次加工。
- demo：`python3 lab.py demo release.zip evidence/demo` —— 安装产物上展示两个批次的
  真实结果与回执、接管回执及切换后由 b 提供的同一回执。

证据（实际命令、协调器/旧执行器 PID、冻结前沿、两库完成前缀、原业务回执、旧主恢复
前后 ledger 核账）写入各证据目录 evidence.json。重跑前先删除对应证据目录
（`rm -rf evidence`）；证据目录非空时实验拒绝覆盖。快速回归：
`python3 -B checks.py verify`（基线）与 `python3 -B checks.py failover`（接管，进程内）。
