# 批次加工服务与主备实验

## 已有设施及部署条件
Python 3.11+ 标准库与 SQLite；目标为 macOS 13+ 已登录用户会话，已有长期可用的 Python 解释器，无管理员权限或外部服务依赖。解释器不会随开发副本移走。ROOT、CONTROL 和发布包路径均可含空格。包与业务数据同在 ROOT，CONTROL 只放钩子及日志；两个实例是 ROOT/a、ROOT/b 下相互独立的 state.sqlite，不是同库两个连接。共享 journal.sqlite 位于 ROOT，本题不模拟共享日志、整个磁盘或机器丢失。

pipeline.py 的最高修订、墓碑、整数汇总和回执保持不变；runtime.py 已有批次重试、文件落盘与单库原子入账。checks.py 保留原业务断言并给出独立期望。journal.py 已实现持久接纳日志和逐批手工重放：输入正文直接存在共享日志，确认前已提交；本地入账后游标未更新时也能重放。ops.py 提供可复现构建、校验、安装及 LaunchAgent；后台协调器目前仅驱动 a，b 可手工核账。没有自动接管、旧主提交隔离或接管恢复机制，不能把原单库文件锁当成双库隔离。不要改写业务核或换数据库，也不要求构建网络复制协议。

## 已有入口
所有命令均为独立 argv，不经 shell。以下 python3 ops.py 在安装后替换为 python3 ROOT/app.pyz：
- build OUT：ZIP 包含 app.pyz 和 manifest.json；相同源码输出须逐字节相同。
- install OUT ROOT --control CONTROL：ROOT 不存在或为空；验证包后才创建安装数据，损坏包不得改变目标。
- start ROOT / stop ROOT：注册并启动 / 卸载本 ROOT 的用户级 LaunchAgent，后台入口为 worker ROOT。
- submit ROOT INPUT：读取 UTF-8 JSON 批次，返回 {"accepted":批次号,"seq":输入序号,"input_sha256":规范输入摘要}。这是输入接纳回执，不代表加工完成。同批同内容返回原接纳值，异内容返回 {"error":"BATCH_CONFLICT"} 并退出2；非法批次也输出 error 并退出2，不占用序号。日志序号从1递增且不删除；已确认输入不得修改。
- read ROOT BATCH_ID：读取当前实例的结果；未完成返回 {"batch_id":批次号,"available":false}，完成还包含 result、receipt。receipt 的 batch_id、input_sha256、result_sha256 沿用原格式。
- status ROOT：返回 active、accepted_seq、replicas 和 phase；replicas 为 {"a":完成序号,"b":完成序号}。完成序号指连续且已经入账的输入前缀。
- run ROOT：同步驱动当前实例处理一条；worker ROOT 是常驻协调器，每轮启动独立 node-step 子进程加工。每个实例有自己的 worker.lock，不同实例不互斥。
- replay ROOT a 或 replay ROOT b：手工重放一条；inspect ROOT NODE BATCH_ID 直接核对指定实例，供回归使用，不作为对外读取入口。
- selftest：执行包内业务和双实例手工重放回归。

单一提交入口可以与后台加工及接管同时调用；不要求多个 submit 进程相互竞争。手工 run/replay 不与同实例后台加工同时使用。本轮只支持首次 a 到 b 接管，不做回切、自动故障判定、跨主机通信、代码升级、历史清理或数据库格式迁移。

## 输入及原业务预期
sample.json 和 second.json 是完整输入。每批具有 batch_id、scale、rows；row 含 id、rev、station、minute、value、deleted，精确规则由 pipeline.validate 固定。同批每个 id 最高 rev 胜出，同 rev 异内容拒绝；胜出的墓碑不进入汇总。按 station、minute 汇总存活记录，value 乘 scale，零和组仍保留；各批独立加工。
sample 结果为 [{"station":"east","minute":0,"count":2,"sum":0},{"station":"west","minute":1,"count":1,"sum":70}]。
second 结果为 [{"station":"south","minute":3,"count":2,"sum":0}]。
输入摘要取 pipeline.encode(batch) 的 SHA-256，结果摘要取同编码汇总数组的 SHA-256。每个实例同批 ledger 只能有一行，原回执重放不变。

## 本轮新增的对外运维接口
新增 promote ROOT REQUEST_ID 和 failover-status ROOT。REQUEST_ID 为非空 ASCII 字母、数字、下划线或连字符。promote 同步返回接管完成回执 {"request_id":REQUEST_ID,"frontier":冻结输入序号,"active":"b"}；进程退出时允许尚未返回。只允许同一接管请求重试，完成后仍返回原回执；另一请求号应明确冲突，不发起第二次切换。failover-status 展示 request_id、frontier、active、accepted_seq、replicas、phase；未请求时 request_id、frontier 为 null。phase 可细分，但必须能区分正常服务、接管未完成及完成服务于 b。

promote 接纳时冻结已确认输入前沿。冻结与并发 submit 必须有明确先后：成功确认的输入要么在冻结前沿内，要么属于切换完成后的 b；不得确认输入后把它遗漏在接管之外。接管中 submit 可以输出 {"error":"MAINTENANCE","retryable":true} 并退出75，此时不能新增日志项；重复提交已接纳批次仍应返回原接纳回执。备用库必须完整入账冻结前沿后才能成为当前实例，不能只修改路由或复制旧库文件。切换后新批次继续由 b 完成，即使旧主仍停在公开钩子中。对外 read 每次固定一个当前实例；允许接管未完成时同样返回明确维护状态，不能拼接两个实例的结果。

旧主可能在已有 stage 或 ledger 钩子暂停而非退出，接管不能等待或终止它。本题不模拟持有数据库写事务或提交隔离锁时被无限 SIGSTOP；公开钩子不得移入这些临界区。切换后恢复的旧执行器不得再提交 ledger，不能仅依靠启动时检查身份或对外隐藏旧库；切换前已完成的入账不撤销。不跨业务计算或结果文件读写持有全局协调锁。输入日志、实例数据库与路由没有跨库事务，需处理它们之间的切换一致性；本题不要求在物理断电下验证磁盘硬件保证。

接管前沿持久化后、路由切换前后协调进程被 SIGKILL，真实 LaunchAgent 重开后必须自动完成同一请求，不依赖人工重发 promote。保留前沿和原接管回执，任何时候只有一个对外服务实例；旧执行器残留不能阻止恢复。新增钩子可自行命名并写入说明，用于准确暂停这些边界，钩子不能代替恢复实现。

## 已有实验与上线交付
python3 -B checks.py verify 核对独立业务期望、输入冻结、接纳幂等、冲突拒绝、主库与备用手工重放，以及本地 ledger 已提交但游标未推进的重试；demo 展示两个批次的实际结果与回执。这些基线回归不验证接管。
python3 ops.py build OUT 后，python3 lab.py normal OUT NEW_DIRECTORY 是正常安装实验：解包重建、核对逐字节一致、移走开发副本，使用安装包和真实 LaunchAgent 加工 a，再手工重放 b 并核账，证据保存在实验目录。lab.py 的 command、wait_for、service_pid 可复用，停止代理用安装包 stop ROOT。

暂停旧主样例：安装并启动后在 CONTROL/a/hook.json 写 {"phase":"ledger"}，再提交 sample；看到 CONTROL/a/ledger.ready 后其内容是旧执行器 PID。此时可以继续提交 second，两个接纳回执必须留存。在另一个进程发起接管，期间不要写 ledger.go 或杀旧主。接管完成后写 CONTROL/a/ledger.go，核对旧主没有晚到入账，b 包含两个批次的原结果与回执。stage 钩子暂停在计算前，result 钩子暂停在结果文件写入前；各钩子通过后生成 .spent，单个实验只触发一次。更换实验使用新 ROOT。

本轮上线回归还应在上述接管边界杀死协调进程，用 launchctl 实际 PID 证明自动重开，核对同一接管回执、唯一当前实例及后续新批次加工。证据应保留实际命令、协调器和旧执行器 PID、冻结前沿、两库完成前缀、原业务回执及旧主恢复前后 ledger 核账，不能用修改数据库、伪造状态或仅调用函数代替真实进程实验。实验期间所有进程使用安装产物，开发副本移走后仍可完成。

交付可校验发布包、简短接管及恢复说明、可重跑上线证据。运行清单保留 version=1、kind=cli、setup 为 argv 数组列表；最终 verify 和 demo 须经过安装产物核对并展示真实业务与接管结果，不要求固定新增测试数量、文件名或证据 schema。现有 build/install/start/stop 设施可复用。
