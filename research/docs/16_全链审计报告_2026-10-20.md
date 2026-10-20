# stock_monitor 全链审计报告（代码 + 内容）

> 审计时间：2026-10-20｜审计人：WorkBuddy Agent｜对应提交：`9aa7b743`（D26 第十一轮）
> 范围：`quant_engine/` 86 个 Python 文件（15,450 行）+ `test/` 8 个测试文件（108 用例）+ `prd/` 16 个文档 + README + manifest.json
> 基线：HEAD=1cebb6bc（E 线 CodeGuard），工作区干净，本地领先 origin/main 4 个提交未 push

---

## 一、总结论

**总体健康度：良好（无 P0 致命问题）**。代码库安全基线干净、测试覆盖充分、文档与实现高度一致。本轮发现 **P1×3、P2×6**，其中 **5 项已当场修复并提交**，3 项记录在案（涉及设计权衡或需 ECS 实测，不建议盲目改）。

| 维度 | 评分 | 说明 |
|---|---|---|
| 安全 | ★★★★☆ | 无硬编码密钥、无危险函数、无注入面；鉴权覆盖 35/36 路由（/health 免鉴权符合惯例）；未配置 token 时全端点开放有一次性告警 |
| 健壮性 | ★★★★☆ | 无裸 except、无无超时请求、外部调用全 try 隔离；Redis 优雅降级；仅 2 处 `except: pass`（均可接受） |
| 正确性 | ★★★★☆ | 三源代码错判已修（E 线）；本轮揪出 1 个测试时间炸弹 + 1 个调度硬编码缺陷 |
| 文档一致性 | ★★★★☆ | 编号体系完整（CG1-6/V1-10/G1-30/D1-26）；版本号已同步；manifest 清单与文件一一对应 |
| 测试 | ★★★★★ | 108/108 通过，fixture 均为真实 API 响应，覆盖解析/去重/校验/修复全链路 |

---

## 二、代码审计

### 2.1 安全审计（全部通过）

| 检查项 | 方法 | 结果 |
|---|---|---|
| 硬编码密钥 | 正则扫描 api_key/secret/token/password 字面量 | **0 处**；config.py 全部密钥默认空串，凭据只在 .env（已 gitignore） |
| 危险函数 | eval/exec/pickle/yaml.load/subprocess/shell=True/verify=False | **0 处** |
| 注入面 | 无 SQL（Redis+内存）、无模板渲染、无命令拼接 | 无 |
| API 鉴权 | 逐路由块比对 verify_api_key | 36 路由中 35 个带鉴权；仅 `GET /health` 免鉴权（惯例） |
| 密钥比对 | `api_key != settings.API_AUTH_TOKEN` | 非常量时间比较，理论上有时序侧信道；内网站点实践风险可忽略（记录 P2-7） |
| 钉钉签名 | hmac-sha256(timestamp\nsecret) + quote_plus | 实现正确 |

**鉴权设计说明**：未配置 `API_AUTH_TOKEN` 时跳过鉴权（向后兼容），`_warn_no_auth()` 保证只告警一次。生产 ECS 必须配置 token——建议部署脚本加一道检查（见 §六）。

### 2.2 健壮性审计（通过，2 个可接受项）

- 异常处理：全项目无裸 `except:`；仅 `cache.py:171`、`signal_generator.py:74` 两处 `except Exception: pass`，均为缓存/展示非关键路径，可接受
- 外部请求：全部带 timeout（10-15s）；东财/腾讯/新浪均有重试或降级
- Redis 不可用时：缓存层降级进程内内存，监控/校验功能不中断（本轮实测确认：本地无 Redis 全链路正常）
- 外部源故障隔离：NewsSourceManager 单源失败不影响其余源；CodeGuard 批量丢批只降级为 warning

### 2.3 正确性审计（本轮主要战果）

**P1-1 测试时间炸弹（已修）**
`test_collect_digest_preview_with_fixtures` 在 2026-10-20 复现失败：fixture 新闻发布于 10-18，超出 `news:last_push_at` 缺失时的 20h 回看窗，150 条被全部滤除 → digest None。**凡走墙钟时间窗的 fixture 测试都会随日期推移失效**。修复：冻结 `_window_start` 为 fixture 时间之前的固定值，用例与创建时间解耦。教训已写入 12 文档 D26 方法论。

**P1-2 worker 调度硬编码 sleep（已修）**
`SELF_CHECK` 分支 `time.sleep(600)`：若 worker 在 9:16-9:24 之间启动/重启，一觉睡到 9:26+，**错过 CALL_AUCTION（9:25-9:30）集合竞价播报**。同理 `CALL_AUCTION` 的 `sleep(300)` 在 9:27 进入时会睡到 9:32。修复：两处均改 `max(get_sleep_seconds(phase), 5)`，精确睡到时段边界（9:25 / 9:30），晚启动场景不再睡过头。

**P2-3 活跃池双重刷新（已修）**
phase 切换到 SELF_CHECK 时刷新一次（`if phase != last_phase` 内），同轮 `if phase == "SELF_CHECK"` 再刷新一次——每次进入竞价打两轮全市场行情接口。已删除 phase 切换处的重复刷新。

**P2-4 CodeGuard 死常量与注释过度承诺（已修）**
`KEY_DELISTED` 定义后从未使用（退市集合实际未落地）；`missing` 分支注释称"连续两次查无才升级 error（由 check_and_push 计数）"，但 `check_and_push` 并无此计数逻辑。已删常量、注释改为诚实描述（missing 恒定 warning，疑似退市人工确认）。

### 2.4 性能与并发（记录未修）

- **P2-5 stock_list 全量校验开销**：`collect_targets` 第三路会聚合 `stock_list` 全市场缓存（~5,400 只），CodeGuard 按 50/批查腾讯 → 100+ 批 × 0.15s 间隔 ≈ 20s+，盘前 15 分钟窗口内可完成但偏重。建议后续改增量校验（只查新增/变更代码）或抽样。
- **P2-6 并发触发**：API 手动触发（线程池同步执行）与 worker 定时可能并发跑 CodeGuard，无共享可变状态（仅 cache 写），实际无害；报告键会被后写覆盖，可接受。
- **P2-7 WebSocket token 走 query 参数**：`/ws?token=xxx` 会留在访问日志/浏览器历史。WS 协议无法自定义头，属常见权衡；建议日志脱敏（记录未修）。

---

## 三、内容审计（PRD 文档 vs 实现）

### 3.1 编号体系核对（全部完整）

| 体系 | 文档 | 编号 | 状态 |
|---|---|---|---|
| 代码一致性需求 | 01 §7.5 | CG1-CG6 | ✅ 完整 |
| 代码一致性验收 | 08 §12 | V1-V10 | ✅ 完整（含"全绿不推送/自动修复/worker 双窗口"验收标准） |
| 风险登记 | 10 | G1-G30 | ✅ 完整（G28-G30 为 E 线新增） |
| 审计决策 | 12 §3 | D1-D26 | ✅ 本轮补 D26 |
| 键结构登记 | 03 §4.3/4.4/4.5 | quote/etf_holdings/news/code_guard | ✅ 本轮补 §4.5 |

### 3.2 数字口径核对

| 声明 | 文档位置 | 实测 | 结论 |
|---|---|---|---|
| 全量测试 108/108 | README、12 | 108/108（本轮三次复跑） | ✅ |
| 现行 38 端点 | 07 §7 标题 | 36 HTTP + 1 WS + 1 根路由 = 38 | ✅ 口径吻合，已细化表述注明构成 |
| ETF/news/code-guard 端点明细 | 07 §7 表格 | 与 routes.py 逐一相符 | ✅ |
| manifest v2.4 / date 2026-10-18 | manifest.json | — | ✅ |
| 01/02/03/07/08 版本行 | 各文档头部 | 原停留 v2.3（内容已含 E 线） | ❌→✅ 本轮升 v2.4 |
| manifest documents 清单 | manifest.json | 15 个文档与实际一一对应 | ✅（README 为索引单列） |

### 3.3 内容质量 spot check

- 08 §12 的 V1-V10 与代码实现精确对应（包括 PRICE_GAP_THRESHOLD=11% 缓冲、hot_etf 简称跳过名称校验等细节）✅
- 07 的故障矩阵、worker 时段表已含 E 线双窗口 ✅
- README 状态看板准确（E 线描述、61 个新测试、待办 5 项）✅
- 12 的复查清单 7 项全部可执行 ✅

---

## 四、发现清单总表

| # | 级别 | 发现 | 状态 |
|---|---|---|---|
| 1 | P1 | 测试时间炸弹（fixture 老化） | ✅ 已修（9aa7b743） |
| 2 | P1 | SELF_CHECK sleep(600) 硬编码可错过竞价播报 | ✅ 已修 |
| 3 | P1 | 五文档版本行与 manifest 不同步 | ✅ 已修 |
| 4 | P2 | 活跃池双重刷新 | ✅ 已修 |
| 5 | P2 | CodeGuard 死常量 + 注释过度承诺 | ✅ 已修 |
| 6 | P2 | 03 未登记 code_guard 键 | ✅ 已修（补 §4.5） |
| 7 | P2 | 12 缺 D26 记录 | ✅ 已修 |
| 8 | P2 | stock_list 全量校验盘前耗时 ~20s | 📋 记录（后续增量校验） |
| 9 | P2 | WS token 走 query 入日志 | 📋 记录（日志脱敏可缓解） |
| 10 | P2 | README 待办① `/api/market` 全涨 11%+ 异常根因未查 | 📋 记录（疑与代码错判同源，ECS 部署 CodeGuard 后观察） |

---

## 五、验证证据

1. **全量测试**：`unittest discover` 三次复跑均 `Ran 108 tests ... OK`（修复前 1 failure）
2. **导入冒烟**：worker/code_guard/routes/四数据源全部 import OK
3. **sleep 逻辑实测**：`get_sleep_seconds('SELF_CHECK')` 返回距 9:25 的精确秒数（当前 7620s = 07:19→9:25 语义正确）
4. **路由核对**：36 HTTP 路由逐一列出与 07 文档表格比对相符
5. **manifest 核对**：documents 清单 15 项与实际文件零差异
6. **安全扫描**：密钥/危险函数/裸 except/无超时请求四项均零命中

---

## 六、遗留事项与后续建议

1. **（建议）部署脚本加 token 检查**：`deploy_onekey.sh` 启动前若 `API_AUTH_TOKEN` 为空应 warning 甚至拒绝启动——生产暴露无鉴权 API 是本节最重的残留风险
2. **（观察）ECS 部署验收**：本地领先 origin/main 4 个提交（C/D/E 线 + 本轮修复），部署后首个交易日看三个日志点：9:00 CodeGuard static 报告、9:15 新闻推送、`/api/market` 全涨异常是否复现（若复现说明与代码错判不同源，需单独立项）
3. **（优化）CodeGuard 增量校验**：stock_list 全量 ~20s 可接受但笨拙，建议缓存"已校验 code 集合"，只查新增
4. **（纪律）fixture 时间冻结**：今后新增带时间窗的测试一律显式固定窗口（本轮教训，已入 D26 方法论）
5. **（流程）push 前先跑全量测试**：本轮时间炸弹如果 CI 常驻不会活过两天——建议部署流水线加 `unittest discover` 门禁

---

*本报告与提交 9aa7b743 对应；审计过程与设计裁定已同步至 prd/12 §3.13（D26）。*
