# eltdx / AxData 深度调研与接入方案（D73，2026-11-21）

> 调研对象：[electkismet/eltdx](https://github.com/electkismet/eltdx) v3.3.5（Rust 驱动通达信客户端）、
> [electkismet/AxData](https://github.com/electkismet/AxData)（eltdx 后继的多源量化数据库框架）。
> 方法：拉取源码逐函数分析 + 真机 pip 安装 + 生产时段真实请求验证。
> 结论一句话：**旧裁定 I-03「公共 TDX 整体停供、不引入 eltdx」的前提已被实证推翻**；
> 新证据表明停供的是"pytdx 客户端实现"，不是上游服务器。但两者许可证均禁止生产/自动化交易服务，
> 因此本轮只落地**默认关闭、零信号影响**的接入适配层，是否启用交由用户裁定。

---

## 0. 执行摘要（先看这里）

| 结论 | 证据 | 影响 |
|---|---|---|
| **TDX 公共行情已恢复可用** | eltdx 内置 43 节点 TCP **43/43 全通**；协议握手 3/3 返回 `pong`；真实日K取到 11-20，与新浪 OHLC **收盘价差 0.0** | 推翻 I-03 前提 |
| **根因是 pytdx 客户端，不是上游** | 同一节点 `116.205.183.150`：eltdx 取到 `KlineBar(2026-11-20, O=11.99/H=12.25/L=11.91/C=12.22)`；本项目 pytdx `get_security_bars` / `get_security_quotes` **恒返回 0 条** | 换服务器列表无效，必须换客户端实现 |
| **eltdx 能力覆盖本项目多个断链点** | 1/5/15/30/60 分钟K、当日分时、逐笔 tick、7709 原生资金流、实时榜单/连板天梯——本项目现无 tick、资金流依赖东财（反封禁敏感）、1 分钟K明确不支持 | 可作为旁路证据/备源 |
| **许可证是硬约束** | eltdx = Non-Commercial Learning and Data Research License；AxData = Research-Only License；均**明文禁止 production service / automated trading service** | 本项目 ECS 定时自动推信号的生产部署，启用前需用户裁定合规 |
| **AxData 不宜直接引入** | 16.6 万行 monorepo，256 接口中默认采集器仅 8 个 TDX 类，多数为 `source_request-only`；Python 3.11+；与本项目"轻量数据源适配层"架构不匹配 | 只借鉴接口清单，不引入框架 |

---

## 1. eltdx 代码级分析（v3.3.5）

### 1.1 架构分层

```
eltdx/
├── crates/eltdx-protocol/   7709 二进制协议编解码（Rust，核心）
├── crates/eltdx-runtime/    Rust 连接池、心跳、主机测速、push 订阅
├── crates/eltdx-python/     PyO3 绑定（cp310-abi3 wheel）
├── src/eltdx/
│   ├── client.py           TdxClient 门面（.bars/.quotes/.helpers/.f10/.ex）
│   ├── hosts.py            43 节点 FALLBACK_HOSTS + 并发探测 + 排名缓存
│   ├── api/                bars / quotes / minutes / trades / money_flow /
│   │                       delisted_bars / corporate / limits / subscription
│   ├── helpers/core.py     latest_stock_list / full_quotes / realtime_rank /
│   │                       limit_ladder / theme_strength_rank /
│   │                       shortline_indicators / auction_data / stock_topics
│   ├── f10/                7615 HTTP Entry（公司概况/资讯/财务，端口 7615）
│   ├── transport/          socket + pool（Rust 连接池）
│   └── ex/                 EX / MAC-EX 扩展行情（期货/期权/基金/外盘）
└── docs/                   METHOD_REFERENCE / API_REFERENCE / FIELD_REFERENCE
```
代码规模：Rust + Python 合计 **84,499 行**。Python 3.10+，提供 cp310-abi3 wheel 覆盖 3.10–3.14 五个平台。

### 1.2 关键函数与接口（逐函数核对）

| 入口 | 函数签名（摘要） | 返回 | 本项目对应断链 |
|---|---|---|---|
| `client.ping()` | `TdxClient(hosts=[...], probe_hosts=False).ping()` | `'pong'` | 协议可用性判据（本项目 pytdx 探活恒失败） |
| `client.bars.get()` | `get(code, period="day", start=0, count=800, adjust=None, all_pages=False)` | `KlineSeries(.bars=tuple[KlineBar])`，`KlineBar(time, open, close, high, low, vol, amount)` | 日K第5源 / 分钟K备源；**支持 1m**（本项目 pytdx 明确不支持） |
| `client.minutes.today()` | `today('sz000001')` | `MinuteSeries(.points=241)` | 当日分时（腾讯 web.ifzq 之外的新源） |
| `client.trades.all_today()` | `all_today('sz000001')` | `TradePage(.ticks=4718)` | **逐笔 tick 本项目完全没有** |
| `client.money_flow.daily()` | `daily('sz000001')` | `MoneyFlowBlock(.records[.buckets])` | 7709 原生资金流，避东财反封禁；`.history()` 走 F10 HTTP 会 503 |
| `client.helpers.realtime_rank()` | `realtime_rank()` | `RealtimeRankTable(.rows[.rank/.last_price/.chg])` | 榜单/异动证据 |
| `client.helpers.limit_ladder()` | `limit_ladder(codes=None, include_touched=False)` | `LimitLadderTable(.rows[.limit_status/.ladder_level/.seal_amount])` | 连板天梯（`market_alert` 算不出） |
| `client.helpers.theme_strength_rank()` | `theme_strength_rank(codes=None, count=None)` | `ThemeStrengthTable` | 题材强度（同上） |
| `client.helpers.shortline_indicators()` | `shortline_indicators(full_codes)` | 竞价/开盘量比/封单/几天几板 | 短线证据（D71 同花顺部分重叠） |
| `client.helpers.auction_data()` | `auction_data(code)` | 竞价过程/09:25 撮合/开盘量 | 竞价证据；实测接受单 code 字符串非 list |
| `hosts.probe_hosts()` | `probe_hosts(hosts, timeout=1.2, max_workers=32, persist=True)` | `list[HostProbeResult]` | 主机测速+排名缓存（可借鉴到 D67 能力探测） |

代码格式陷阱：`normalize_code()` 只接受 **`sz000001` / `sh600519`** 8 位前缀或裸 6 位数字；
本项目 `000001.SZ` 格式传入会抛 `ProtocolError: invalid code`。需在做接入层做格式转换。

周期别名：日K用 `day`；分钟K用 `1m/5m/15m/30m/60m`（或 `5min`）；`min5`/`m5`/`1` 均非法。

### 1.3 主机与连接机制

- `FALLBACK_HOSTS`：43 个 `ip:7709`（`116.205.x` / `111.230.x` / `43.139.x` 等，与本项目 `117.34.114.x` 完全不同）。
- `probe_hosts` 并发 32 线程、默认 1.2s 超时，结果落 `tdx_server_ranking.json` 供下次优先连接。
- Rust 连接池管理心跳/pin/push，避免每次调用重新握手——这是 pytdx 单连接方案不具备的。

---

## 2. AxData 代码级分析（架构借鉴为主）

### 2.1 定位与规模

- 165,811 行 monorepo：Python SDK / FastAPI API(`apps/api`) / Web 控制台(`apps/web`) / 采集 worker / 插件包。
- 前身是 eltdx，共用同一套 `_tdx_wire` 主机默认列表（`116.205.183.150:7709` 等）与协议栈。
- 数据源：TDX 普通行情 90 + TDX Ext 31 + 预装 9（交易所3/巨潮2/腾讯1/东财3）+ core/generic 135 = **256 接口**。

### 2.2 关键观察（文档与代码核对后的诚实结论）

1. **默认采集器只有 8 个**，全是 TDX 系：`stock_codes_tdx`、`stock_suspensions_tdx`、`stock_st_list_tdx`、
   `stock_daily_share_tdx`、`stock_daily_price_limit_tdx`、`stock_kline_daily_tdx`、
   `stock_limit_ladder_tdx`、`stock_theme_strength_rank_tdx`。
2. README 宣称的"交易所/新浪/财联社/开盘红"多数是 `source_request-only`（临时查询，无入库/采集链路），
   文档自己在 `data-interface-inventory.md` 里明确："闭环≠全市场长期采集≠真实源长期稳定性"。
3. TDX Ext 31 接口（期货/期权/基金/债券/外汇/宏观）全是 `adapter-only`，无 DownloaderProfile/CollectorSpec。
4. 独立 collector 用 `runner_entry=axdata_source_tdx.collectors:run_tdx_collector` 直接调 TDX provider，
   不走 `/v1/request` / SDK / ProviderRegistry——即所谓"多源框架"的采集主链仍锁定 TDX。

### 2.3 可借鉴的设计（不引入框架，只借鉴）

| 设计 | 出处 | 本项目的借鉴点 |
|---|---|---|
| 接口资产矩阵（complete / source_request-only / partial / adapter-only / documented-only / blocked 六态） | `docs/data-interface-inventory.md` | D67 `/api/data-capability` 的状态机可扩展为六态，避免"能用"被误读为"全市场稳定" |
| `required_interfaces` / `quality contract` 声明（必填列、日期范围、非负数值、字段映射） | `downloader_profiles.py` | D66/D67 的 `df.attrs` 血缘可补一份字段契约清单 |
| 主机排名缓存 + 结构化 `HostProbeResult(host, ok, latency_ms, error)` | `_tdx_wire/_host_probe.py` | 替换本项目 `TdxSource` 布尔探活，留下可观测延迟/错误 |
| 采集器与接口 ID 分离（interface_id / collector_id / dataset_id 不得混用） | `docs/architecture.md` | 若未来做数据湖（N-09，暂缓）的前置命名纪律 |

---

## 3. 真机实测证据（2026-11-21，生产时段，可复现）

环境：`pip install eltdx==3.3.5` 于 Python 3.13 venv（本地项目 venv 为 py3.9，装不了 wheel，
故用独立 venv 验证；ECS 为 py3.14，在 wheel 支持范围内）。

| 测试 | 结果 | 备注 |
|---|---|---|
| 内置节点 TCP 探测 | **43/43 可达**（12–19ms） | `probe_hosts(FALLBACK_HOSTS)` |
| 协议握手 | **3/3 `pong`** | `116.205.183.150` / `116.205.171.132` / `111.230.186.52` |
| 日K（sz000001） | `2026-11-20 O=11.99 H=12.25 L=11.91 C=12.22` | 6 根，时间升序 |
| 日K（sh600519） | `2026-11-20 O=1450.89 H=1489.09 L=1423.9 C=1485.75` | 6 根 |
| **交叉验证** | 两只票与新浪 getKLineData 的 OHLC **完全一致，收盘价差 0.0** | 数据真实，非幻觉 |
| 分钟K | `1m/5m/15m/30m/60m` 全部 OK（3 根/次，最新 11-20 15:00） | `1m` 是本项目缺口 |
| 当日分时 | `MinuteSeries` 241 点 | OK |
| 当日逐笔 | `TradePage.ticks` 4718 条 | 本项目无 tick 能力 |
| 7709 原生资金流 | `MoneyFlowBlock(date=2026-11-20, buckets=...)` | OK；F10 HTTP `.history()` 503 |
| 实时榜单 | `RealtimeRankTable.rows` 含涨幅/价格 | OK |
| 连板天梯 | `LimitLadderTable(trade_date=2026-11-20)` | 小样本 rows 为空（正常，非涨停票） |
| **对照：本项目 pytdx** | 新旧节点 `get_security_bars` / `get_security_quotes` **均 0 条** | 客户端实现不兼容，非服务器停供 |

### 3.1 对 I-03 裁定的前提订正

> 原裁定（research/docs/31 附近）："74 节点 → TCP 29 → 协议可用 0/29 → 上游公共 TDX 整体停供，
> 不引入 eltdx、不改 TDX_SERVERS。"
>
> **订正**：TCP 层和协议层本次均可用；"协议可用 0"是 pytdx 客户端与当前上游不兼容所致。
> eltdx（Rust 7709 实现）在同批节点上能取到与新浪零误差的真实数据。
> **因此"不引入 eltdx"的理由不再成立，但"不改 TDX_SERVERS"仍然正确**
> （pytdx 换节点也取不到，改列表无意义）。该裁定需用户重新拍板，见 §6。

---

## 4. 结合本项目的接入评估

### 4.1 能力矩阵

| 能力 | 本项目现状 | eltdx 可补 | 是否改信号 |
|---|---|---|---|
| 日K | 腾讯→新浪→同花顺网页→东财→tushare（5 源） | 第 6 源（交叉复核价值 > 主链价值） | 若进主链会改 |
| 5/15/30/60 分钟K | 新浪主路，TDX（pytdx，恒空）备路 | 可用备源 | 若进主链会改 |
| 1 分钟K | 不支持 | **支持 1m** | 新增能力，不改现有 |
| 当日分时 | 腾讯 web.ifzq | 241 点 TDX 分时 | 旁路 |
| 逐笔 tick | 无 | **4718 条/日** | 旁路（新证据） |
| 资金流 | 东财（D68 反封禁敏感） | 7709 原生，避东财 | 旁路 |
| 连板/题材/竞价/封单 | D71 同花顺（默认关，需 Key） | 免费无 Key（但许可证更严） | 旁路证据 |
| 主机健康探测 | D67 能力面板（HTTP 层） | 结构化 TDX 节点延迟/错误 | 旁路可观测 |

### 4.2 零信号影响边界（本轮硬约束）

- 默认 `ELTDX_ENABLED=false` 且未安装 eltdx 包时，适配层所有方法返回空 DataFrame / `None`，
  **零请求、零行为变化**。
- 不自动注入 `_PRIORITY`；不写 `df.attrs`；不改评分、门控、Kelly、风控、回测。
- 只提供显式调用的只读能力（研究脚本 / 手动验证 / 未来裁定后作为可选备源）。

### 4.3 许可证风险（必须让用户知晓）

- eltdx：`ELTDX Non-Commercial Learning and Data Research License`——允许个人研究读写改源码，
  **禁止**商业活动、付费服务、**production service**、转售、**automated trading service**。
- AxData：`AxData Research-Only License`——措辞几乎一致，同样禁止 production / automated trading。
- 本项目虽然定位"个人研究工具"，但在 ECS 上定时扫描并自动推送 BUY/SELL 信号，
  **是否落入 "automated trading service" 属灰区**。本轮不替用户做这个合规判断：
  代码默认关闭，启用前需用户明确裁定。

> **D82 用户裁定（2026-11-24，纪律④就地订正）**：用户明确「非商业项目，不存在许可证风险」，
> 上述"硬约束/灰区/启用前需裁定"的表述**对用户项目不再构成阻碍**——本项目为个人自用、
> 非商业、不转售、不提供付费服务，用户已就自用场景完成合规裁定。本节保留原始风险描述
> 作为事实留档，但裁定结论以本条为准：**许可证不再是启用 eltdx 能力的前提障碍**。
> 且 K 线主路径已由 D82 自研协议核承载（`TDX_WIRE_ENABLED`），是本项目自有代码，
> 从根本上不依赖上游 wheel 及其许可证（见 §7）。

---

## 5. 本轮落地内容

1. `quant_engine/data/sources/eltdx_client.py`：默认关闭的只读适配层（延迟导入 + 优雅降级 +
   代码格式转换 + 输出对齐现有 DataFrame 列）。
2. `config.py`：`ELTDX_ENABLED` / `ELTDX_HOSTS` / `ELTDX_TIMEOUT` / `ELTDX_PROBE_HOSTS`（默认关/空）。
3. `quant_engine/.env.example`：对应开关行。
4. `test/test_eltdx_client_d72.py`：注入式专项测试（无需安装 eltdx，py3.9 可跑），
   覆盖降级、格式转换、列对齐、零信号影响。
5. 本文档 + PRD/README/manifest 状态同步。

**回退**：`ELTDX_ENABLED=false`（默认）或卸载 eltdx 包 → 完全回到改动前行为。

---

## 6. 待用户裁定

| # | 事项 | 建议 |
|---|---|---|
| D73-1 | 是否接受 eltdx/AxData 的 Research-Only 许可证用于本项目 ECS 生产部署 | 若不能确认合规，保持默认关闭，仅作研究数据源。**D82 更新（11-24）**：K 线主路径已自研纯 Python 协议核（`TDX_WIRE_ENABLED`，见 §7），不再依赖 wheel；仅旁路仍用 wheel（用户 11-21 已裁定自用可用） |
| D73-2 | 是否把 eltdx 加为 daily/minute **末位备源**（会改变极端情况下的信号） | 先不加，保持零信号；需要时单独批次 + D41 双跑 |
| D73-3 | 是否把 TDX tick/资金流/连板天梯接为 `market_alert` 旁路证据（类 D71） | 可低成本做，但同上先过许可证裁定 |
| D73-4 | I-03 前提订正后，是否更新调研基线记录 | 已更新 **`research/docs/34`**（I-03 原始基线所在；D73 原稿误写 docs/31，已订正）为"根因订正、eltdx 已启用见 docs/46" |

---

## 7. D82：eltdx 协议核源码级移植（自研纯 Python，2026-11-24）

> 用户指令："研究下 eltdx 代码如何实现，然后移植到我们项目上。"
> 纪律：源码级逐函数分析（已 clone 仓库）+ 冻结向量逐位校验 + ECS 真机抓包 +
> 与 wheel 逐值对照。以下结论全部有实证，不凭印象。

### 7.1 源码级研究结论（为什么不是整体搬 84,499 行）

eltdx v3.3.5 分层（GitHub 源码逐文件核对）：

| 层 | 规模 | 职责 | 移植决策 |
|---|---|---|---|
| `crates/eltdx-protocol` | ~96KB Rust | 7709 帧编解码、varint/压缩浮点、K线/报价/资金流解析 | **核心借鉴**，纯 Python 复刻子集 |
| `crates/eltdx-runtime` | ~670KB Rust | tokio 连接池/心跳/actor/push/重连/测速 | 只借鉴语义（单连接+锁+握手+失败重连），不复刻 actor 框架 |
| `crates/eltdx-python` | PyO3 0.29 | ABI 绑定 | 不需要（自研纯 Python，无二进制） |
| `src/eltdx/api/*` | ~150KB Python | 门面（bars/quotes/minutes/trades/money_flow/helpers） | 只对齐 `.bars.get()` 契约子集 |

**关键发现**：Python 侧 `protocol/frame.py` 与 `transport/socket.py` 都只是 Rust 扩展的
薄壳——`call_native(native_module().encode_request_frame, ...)`。真正的协议在 Rust，
pytdx 之所以恒空而 eltdx 可取数，根因在请求帧与 payload 细节，不在服务器（docs/45 §3.1 已订正）。

### 7.2 协议逆向（冻结向量 + ECS 真机抓包双证）

1. **请求帧 12B 头**：`0x0C` + msg_id<u32 LE> + **control 单字节 0x01** + len<u16>(=data+2) ×2
   + msg_type<u16> + data。
   - **本轮踩坑**：初版误把 control 写成 2 字节（`struct.pack('<BIHHH')`），帧长多 1 字节，
     服务端静默超时（本地沙箱还有 fake-accept 假象：`1.2.3.4:7709` 也显示可达）。
     修正为 `<BIBHH` 后一次成功——冻结向量 `frame.rs:request_frame_matches_the_frozen_python_header`
     与抓包 `0c 21 37 58 a8 01 03 00 03 00 0d 00 01` 同时验证。
2. **响应帧 16B 头**：`b1 cb 74 00` + control + msg_id + reserved + msg_type + zip_length + length
   + payload；`zip_length != length` 时 zlib 解压（冻结向量 `response_decoder_handles_zlib`）。
3. **握手**：`0x000D` + 单字节 `0x01`，响应 payload ≥189B（服务器名/产品名 GBK）。
   这与 pytdx 的 `SetupCmd1/2/3`（三段固定报文）完全不同——是 pytdx 不兼容的重要环节。
4. **K 线请求 0x052D**（40B payload，抓包实证）：market<u16>(沪1/深0/北2) + 6s 代码 +
   period<u16> + param<u16> + start<u16> + count<u16> + adjust<u16> + anchor<u32> + **18×0**。
   - ⚠️ 与 GitHub 源码 `klines.rs` 写的 **20 个尾零（42B）不一致**——线上 wheel 3.3.5 实发 18 零。
     以抓包为准；移植时用 18 零真机取数成功，用 20 零未验证（版本差异，记录在此避免后人踩坑）。
   - 复权：0=none/1=qfq/2=hfq/3=fixed_qfq/4=fixed_hfq；周期 day=4/1、5m=0/1、1m=7/1…
5. **解码**：价格 varint（首字节低 6 位 + 0x40 符号位，后续 7 位续接，千分位），
   成交量/额用 TDX 4 字节压缩浮点；日K时间=yymmdd<u32>，分钟K=packed date + minute-of-day。
   ⚠️ 成交量口径：`volume_lots = get_volume(raw)/100`（wheel 语义，eltdx `convertible_tests` 实证
   `get_volume=2.0 → 0.02 手`）；适配层 `round(lots)*100` 把它还原为 D80 腾讯按手取整口径。
   若 wire 层直接返回股数会被再乘 100 **放大 100 倍**——已写专项回归测试锁死。

### 7.3 移植方案与合规边界

- **实现**：`quant_engine/data/sources/tdx_wire.py`（约 430 行，纯标准库 struct/zlib/socket/
  threading，零第三方依赖），提供 `TdxWireClient.bars.get()` 与 eltdx wheel 接口子集兼容。
- **接入**：`eltdx_client.py` K 线路径优先 wire；`TDX_WIRE_ENABLED=false` 或注入
  `client_factory` 时回退原 wheel 路径。旁路能力（分时/逐笔/资金流/榜单/连板）**仍走 wheel**，
  未安装时维持空（零降级）——避免一次性移植 1600+ 行解析引入风险。
- **许可证**：§0/§4.3 的 Research-Only / 禁止 production 风险，对 K 线主路径**不再依赖 wheel**
  而大幅缓解（自研实现是本项目自有代码）；旁路仍依赖 wheel，合规结论不变（用户 11-21 已裁定自用）。
- **零信号影响**：只输出标准 DataFrame（列/值/D80 成交量口径完全对齐），不写 df.attrs、
  不改评分/门控/Kelly/风控/回测；开关默认 `TDX_WIRE_ENABLED=true`。

### 7.4 实证证据（ECS 2026-11-24）

| 验证 | 结果 |
|---|---|
| 冻结向量帧编解码 | 请求帧/握手帧/响应头/压缩响应全部逐字节一致（15 例专项测试） |
| ECS 真机 wire 取数 | 600519 11-20 O1450.89/H1489.09/L1423.9/**C1485.75**；000001 C**12.22**——与 eltdx wheel 及 docs/45 §3 历史记录逐值一致 |
| 分钟K | 5m 2026-11-23 14:55 C=1440.76、15:00 C=1439.0，正常 |
| 性能 | 单票 8ms（首连含握手），连接复用后更快；无 PyO3/wheel 加载开销 |
| 全量回归 | 48 例专项 + 既有 eltdx 测试全过；ruff 全仓 0；D41 双跑结果见提交记录 |

### 7.5 回退

- `TDX_WIRE_ENABLED=false` → K 线回退 eltdx wheel（需安装）；`ELTDX_ENABLED=false` → 整个 eltdx 链路零请求。
- 无 wheel 环境下 wire 独立可用（本地 py3.9 / ECS py3.14 同一份纯 Python 代码）。

---

## 免责声明

本文仅记录个人学习与非商业研究过程，所有接口能力描述来自公开仓库与真实请求验证；
第三方数据的使用须遵守各数据源条款。本项目全部输出不构成投资建议。
