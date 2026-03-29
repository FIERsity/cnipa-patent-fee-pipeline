# CNIPA 专利法律状态抓取与年费终止识别

这是一个面向研究用途的最小可运行 pipeline，用于从国家知识产权局公开网页抓取专利的法律状态/事务数据，并基于公开法律事件推断“是否可能持续缴费”。

## 项目目的

- 抓取公开法律状态/事务事件
- 标准化事件类别
- 基于规则推断年费缴纳连续性
- 导出结构化 CSV，便于后续统计分析

## 重要声明

本项目得到的是“基于公开法律状态事件的年费持续性推断”，不是国家知识产权局内部缴费明细。

也就是说：

- 这里只能观察公开网页中的法律状态/事务事件
- 无法读取真实缴费流水、缴费金额、缴费主体内部记录
- `inferred_fee_status` 只是研究型变量，不等同于真实缴费事实

## 数据来源范围

当前最小版优先使用国家知识产权局公开站点：

- `http://epub.cnipa.gov.cn/`
- `http://epub.cnipa.gov.cn/SW`
- `http://epub.cnipa.gov.cn/SwListQuery`

另外，已经补上国家知识产权局知识产权数据资源公共服务系统的公开目录路线：

- `https://ipdps.cnipa.gov.cn/`
- 公开目录里可以直接看到中国专利法律状态标准化数据：
  - `CN-PA-PRSS-10` 中国发明专利法律状态标准化数据
  - `CN-PA-PRSS-20` 中国实用新型专利法律状态标准化数据
  - `CN-PA-PRSS-30` 中国外观专利法律状态标准化数据
- 公开样例包可直接下载并解析，样例里已经能识别到：
  - `未缴年费专利权终止`
  - `专利权的终止`
  - `视为放弃 / 视为撤回`

这条路线比纯网页事务查询更适合批量、可审计地拿标准化事件数据。

另外，仓库还接入了一份更适合做全历史主表的年份压缩包：

- `raw/分年份保存数据.rar`

它是按年份保存的中国专利数据库，里面已经包含：

- `专利名称`
- `申请人`
- `申请人地址`
- `申请人城市`
- `申请人区县`
- `申请号`
- `申请日`
- `申请年份`
- `公开公告号`
- `公开公告日`
- `授权公告号`
- `授权公告日`

这条数据源更适合作为城市面板主表，因为它自带城市和地址字段，能更直接和年费终止记录对齐。


更进一步，`ipdps.cnipa.gov.cn` 背后还挂着 FTP 原始数据镜像。这个仓库已经实测可用：

- `ftp1.ipdps.cnipa.gov.cn`
- `ftp2.ipdps.cnipa.gov.cn`
- `ftp3.ipdps.cnipa.gov.cn`
- `ftp4.ipdps.cnipa.gov.cn`

登录后可以看到按日期分片的原始包，例如：

- `CN-PA-BIBS-ABSS-10-A` 专利著录项目数据
- `CN-PA-PRSS-10` 专利法律状态数据
- `CN-PA-TXTS-10-A` 专利全文文本数据

原始包里有 `rawdata` 目录，里面是按日期发布的 ZIP 分片和索引 XML/TXT。这条路线适合批量回填主表。

其中 `SW` 页面是公开的事务数据查询入口。脚本会优先尝试浏览器自动化；如果公开页面或会话不可用，会记录失败原因并保留 HTML 快照。

## 输入类型与限制

输入文件格式见 [`raw/sample_patent_ids.csv`](raw/sample_patent_ids.csv)。

当前最稳妥的输入类型是：

- `application_no`

兼容性说明：

- `application_no`：支持最好，推荐使用 12 位或 14 位中国申请号
- `publication_no`：最小版仅做有限兼容，可能需要后续补强到公布公告查询
- `patent_no`：同上，属于 best-effort

注意：

- 中国申请号常见 12 位或 14 位
- 13 位场景需要按 CNIPA 规则做归一化
- 当前最小版会做基础清洗，但不会替你猜所有历史格式

## 事件分类规则

代码里是规则优先，不是黑盒 NLP。

显式规则包括：

- 包含 `未缴年费终止` -> `annual_fee_nonpayment_termination`
- 包含 `专利权终止` 且上下文涉及年费 -> `annual_fee_nonpayment_termination`
- 包含 `恢复权利` -> `right_restoration`
- 包含 `视为放弃` -> `deemed_abandoned`
- 仅出现 `专利权的终止` / `专利权终止`，但没有年费上下文 -> `termination_unspecified`
- 其他 -> `other`

事件推断逻辑：

- 出现 `annual_fee_nonpayment_termination` -> `likely_stopped_payment_due_to_fee_nonpayment`
- 出现 `deemed_abandoned` -> `deemed_abandoned`
- 先终止后恢复 -> `restored_after_lapse`
- 没抓到法律事件 -> `no_legal_event_found`
- 信息不足 -> `ambiguous`



### 从 PatentStar 批量回查法律状态

在 `https://cprs.patentstar.com.cn/Search/ListSearch` 登录后，可用号单检索按申请号批量回查当前状态与法律状态详情。该路线适合做“未缴年费终止 / 视为放弃 / 失效”类低质量专利剔除。

现在仓库里同时提供了一个更快的纯 HTTP 版本，它不再打开详情页，也不再依赖浏览器 tab：

- [`scripts/fetch_patentstar_legal_status_http.py`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/scripts/fetch_patentstar_legal_status_http.py)

这个版本需要先导出一次已登录会话的 storage state：

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
print(Path('output/playwright/patentstar_state.json').as_posix())
PY
```

或直接复用当前已保存的：

- [`output/playwright/patentstar_state.json`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/output/playwright/patentstar_state.json)

建议默认优先使用纯 HTTP 版：

```bash
.venv/bin/python scripts/fetch_patentstar_legal_status_http.py \
  --input raw/patentstar_sample_ids.csv \
  --output outputs/patentstar_legal_events_http.csv \
  --log logs/fetch_patentstar_legal_status_http.log \
  --state output/playwright/patentstar_state.json \
  --detail-workers 2
```

说明：

- `--detail-workers` 控制 `GetFLZT` 并发数，默认 `2`，建议先从 `2` 跑起，确认稳定后再尝试 `4`。
- 纯 HTTP 版不打开详情页，不再创建新的浏览器 tab。
- `SearchByQuery` 先批量拿结果，再只对 `LG=2` 的失效专利抓法律状态，速度比浏览器详情页版快很多。

最小样例：

```bash
.venv/bin/python scripts/fetch_patentstar_legal_status.py \
  --input raw/patentstar_sample_ids.csv \
  --output outputs/patentstar_legal_events.csv \
  --log logs/fetch_patentstar_legal_status.log \
  --session patentstar_login
```

纯 HTTP 版样例：

```bash
.venv/bin/python scripts/fetch_patentstar_legal_status_http.py \
  --input raw/patentstar_sample_ids.csv \
  --output outputs/patentstar_legal_events_http.csv \
  --log logs/fetch_patentstar_legal_status_http.log \
  --state output/playwright/patentstar_state.json
```

说明：

- 当前版本优先抓取结果页可见行，并对当前状态包含 `失效` / `终止` / `放弃` / `恢复` 的专利进一步进入详情页抓法律状态表。
- 批量号单检索支持每批最多 3000 个申请号；脚本默认按批拆分。
- 输入最好先规范成申请号，形如 `CN201610158350.X`。
- 如果会话未登录，请先在浏览器里登录一次，再重跑同一 `--session` 名称。
- 纯 HTTP 版默认只对当前状态为 `失效` 的专利回查法律状态，可以显著减少请求量；如果你要全部回查，可以加 `--all-details`。
- 纯 HTTP 版会用 `SearchByQuery` 先批量拿到 `LG`（`有效 / 失效 / 审中`），再直接 POST `/WebService/GetFLZT`，不需要打开详情页。

## 目录结构

```text
raw/
  sample_patent_ids.csv
  patent_master_template.csv
  reference/prefecture_level_cities.csv
  html_snapshots/
outputs/
  patent_legal_events.csv
  patent_fee_inference.csv
  city_patent_panel.csv
scripts/
  fetch_cnipa_legal_status.py
  fetch_cnipa_ftp_master.py
  parse_legal_events.py
  infer_fee_status.py
  run_pipeline.py
  build_prefecture_city_master.py
  build_city_patent_panel.py
configs/
  default.json
logs/
  run.log
```

## 地级市主数据

仓库里已经生成了地级市主数据：

- [`raw/reference/prefecture_level_cities.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/raw/reference/prefecture_level_cities.csv)

字段包括：

- `year`
- `admin_level`
- `city_type`
- `province_name`
- `province_adcode`
- `city_name`
- `city_short_name`
- `city_adcode`

当前文件包含 333 条地级层级记录。数据是按最新可用年度生成的标准化维表，适合和专利数据做地区聚合。

城市匹配规则是显式规则，不是黑盒：

- 如果主表里已经有 `city_name` 或 `city_adcode`，优先直接合并
- 如果没有显式城市字段，脚本会尝试从 `applicant`、`applicant_address`、`title` 里按地级市名称做字符串匹配
- 没有匹配到的记录会保留为空，不会强行猜

如果你要重建：

```bash
python scripts/build_prefecture_city_master.py --output raw/reference/prefecture_level_cities.csv
```

## 生成地级市专利面板

如果你已经有一份专利主表，且其中至少包含：

- `input_id`
- `year`
- `city_name` 或 `city_adcode`

就可以直接和年费推断结果拼成面板。

如果你手头的是官方数据资源公共服务系统导出的法律状态推断结果，直接把 `--fees` 指向：

- `outputs/cnipa_public_fee_inference_all_samples.csv`
- `outputs/patent_fee_inference_ftp_demo.csv`

即可。

### RAR 全量主线的一键跑法

如果你要直接用 `raw/分年份保存数据.rar` 跑主表和城市面板，可以用：

```bash
.venv/bin/python scripts/run_rar_pipeline.py \
  --archive raw/分年份保存数据.rar \
  --cities raw/reference/prefecture_level_cities.csv \
  --fees outputs/patent_fee_inference_ftp_full.csv \
  --master-output outputs/patent_master_rar_full.csv \
  --panel-output outputs/city_patent_panel_rar_full.csv \
  --unmatched-fee-output outputs/patent_fee_unmatched_to_rar_full.csv
```

可选参数：

- `--year 2024`：只跑某一年
- `--year 1990 --year 2016`：跑多个年份
- 不传 `--year`：跑全量年份

面板脚本会输出：

- `patent_count`
- `fee_nonpayment_termination_patent_count`
- `restoration_patent_count`
- `unspecified_termination_patent_count`
- `fee_nonpayment_excluded_patent_count`
- `unspecified_termination_excluded_patent_count`
- `restoration_excluded_patent_count`
- `excluded_patent_count`
- `kept_patent_count`
- 各类占比

其中 `patent_count` 是按 `input_id` 去重后的专利数，不会因为同一专利有多条法律事件而重复计数。

运行示例：

```bash
python scripts/build_city_patent_panel.py \
  --patents outputs/patent_master_ftp_demo.csv \
  --fees outputs/patent_fee_inference_ftp_demo.csv \
  --cities raw/reference/prefecture_level_cities.csv \
  --output outputs/city_patent_panel.csv
```

如果你想把没有专利的城市-年份也补成 0，可以加 `--fill-zeros`。

模板文件：

- [`raw/patent_master_template.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/raw/patent_master_template.csv)

研究上建议优先使用：

- `kept_patent_count` 作为剔除未缴费终止后的面板专利数
- `fee_nonpayment_termination_patent_count` 作为明确的未缴年费终止计数
- `unspecified_termination_patent_count` 作为保守排除项

如果你的核心问题是“因未缴费终止的专利数”，优先看：

- `fee_nonpayment_termination_patent_count`

样例面板也已经落盘：

- [`outputs/city_patent_panel_pss_demo.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/city_patent_panel_pss_demo.csv)

它展示了如何从 `pss-system` 主表样例中，通过申请人名称规则推断地级市并生成 city-year 面板。
- `fee_nonpayment_excluded_patent_count`

## 年份压缩包主线

如果你的目标是尽可能完整地做地级市面板，优先用年份压缩包这条路线：

1. 从 `raw/分年份保存数据.rar` 流式读取年度专利主表
2. 用 `申请人城市`、`申请人区县`、`申请人地址`、`申请人` 推断地级市
3. 和法律状态推断表按 `input_id` 合并
4. 直接输出 city-year 面板，并单列 `因未缴年费终止` 的剔除数

对应脚本：

- [`scripts/build_patent_master_from_rar.py`](scripts/build_patent_master_from_rar.py)
- [`scripts/build_city_patent_panel_from_rar.py`](scripts/build_city_patent_panel_from_rar.py)

运行示例：

```bash
python scripts/build_city_patent_panel_from_rar.py \
  --archive raw/分年份保存数据.rar \
  --cities raw/reference/prefecture_level_cities.csv \
  --fees outputs/patent_fee_inference_ftp_full.csv \
  --output outputs/city_patent_panel_rar_full.csv \
  --unmatched-fees outputs/patent_fee_unmatched_to_rar_master.csv
```

如果你只想先验证某一年，可以加：

```bash
python scripts/build_city_patent_panel_from_rar.py \
  --archive raw/分年份保存数据.rar \
  --cities raw/reference/prefecture_level_cities.csv \
  --fees outputs/patent_fee_inference_ftp_full.csv \
  --output outputs/city_patent_panel_rar_2024.csv \
  --unmatched-fees outputs/patent_fee_unmatched_to_rar_2024.csv \
  --year 2024
```

这条路线的优势是：

- 主表自带城市信息，减少“主表和城市面板对不上”的问题
- 不需要先把巨型主表完整落盘，能直接流式生成面板
- 仍然保留每个年份分片的来源文件，便于审计和复核

研究上建议优先使用：

- `fee_nonpayment_termination_patent_count` 作为明确的未缴年费终止计数
- `kept_patent_count` 作为剔除后保留的专利数
- `excluded_patent_count` 作为需要稳健性处理的剔除总数

在当前实现里，这两个字段是同一口径，都是明确被判定为“未缴年费终止”的专利数。

## 官方检索系统主线

仓库里还增加了国家知识产权局专利检索及分析系统 `pss-system.cponline.cnipa.gov.cn` 的浏览器会话抓取脚本：

- [`scripts/fetch_cnipa_pss_master.py`](scripts/fetch_cnipa_pss_master.py)

它会复用已经登录的会话，抓取检索结果页里的主表字段，例如：

- `申请号`
- `公开号`
- `申请日`
- `公开日`
- `发明名称`
- `申请人`

当前已经确认：

- 结果页可以稳定拿到检索结果
- 法律状态和结果分页可以通过页面内部接口读取
- 但结果摘要页目前没有直接暴露稳定的 `申请人地址 / 城市` 字段

检索式约束：

- 官方检索框对 `=` 比较敏感，很多场景会直接报错
- 日期范围更稳妥的写法是用 `>` / `<`
- 例如：`申请日>2024-01-01 and 申请日<2025-01-01`

这意味着：

- 它足够作为更完整的专利主表抓取线
- 但城市面板还需要后续做地址补全，或者继续找带城市字段的官方源

### 官方批量审查信息查询平台

为了更完整地抓取“未缴年费终止失效 / 未缴年费专利权终止，等恢复”这类低质量状态，仓库现在还新增了一条更贴近官方审查状态的回查脚本：

- [`scripts/fetch_cnipa_cpquery_status.py`](scripts/fetch_cnipa_cpquery_status.py)
- [`scripts/fetch_cnipa_cpquery_status_cli.py`](scripts/fetch_cnipa_cpquery_status_cli.py)

这条脚本面向国家知识产权局官方批量审查/案件状态查询入口，重点按申请号回查当前案件状态。

官方公开材料显示，这条路线支持：

- 按申请号 / 专利号回查
- 查看当前案件状态
- 批量导出申请人检索结果
- 区分 `未缴年费终止失效` 与 `未缴年费专利权终止，等恢复`

脚本输出的状态分类会把这些结果拆成更适合回归口径的几类：

- `annual_fee_nonpayment_termination_final`
- `annual_fee_nonpayment_termination_restorable`
- `deemed_abandoned`
- `right_restoration`
- `termination_unspecified`
- `other`

运行时如果页面跳转到统一身份认证平台，说明还缺少有效登录态。此时可以：

- 在已打开的 headed 浏览器里手动完成一次登录
- 或先保存登录态，再用 `--state` 复用

如果页面直接返回空白或 412/400，脚本会把 `parse_status` 记为 `login_required_or_blocked`，这通常意味着当前会话还没有拿到官方查询入口所需的身份态或访问条件。

示例：

```bash
python scripts/fetch_cnipa_cpquery_status.py \
  --input raw/sample_patent_ids.csv \
  --output outputs/patent_cpquery_status.csv \
  --headed \
  --wait-for-login-seconds 120
```

如果你已经登录并想复用同一个会话，优先用 CLI 版。CLI 版现在默认**无头运行**，先走官方 JSON 接口，再在必要时回退到页面解析，速度明显快于逐页点击，而且不会每次弹出浏览器窗口。只有显式加 `--headed` 才会开可见窗口：

```bash
python scripts/fetch_cnipa_cpquery_status_cli.py \
  --input raw/sample_patent_ids.csv \
  --output outputs/patent_cpquery_status.csv \
  --session cpquery_batch_test \
  --mode api
```

如果要并发 4 路跑分片，也可以直接：

```bash
python scripts/run_cpquery_parallel.py \
  --input outputs/cpquery_low_quality_candidates.csv \
  --output outputs/patent_cpquery_status_low_quality.csv \
  --shards 4 \
  --mode api
```

如果你已经有可复用的登录态，也可以直接指定：

```bash
python scripts/fetch_cnipa_cpquery_status.py \
  --input raw/sample_patent_ids.csv \
  --output outputs/patent_cpquery_status.csv \
  --state output/playwright/cpquery.state.json
```

## 已验证样例

当前仓库里已经落了以下可复核样例：

- [`outputs/patent_master_ftp_demo.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_master_ftp_demo.csv)
- [`outputs/patent_legal_events_ftp_demo.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_legal_events_ftp_demo.csv)
- [`outputs/patent_fee_inference_ftp_demo.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_fee_inference_ftp_demo.csv)
- [`outputs/city_patent_panel_ftp_demo.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/city_patent_panel_ftp_demo.csv)

这些样例来自 FTP 原始分片的真数据，不是手工模拟。

## 全量输出

当前全量抓取已经生成：

- [`outputs/patent_master_ftp_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_master_ftp_full.csv)
- [`outputs/patent_legal_events_ftp_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_legal_events_ftp_full.csv)
- [`outputs/patent_fee_inference_ftp_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_fee_inference_ftp_full.csv)
- [`outputs/city_patent_panel_ftp_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/city_patent_panel_ftp_full.csv)
- [`outputs/city_patent_panel_rar_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/city_patent_panel_rar_full.csv)
- [`outputs/patent_fee_unmatched_to_rar_full.csv`](/Volumes/DataHub/Dev/cnipa-patent-status-pipeline/outputs/patent_fee_unmatched_to_rar_full.csv)

全量规模：

- `patent_master_ftp_full.csv`: `82,962` 条专利主表记录
- `patent_legal_events_ftp_full.csv`: `442,847` 条官方法律状态/法律事件记录，已并入 `20260327` 的 PRSS 最新批次
- `patent_fee_inference_ftp_full.csv`: `440,223` 条专利级年费推断记录
- `city_patent_panel_ftp_full.csv`: `914` 条地级市-年份面板行
- `city_patent_panel_rar_full.csv`: `13,817` 条地级市-年份面板行
- `patent_fee_unmatched_to_rar_full.csv`: `201,229` 条未匹配到 archive 主表的 fee 推断记录

全量 FTP 抓取支持两个实用参数：

- `--workers 4`：按 ZIP 并发下载/解析，加快大包抓取
- `--package-dir`：在已知 CNIPA 包目录时跳过目录发现

## 运行方式

先激活虚拟环境：

```bash
source .venv/bin/activate
```

### 单样本测试

```bash
python scripts/run_pipeline.py --input raw/sample_patent_ids.csv
```

### 只跑抓取

```bash
python scripts/fetch_cnipa_legal_status.py --input raw/sample_patent_ids.csv --output outputs/patent_legal_events.csv
```

### 从官方数据资源公共服务系统抓取公开法律状态样例

```bash
python scripts/fetch_cnipa_public_legal_status.py \
  --output outputs/cnipa_public_legal_status_events_all_samples.csv
```

默认会下载并解析这三份样例包：

- `CN-PA-PRSS-10`
- `CN-PA-PRSS-20`
- `CN-PA-PRSS-30`

如果只想先跑一份样例：

```bash
python scripts/fetch_cnipa_public_legal_status.py --data-no CN-PA-PRSS-10
```

### 从 FTP 原始分片抓取主表和法律状态

这个脚本会优先复用你本机 Chrome 里保存的 `ipdps.cnipa.gov.cn` 登录信息；也可以用环境变量：

- `CNIPA_FTP_USER`
- `CNIPA_FTP_PASS`

示例：

```bash
python scripts/fetch_cnipa_ftp_master.py \
  --date 20260324 \
  --limit-dates 1 \
  --max-zips 1 \
  --output-master outputs/patent_master_ftp_demo.csv \
  --output-legal outputs/patent_legal_events_ftp_demo.csv
```

如果要做更大的历史回填，可以先只指定一个数据包类型：

```bash
python scripts/fetch_cnipa_ftp_master.py --data-no CN-PA-BIBS-ABSS-10-A --limit-dates 1
python scripts/fetch_cnipa_ftp_master.py --data-no CN-PA-PRSS-10 --limit-dates 1
```

### 只做事件标准化

```bash
python scripts/parse_legal_events.py --input outputs/patent_legal_events.csv --output outputs/patent_legal_events.csv
```

### 只做年费推断

```bash
python scripts/infer_fee_status.py --input outputs/patent_legal_events.csv --output outputs/patent_fee_inference.csv
```

### 重新跑失败样本

把失败样本单独放到一个 CSV，再重复上面的 `run_pipeline.py` 即可。

## 输出文件

### `outputs/patent_legal_events.csv`

字段：

- `input_id`
- `input_id_type`
- `matched_patent_id`
- `title`
- `applicant`
- `event_date`
- `event_name_raw`
- `event_text_raw`
- `event_category`
- `source_url`
- `crawl_time`
- `parse_status`
- `notes`

说明：

- `title`、`applicant` 是预留字段，最小版先保留结构，后续可继续从公布公告页补全
- `parse_status` 会标记 `ok`、`no_legal_event_found`、`fetch_failed`

### `outputs/patent_cpquery_status.csv`

这是从国家知识产权局官方批量审查/案件状态查询入口按申请号回查得到的当前案件状态结果。

字段：

- `input_id`
- `input_id_type`
- `matched_patent_id`
- `title`
- `applicant`
- `application_date`
- `current_case_status_raw`
- `current_case_status_category`
- `event_name_raw`
- `event_text_raw`
- `event_category`
- `source_url`
- `crawl_time`
- `parse_status`
- `notes`

其中 `current_case_status_category` / `event_category` 的主要取值包括：

- `annual_fee_nonpayment_termination_final`
- `annual_fee_nonpayment_termination_restorable`
- `deemed_abandoned`
- `right_restoration`
- `termination_unspecified`
- `other`

研究上建议把前两类作为“明确可剔除的年费低质量专利”核心口径。

把这份结果继续送进年费推断脚本，就能得到和现有面板兼容的剔除口径：

```bash
python scripts/infer_fee_status.py \
  --input outputs/patent_cpquery_status.csv \
  --output outputs/patent_fee_inference_cpquery.csv
```

### `outputs/patent_fee_inference.csv`

字段：

- `input_id`
- `has_annual_fee_termination_event`
- `annual_fee_termination_date`
- `has_annual_fee_termination_final_event`
- `has_annual_fee_termination_restorable_event`
- `has_right_restoration_event`
- `restoration_date`
- `inferred_fee_status`
- `inferred_fee_status_rule`
- `confidence_level`
- `notes`

`inferred_fee_status` 的取值：

- `likely_continued_payment`
- `likely_stopped_payment_due_to_fee_nonpayment`
- `deemed_abandoned`
- `ambiguous`
- `restored_after_lapse`
- `no_legal_event_found`

如果你要做地级市面板，优先看年份压缩包主线产出的：

- `outputs/city_patent_panel_rar_full.csv`
- `fee_nonpayment_termination_patent_count`
- `deemed_abandoned_patent_count`
- `kept_patent_count`
- `excluded_patent_count`

补充字段：

- `panel_exclusion_recommendation`
- `panel_share_deemed_abandoned`
- `deemed_abandoned_excluded_patent_count`

建议口径：

- `exclude`：有明确终止、恢复或终止原因不明的事件，建议从“持续缴费”口径里剔除或单独做敏感性分析
- `keep`：当前未见足够法律事件，可暂保留

### `outputs/cnipa_public_legal_status_events_all_samples.csv`

这是从公开数据资源公共服务系统下载的中国专利法律状态标准化样例包解析结果。

它更接近标准化原始数据，适合：

- 批量验证规则
- 做事件分布统计
- 继续扩展为更大规模的官方数据抓取管道

注意：

- 这仍然是公开目录里的样例包，不等于国家知识产权局内部全量缴费明细
- 对研究上最关键的“因未缴费终止”识别，它已经能提供直接事件文本

## 反爬与稳定性约束

代码中已包含：

- 请求限速
- 随机等待
- 重试
- User-Agent 管理
- 错误日志
- 单样本失败不终止全批次
- HTML 快照保存到 `raw/html_snapshots/`

如果遇到：

- 验证码
- 站点 400/412/502
- 页面结构变化

脚本会优雅降级并记录失败原因，不会尝试绕过权限或验证码。

## 常见失败原因

- 公开网页前端变更
- CNIPA 对自动化访问做了限制
- 某些号码格式未按 CNIPA 规则归一化
- `publication_no` / `patent_no` 在最小版中兼容性有限
- 站点资源加载失败导致页面不完整

## 研究局限性

- 只能基于公开法律事件推断年费持续性
- 不能还原真实缴费流水
- `专利权终止` 不一定总能直接等价于“未缴年费”，需要事件上下文
- `恢复权利` 只能说明曾经发生过恢复，不代表此前没有断缴

## 当前样例

仓库里已提供：

- `raw/sample_patent_ids.csv`
- `outputs/patent_legal_events.csv`
- `outputs/patent_fee_inference.csv`

这些样例用于展示输出格式和规则推断结果。

## 核心流程

当前最核心、最稳定的研究链路是：

1. 用 `raw/分年份保存数据.rar` 生成全量主表 `outputs/patent_master_rar_full.csv`
2. 用官方查询或 `PatentStar` 路线抓取法律状态/年费终止事件
3. 用 `scripts/infer_fee_status.py` 识别：
   - `annual_fee_nonpayment_termination`
   - `deemed_abandoned`
   - `termination_unspecified`
   - `right_restoration`
4. 在城市-年份面板里剔除低质量专利，保留 `kept_patent_count`

推荐复跑命令：

```bash
.venv/bin/python scripts/build_patent_master_from_rar.py \
  --archive raw/分年份保存数据.rar \
  --cities raw/reference/prefecture_level_cities.csv \
  --output outputs/patent_master_rar_full.csv

.venv/bin/python scripts/fetch_patentstar_legal_status_http.py \
  --input raw/patentstar_3000_probe.csv \
  --output outputs/patentstar_3000_probe_events_paged.csv \
  --state output/playwright/patentstar_state.json \
  --batch-size 3000 \
  --detail-workers 2
```

## 归档与收尾

为了减少目录噪音，新增了：

- `scripts/archive_workspace.py`

它会把 demo、sample、tmp、probe、前缀/尾段等中间产物移动到：

- `archive/YYYY-MM-DD/`

保留的核心输出默认是：

- `outputs/patent_master_rar_full.csv`
- `outputs/patent_fee_inference_ftp_full.csv`
- `outputs/patent_legal_events_ftp_full.csv`
- `outputs/patent_fee_unmatched_to_rar_full.csv`
