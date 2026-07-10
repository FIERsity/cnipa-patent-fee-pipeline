# CNIPA Patent Fee Status Pipeline

A research-oriented Python pipeline for collecting public CNIPA legal-status events and inferring whether a Chinese patent may have lapsed because of unpaid annual fees.

## Scope

- Normalizes patent identifiers and public legal events
- Classifies fee-related termination, abandonment, and restoration events
- Builds auditable patent-level outputs and city-level research panels
- Supports public CNIPA web sources, standardized packages, FTP archives, and local year-split archives

The inferred field is a research variable. It is not an official payment record and must not be treated as proof of payment or non-payment.

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_pipeline.py
```

Normalized 12- or 14-digit Chinese application numbers are recommended. Configuration lives in `configs/default.json`, sample inputs in `raw/`, and generated tables in `outputs/`.

## Method and responsible use

The pipeline uses explicit, auditable rules instead of a black-box classifier. Fee-related termination, deemed abandonment, unspecified termination, and restoration remain distinct in the output.

Use only data you are authorized to access. Respect source terms, rate limits, authentication requirements, and personal-data obligations. Never commit cookies, credentials, session files, or proprietary data.

---

## 中文

一个面向研究的 Python 数据管线：采集国家知识产权局公开法律状态事件，并推断中国专利是否可能因未缴年费而终止。

### 主要功能

- 规范申请号与公开法律事件
- 识别未缴年费终止、视为放弃、权利恢复等事件
- 生成可审计的专利级结果与城市面板
- 支持公开网页、标准化数据包、FTP 原始包及本地分年份数据

推断结果仅是研究变量，不等同于官方缴费流水，也不能单独证明真实缴费事实。

### 快速开始

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_pipeline.py
```

建议使用规范化的 12 位或 14 位中国专利申请号。配置位于 `configs/default.json`，样例输入位于 `raw/`，输出位于 `outputs/`。

本项目采用显式、可审计的规则。请遵守数据来源条款、访问频率与个人信息保护要求，勿提交 Cookie、凭据、会话文件或无权公开的数据。
