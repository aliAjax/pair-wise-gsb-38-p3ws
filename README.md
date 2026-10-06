# 影视字幕本地化质检

一个仅使用 Python 标准库实现的字幕翻译、时间轴审核和交片线服务。SQLite 保存项目、字幕版本、人员分配、时间点评论、术语表、复核意见、渠道规则、锁定指纹、渠道包、外部系统回执、遗留渠道记录和交付快照。

## 运行

```bash
python app.py --init
python app.py --port 8009
```

打开 <http://127.0.0.1:8009>。`--init` 会创建示例纪录片项目、`zh-CN` 草稿版本、一条术语规则和 `cinema`/`streaming` 两个渠道。数据库默认是 `subtitle_qc.db`，可用 `--db` 或 `SUBTITLE_DB` 修改。

## 流程

1. 负责人创建项目、字幕版本和术语规则。
2. 为版本分配 `translator`、`timeline`、`reviewer`。
3. 翻译或时间轴成员保存字幕；每项包含 `expected_revision`，旧页面提交会返回 409。
4. 成员可对具体字幕或毫秒时间点添加评论。
5. 翻译/时间轴成员提交复核，分配的非创建人复核人批准或退回。
6. 负责人维护渠道规则（字幕格式、单条字数上限、最短显示时长）并锁定已批准版本。锁定时生成字幕与术语两份 SHA-256 指纹。
7. 按渠道压包：`POST /api/versions/{id}/pack-batch` 或 `.../pack/{channel}`。每个渠道生成独立包，包内包含按渠道格式渲染的字幕和带逐文件 SHA-256 的清单及清单摘要。
8. 外部系统通过 `POST /api/packages/{id}/receipt` 回执；缺文件、文件 SHA-256 或清单摘要不符都会拒绝签收。全部渠道签收后整版 `complete=true`，此时 `deliver` 才允许落最终快照。

## 交片线规则

- **版本锁定**：锁定指纹包含字幕和术语表；同一语言只保留一个当前锁定版本。渠道压包只接受当前锁定版本。
- **独立包与去重**：每个渠道在每个锁定版本上只有一个包位（`(lock_id, channel_id)` 唯一）。并发压包请求在一个事务里先到者插入包位，后来者直接返回该包（`deduped=true`），不会重复压包。
- **失效语义**：
  - 术语表改动：当前锁定版本上所有未签收包变为 `invalidated`；已签收包保留原清单不动，重新压包后才可以再签收。
  - 同语言新版本锁定：旧锁上未签收包变为 `superseded`；已签收包留原清单。旧锁不再接受压包。
  - 锁定后字幕不可直接修改（沿用草稿/复核状态机），内容修订走新版本重新锁定。
- **批次与续压**：整批压包在第一个规格不符的渠道停下并把该包标为 `failed`，已完成渠道保留；修复后再次调用批次即从缺口继续，包位不变、`attempt` 递增。
- **回执核验**：回执必须带 `manifest_digest` 和每个文件的 SHA-256。任何不一致都会落一条被拒 `receipts` 记录且包不签收；整版完成度只统计 `signed` 的常规渠道包。
- **遗留数据升级**：启动时对既有 `deliveries` 快照幂等补录 `legacy` 渠道、历史锁和遗留包，`confirmed=0`，不进交片完成度；负责人用 `POST /api/packages/{id}/confirm-legacy` 核对原快照摘要后确认，确认后才计入该历史版本完成度。

字幕保存会验证时长范围、起点小于终点、字幕重叠、序号冲突和术语表。术语表中配置的禁用译法会直接阻止保存；指定译法可用。渠道压包按渠道规格再验一次单条字数和最短显示时长，并按 `srt`/`vtt`/`ass` 渲染成品。

## API

所有身份通过 `X-User`、`X-Role` 请求头模拟，角色包括 `owner`、`admin`、`translator`、`reviewer`、`timeline`。

- `POST /api/projects`：创建项目和成片校验信息。
- `POST /api/projects/{id}/versions`：创建目标语言版本，可指定同语言父版本。
- `POST /api/projects/{id}/glossary`：设置指定译法和禁用词。
- `POST /api/projects/{id}/channels`：维护渠道规则（`format` 为 `srt`/`vtt`/`ass`）。
- `GET /api/channels`、`GET /api/projects/{id}/channels`：查看渠道。
- `POST /api/versions/{id}/assignments`：分配角色。
- `POST /api/versions/{id}/cues`：新增或修改字幕，要求 `expected_revision`。
- `POST /api/versions/{id}/comments`：按具体时间毫秒或字幕 ID 评论。
- `POST /api/versions/{id}/submit|review|lock`：完成审核锁定状态机。
- `POST /api/versions/{id}/pack/{channel}`：单渠道压包（并发去重、失败续压）。
- `POST /api/versions/{id}/pack-batch`：整批压包，失败处停下、保留已完成渠道。
- `POST /api/packages/{id}/receipt`：外部系统回执，带 `manifest_digest` 和 `files` 摘要映射。
- `POST /api/packages/{id}/confirm-legacy`：确认历史升级补出的遗留渠道记录。
- `GET /api/versions/{id}/packages`、`GET /api/packages`、`GET /api/packages/{id}`：查看包。
- `GET /api/versions/{id}/delivery-status`、`GET /api/projects/{id}/delivery-status`：整版/整项目完成度。
- `POST /api/versions/{id}/deliver`：全部渠道签收后落最终交付快照。
- `GET /api/versions/{id}/cues|comments`、`GET /api/deliveries`：查看结果。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整复核交付流程、锁定覆盖保护、旧修订冲突、时间轴重叠、术语禁用和人员权限；交片线部分覆盖并发占位去重、规格失败续压、术语失效与已签收清单保留、新锁取代旧包、回执缺文件/摘要不符、整版签收门槛和历史数据遗留补录确认。
