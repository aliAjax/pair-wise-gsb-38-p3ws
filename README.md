# 影视字幕本地化质检

一个仅使用 Python 标准库实现的字幕翻译、时间轴审核和交付服务。SQLite 保存项目、字幕版本、人员分配、时间点评论、术语表、复核意见和交付快照。

## 运行

```bash
python3 app.py --init
python3 app.py --port 8009
```

打开 <http://127.0.0.1:8009>。`--init` 会创建示例纪录片项目、`zh-CN` 草稿版本和一条术语规则。数据库默认是 `subtitle_qc.db`，可用 `--db` 或 `SUBTITLE_DB` 修改。

## 流程

1. 负责人创建项目、字幕版本和术语规则。
2. 为版本分配 `translator`、`timeline`、`reviewer`。
3. 翻译或时间轴成员保存字幕；每项包含 `expected_revision`，旧页面提交会返回 409。
4. 成员可对具体字幕或毫秒时间点添加评论。
5. 翻译/时间轴成员提交复核，分配的非创建人复核人批准或退回。
6. 负责人锁定已批准版本，锁定记录带内容指纹（字幕+术语的 SHA-256）。
7. 按渠道发起打包批次，每个渠道按自己的规格（`srt`/`vtt`/`ttml`、条数上限）生成独立包。
8. 外部系统回执带清单摘要；缺文件或摘要不符时该渠道拒签。所有启用渠道都签收后整版才显示 `delivered`。
9. 历史 `deliveries` 在启动时升级为遗留渠道记录，未确认前不进交片完成度。

### 交片线规则

- **版本锁定**：`lock` 写入带 `content_hash` 的锁定记录。同一渠道只认当前锁定版本；重新锁定且内容未变时旧包仍可继续签收，内容变化时未签收包标记 `invalidated`。
- **字幕/术语改动**：需先 `unlock`；未签收的渠道包立即失效，**已签收包保留原清单和摘要不动**。术语表直接改动也会让同项目所有未签收渠道包失效。
- **打包批次**：`POST /api/versions/{id}/packages`，可传 `{"channels":[...]}`，缺省为全部启用渠道。两个请求同时打到同一渠道（同一 `version+channel+content_hash`）时先到者占住位置，后来者拿到同一占位包结果，不重复压包（单飞，进程内锁 + 数据库槽位行双保险）。
- **断点续压**：批次内某个渠道压包失败只让该包 `failed`、批次 `partial`；已完成渠道保留，重新提交时复用同一槽位行从缺口继续。
- **外部回执**：`POST /api/packages/{id}/receipts`，回执必须包含 `files`（每项 `name`+`sha256`）和 `manifest_hash`；缺文件、多文件、文件摘要或清单摘要不符返回 422，该渠道不能签收，整版也不能显示已交。回执按包幂等。
- **完成度**：`GET /api/versions/{id}/delivery-state` 返回每渠道状态和 `channels_done/channels_total`；全部渠道签收后版本置为 `delivered`。
- **历史升级**：旧 `deliveries` 快照迁移为 `legacy` 渠道包（保留原清单与哈希、未确认），遗留渠道在确认前不进分母；`POST /api/packages/{id}/confirm-legacy` 确认后才计入完成度。迁移幂等。

字幕保存会验证时长范围、起点小于终点、字幕重叠、序号冲突和术语表。术语表中配置的禁用译法会直接阻止保存；指定译法可用。

## API

所有身份通过 `X-User`、`X-Role` 请求头模拟，角色包括 `owner`、`admin`、`translator`、`reviewer`、`timeline`。

- `POST /api/projects`：创建项目和成片校验信息。
- `POST /api/projects/{id}/versions`：创建目标语言版本，可指定同语言父版本。
- `POST /api/projects/{id}/glossary`：设置指定译法和禁用词（改动会失效未签收渠道包）。
- `POST /api/projects/{id}/channels`：维护渠道规则（`code`、`name`、`subtitle_format=srt|vtt|ttml`、`max_cues`、`active`）。
- `POST /api/versions/{id}/assignments`：分配角色。
- `POST /api/versions/{id}/cues`：新增或修改字幕，要求 `expected_revision`。
- `POST /api/versions/{id}/comments`：按具体时间毫秒或字幕 ID 评论。
- `POST /api/versions/{id}/submit|review|lock|unlock|deliver`：复核与锁定状态机。
- `POST /api/versions/{id}/packages`：按渠道发起打包批次（202，返回每渠道包与占位信息）。
- `POST /api/packages/{id}/receipts`：登记外部系统回执并核对清单摘要。
- `POST /api/packages/{id}/confirm-legacy`：确认迁移生成的遗留渠道记录。
- `GET /api/versions/{id}/cues|comments|packages|batches|delivery-state`、`GET /api/deliveries|packages|batches|receipts`：查看结果。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

测试覆盖完整复核交付流程、锁定覆盖保护、旧修订冲突、时间轴重叠、术语禁用和人员权限，以及交片线：渠道独立包、同渠道并发单飞占位、压包失败断点续压、规格校验单渠道失败、字幕/术语改动失效且已签收包保留、回执缺文件与摘要不符拒签、整版完成度、回执幂等、HTTP 端到端和历史数据升级（确认前后完成度变化、迁移幂等）。
