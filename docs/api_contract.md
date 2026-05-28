# Web API 合约

当前 API 是 React Web 前端的最小纵切合约原型。它使用 Python 标准库 HTTP server，不引入新依赖，不导入 Streamlit，不导入根目录 `app.py`，也不接真实算法。

后端字段保持 `snake_case`。前端后续通过 adapter 转成 `camelCase`，例如 `resume_name` 转 `resumeName`、`match_score` 转 `matchScore`、`suggested_actions` 转 `suggestedActions`。

## 前后端字段命名边界

后端 API 允许继续使用 `snake_case` 字段。前端页面和组件统一使用 `camelCase` 字段。

字段转换集中在 `web/src/api/adapters.ts`。页面不得直接读取后端原始字段，也不得直接消费 `Raw` 类型。

未来接入真实 API 时，响应数据应先进入 adapter，再进入页面状态或 store。角色判断结果由后端或 mock 提供，adapter 不新增专业判断、不生成简历改写内容、不替用户改变投递状态。

## Active API

本轮实际实现并纳入 smoke test 的接口：

| Method | Endpoint | 用途 |
| --- | --- | --- |
| GET | `/api/health` | API 健康检查 |
| GET | `/api/bootstrap` | React 启动聚合数据 |
| GET | `/api/me` | 当前用户摘要 |
| GET | `/api/workspaces` | 五个 Web 模块与二级模式 |
| GET | `/api/settings/summary` | 设置页摘要状态 |
| GET | `/api/jobs/ranked` | 岗位列表占位 |
| POST | `/api/jd/analyze` | 单条 JD 分析占位 |

成功返回：

```json
{"ok": true, "data": {}}
```

错误返回：

```json
{"ok": false, "error": {"message": "Unknown endpoint"}}
```

`POST /api/jd/analyze` 请求体：

```json
{"text": "这是一个通用示例岗位描述，用于测试 JD 分析接口。"}
```

返回字段包括：

- `summary`
- `input_length`
- `requirements`
- `keywords`
- `risks`
- `suggested_actions`
- `recommended_job`
- `scores`

## Planned API

以下接口只作为后续规划，本轮不要求代码实现，也不纳入 smoke test：

| Method | Endpoint | 规划用途 |
| --- | --- | --- |
| GET | `/api/profile` | 个人档案详情 |
| GET | `/api/resumes` | 简历列表 |
| GET | `/api/preferences` | 求职偏好 |
| POST | `/api/jd/batch-screen` | 批量 JD 筛选 |
| GET | `/api/jd/trends` | 招聘趋势 |
| POST | `/api/resume/parse` | 简历解析 |
| POST | `/api/resume/match` | 简历与岗位匹配 |
| POST | `/api/resume/rewrite` | 目标 JD 改简历 |
| POST | `/api/resume/gap` | 能力缺口分析 |
| POST | `/api/jobs/evaluate` | 机会评估 |
| POST | `/api/jobs/compare` | 岗位比较 |
| GET | `/api/applications` | 投递管理 |
| GET | `/api/interviews` | 面试记录列表 |
| POST | `/api/interviews/record` | 新增面试记录 |
| POST | `/api/interviews/report` | 面试复盘报告 |
| GET | `/api/reports/dashboard` | 数据看板 |

## 启动

API:

```powershell
.\.venv\Scripts\python.exe -m careerpilot_api.app
```

React:

```powershell
cd web
npm run dev
```

React 默认请求 `http://127.0.0.1:8765`，可用 `VITE_API_BASE_URL` 覆盖。API 未启动时，前端应使用 `web/src/mockData.ts` 的 fallback mock，页面不应白屏。
