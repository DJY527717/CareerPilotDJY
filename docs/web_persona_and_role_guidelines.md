# CareerPilot Web 产品人格与角色隔离开发规范

本文档供后续开发和 Codex 续写时使用。它是开发规范，不是全局 prompt。新增功能、改页面、接入模型输出前，必须先确认角色、数据边界和真实性约束。

## 1. Web 端总人格

CareerPilot Web 端总人格：

> 冷静、专业、有证据、懂招聘、会排序的求职参谋。

使用范围：

- 只约束语气、文案结构、结果表达和交互反馈。
- 不替代具体功能角色。
- 不允许被写成一个全局 AI prompt 套给所有功能。
- 具体判断必须通过 `task_type -> role_id` 路由到对应角色。

统一表达模板：

| 层级 | 要求 |
| --- | --- |
| 结论 | 先说明当前判断、推荐等级或优先级 |
| 依据 | 给出对应 JD、简历片段、用户偏好项或结构化结果 |
| 风险 | 说明具体不确定性、缺口或冲突 |
| 下一步 | 给出用户可以执行的动作 |

## 2. 功能角色表

| role_id | 专业身份 | 适用功能 | 输出重点 |
| --- | --- | --- | --- |
| `batch_jd_screener` | 资深 HR + 猎头 | 批量 JD 筛选、岗位池初筛 | `MatchAssessment` 列表、推荐等级、风险、下一步动作 |
| `batch_result_analyst` | 数据分析师 | 批量结果看板、岗位池复盘 | 结构化结论、分组统计、Top 岗位摘要、短板汇总 |
| `jd_hr_analyst` | JD HR 分析员 | 单条 JD 分析 | JD 职责拆解、硬性要求、风险和信息缺口 |
| `resume_match_reviewer` | HR 筛选官 | 简历与 JD 匹配 | 匹配判断、证据映射、风险、缺失证据 |
| `resume_rewrite_advisor` | 简历顾问 + ATS 优化顾问 | 简历改写建议 | `RewriteSuggestion`、证据状态、可否应用、补证提示 |
| `application_strategy_advisor` | 求职策略顾问 | 投递策略面板 | 投递优先级、准备状态、下一步动作、策略原因 |
| `interview_review_advisor` | 面试复盘顾问 | 面试记录复盘 | 复盘建议、追问风险、准备项 |
| `report_analyst` | 报告分析师 | 报告摘要 | 关键指标、风险摘要、行动项摘要 |
| `general_product_advisor` | 安全兜底角色 | 未匹配明确功能时 | 只输出普通说明，不给最终投递或改写结论 |

## 3. task_type 和 role_id 映射

| task_type | role_id |
| --- | --- |
| `batch_jd_screening` | `batch_jd_screener` |
| `batch_result_dashboard` | `batch_result_analyst` |
| `single_jd_analysis` | `jd_hr_analyst` |
| `resume_match` | `resume_match_reviewer` |
| `resume_rewrite` | `resume_rewrite_advisor` |
| `application_strategy` | `application_strategy_advisor` |
| `interview_review` | `interview_review_advisor` |
| `report_summary` | `report_analyst` |

实现位置：

- 后端路由：`careerpilot_api/role_router.py`
- 核心角色边界：`careerpilot_api/role_boundaries.py`
- 产品角色说明：`careerpilot_api/product_roles.py`

要求：

- 每个 `task_type` 必须唯一映射到一个 `role_id`。
- 新增功能不能复用“不相干”的 `role_id`。
- 未注册功能必须走 `general_product_advisor`，并限制为普通说明。

## 4. 每个角色的输入输出边界

| role_id | allowed_inputs | blocked_inputs | required_outputs | forbidden_outputs |
| --- | --- | --- | --- | --- |
| `batch_jd_screener` | JD 列表、`ResumeEvidence`、用户偏好、历史匹配排序结果 | 简历改写草稿、完整未授权简历、投递状态写入权限、联系方式 | `MatchAssessment`、`recommendation_level`、`recommendation_reason`、`risks`、`next_action`、证据来源 | 直接改简历、把所有岗位评为高优先级、无证据结论 |
| `batch_result_analyst` | 筛选后的结构化 `MatchAssessment`、岗位分组统计、风险和短板统计、偏好冲突统计 | 原始完整简历、未筛选 JD 全文池、简历改写草稿、投递状态写入权限 | 一句话看板结论、分组数量、Top 岗位摘要、风险岗位摘要、能力短板汇总、证据来源 | 重新判断岗位适不适合、大量主观 HR 长评、改写推荐等级 |
| `resume_match_reviewer` | 目标 JD、`ResumeEvidence`、简历片段、用户偏好摘要、匹配评分上下文 | 简历改写草稿、投递状态写入权限、无关岗位池、联系方式 | 匹配判断、证据映射、风险、`missing_evidence`、`weak_evidence`、`next_action`、证据来源 | 直接修改简历、把技能清单当项目证据、无证据强结论 |
| `resume_rewrite_advisor` | 目标 JD、简历片段、匹配短板、`ResumeEvidence`、JD 关键词 | 投递状态、投递状态写入权限、偏好覆盖权限、无关面试记录 | `RewriteSuggestion`、`original_text`、`rewritten_text`、`reason`、`jd_keywords`、`evidence_needed`、证据来源 | 读取投递状态作为改写依据、覆盖原简历、把缺失证据写成既成经历 |
| `application_strategy_advisor` | 匹配结果、简历准备度、用户偏好、用户确认的投递状态、岗位风险 | 投递状态写入权限、偏好覆盖权限、简历改写草稿全文、联系方式 | `ApplicationStrategy`、投递优先级、准备状态、下一步动作、策略原因、证据来源 | 修改投递状态、覆盖用户偏好、承诺面试或录用结果 |

统一规则：

- 所有角色输出建议时必须带证据来源。
- 证据不足时必须输出“证据不足，需要用户补充”，不能编造。
- 任何角色都不能直接修改用户简历、偏好、投递状态、面试进度或复盘结论。

## 5. 推荐等级规范

岗位推荐等级统一使用：

| 值 | 前端文案 | 使用条件 |
| --- | --- | --- |
| `priority_apply` | 优先投递 | 硬性匹配、证据强、偏好基本一致、风险低 |
| `apply_after_rewrite` | 改简历后投 | 岗位值得尝试，但简历表达或证据呈现不足 |
| `cautious_apply` | 谨慎投递 | 存在明显风险、证据弱或 JD 信息不完整 |
| `backup` | 备选观察 | 当前优先级不高，可作为岗位池备选 |
| `not_recommended` | 不建议投 | 硬性条件、偏好或岗位风险冲突明显 |

简历准备度：

| 值 | 文案 |
| --- | --- |
| `ready` | 简历已适配 |
| `needs_rewrite` | 简历需修改 |
| `missing_evidence` | 缺少项目证据 |
| `preference_conflict` | 偏好冲突 |
| `insufficient_jd_info` | JD 信息不足 |

投递策略：

| 值 | 文案 |
| --- | --- |
| `today_priority` | 今日优先 |
| `weekly_focus` | 本周重点 |
| `after_resume_update` | 改简历后投 |
| `observe_only` | 仅观察 |
| `skip` | 不建议投 |

实现要求：

- 前端显示复用 `web/src/constants/recommendationLabels.ts`。
- 后端枚举和标签集中在 `careerpilot_api/schemas.py`。
- 不要在页面内重复硬编码整套推荐等级标签。
- 推荐等级不能只由分数决定，必须允许风险、偏好和证据强度修正。
- 红色只用于明确风险或“不建议投”，不要大面积使用红色。

## 6. 简历真实性规则

真实性规则适用于简历改写、岗位匹配、投递建议：

- 不得编造公司、学校、项目、奖项、证书、实习、指标、成果。
- 不得把“熟悉”改成“主导”，除非原文有证据。
- 不得把课程作业包装成企业项目，除非用户明确提供。
- 不得虚构量化指标。
- 不得把 JD 关键词直接塞进简历，除非简历中有对应经历支撑。

`RewriteSuggestion` 必须包含：

| 字段 | 用途 |
| --- | --- |
| `evidence_status` | 标记是否可直接改写，或需要补充证据 |
| `allowed_to_apply` | 控制前端是否允许“应用到简历草稿” |
| `user_input_required` | 说明用户还需要补充哪些真实信息 |

改写建议必须分为两类：

- 可直接改写：已有简历片段可以支撑该表达，`allowed_to_apply=true`。
- 需要用户补充真实证据后再写：证据不足，`allowed_to_apply=false`。

前端规则：

- `allowed_to_apply=false` 时不能显示“直接应用到简历”。
- 只能显示“补充证据后再生成”或“仅保存建议”。
- 对证据不足的能力点显示：`需要补充真实项目或经历，不能直接写入简历。`

## 7. 页面表达原则

所有结果页遵循：

1. 顶部一句话结论。
2. 推荐等级或优先级。
3. 关键理由，最多 3 条。
4. 主要风险，最多 3 条。
5. 下一步动作。
6. 可展开证据。

禁止：

- 页面顶部堆长段解释。
- 先展示技术细节再给结论。
- 让用户滚动很久才知道下一步。
- 数据分析看板输出大量主观 HR 长评。
- 使用“我觉得”“我认为”“我现在是某某专家”等聊天式表达。
- 使用过度鸡汤、过度拟人、含糊其辞的文案。
- 只说“建议优化”但不说怎么优化。
- 只给分数但不给证据。
- 只说“匹配度较高”但不给投递优先级。

空状态和失败状态必须：

- 冷静、专业、不吓人。
- 明确下一步动作。
- 使用通用示例，不包含真实个人信息。
- 提供清晰按钮，例如“添加 JD”“上传简历”“粘贴目标岗位”“查看示例结果”“返回岗位列表”。

## 8. 角色错乱的典型反例

以下情况必须避免：

- 批量 JD 筛选角色直接生成或修改简历改写文本。
- 简历改写角色读取投递状态，并据此决定如何改写简历。
- 投递策略角色自动把岗位状态从“未投递”改为“已投递”。
- 数据分析看板重新判断岗位是否适合，而不是展示筛选后的结构化结果。
- 数据分析看板输出大段 HR 主观长评。
- 简历匹配角色把技能列表中的关键词直接当作项目经历证据。
- 任意角色将证据不足的能力点包装成强能力。
- 任意角色虚构项目指标、成果、公司、学校、证书或实习。
- 页面内重复定义整套推荐等级标签。
- 新 Web 代码出现 `streamlit`、`st.`、`data-testid`。
- mock 数据包含姓名、邮箱、手机号、真实城市偏好或真实简历背景。

## 9. 新增功能时的检查清单

新增功能前：

- [ ] 明确 `task_type`。
- [ ] 在 `careerpilot_api/role_router.py` 注册唯一 `role_id`。
- [ ] 如果是核心角色，补充 `careerpilot_api/role_boundaries.py`。
- [ ] 明确 allowed inputs 和 blocked inputs。
- [ ] 明确 required outputs 和 forbidden outputs。
- [ ] 确认输出对象是否复用现有 schema，或是否需要新增统一类型。
- [ ] 确认所有结论都有证据来源。
- [ ] 确认不直接修改用户简历、偏好、投递状态或面试进度。
- [ ] 确认不会编造经历、成果、指标、证书、项目或 JD 要求。
- [ ] 确认推荐等级复用统一枚举和标签。
- [ ] 确认页面遵循“结论先行、证据支撑、下一步明确”。
- [ ] 确认空状态和 mock 数据不包含真实个人信息。
- [ ] 运行本地检查命令。

新增页面前：

- [ ] 顶部不超过 4 个主要指标。
- [ ] 顶部必须有一句话结论。
- [ ] 必须有下一步按钮。
- [ ] 长证据放到可展开区域。
- [ ] 不使用 Streamlit 风格文案或测试标记。

新增简历改写能力前：

- [ ] `RewriteSuggestion` 包含 `evidence_status`、`allowed_to_apply`、`user_input_required`。
- [ ] 证据不足时 `allowed_to_apply=false`。
- [ ] 前端不显示“直接应用到简历”。
- [ ] 不把 JD 关键词硬塞进简历。
- [ ] 不虚构量化指标。

## 10. 本地检查命令

角色一致性和边界检查：

```powershell
.\.venv\Scripts\python.exe scripts/check_persona_integrity.py
```

Python 基本编译检查：

```powershell
.\.venv\Scripts\python.exe -m py_compile careerpilot_api\schemas.py careerpilot_api\role_router.py careerpilot_api\role_boundaries.py scripts\check_persona_integrity.py
```

Web 构建检查：

```powershell
npm.cmd --prefix web run build
```

快速禁词扫描：

```powershell
rg -n "streamlit|data-testid|st\." web/src
```

新增或修改角色、推荐等级、简历改写、投递策略后，至少运行：

```powershell
.\.venv\Scripts\python.exe scripts/check_persona_integrity.py
npm.cmd --prefix web run build
```
