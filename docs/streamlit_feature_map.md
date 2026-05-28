# Streamlit 功能地图

本轮只从旧 Streamlit 版本中提取功能口径和未来接口方向，不迁移 Streamlit UI。`app.py` 仍作为过渡入口保留，`careerpilot_api` 不导入 Streamlit、不导入根目录 `app.py`，也不搬运 `st.session_state`、`st.button`、`st.tabs`、`st.columns`、`st.write`、Streamlit CSS 或页面布局代码。

## 五个 Web 模块

- `settings` 设置与资料：用户资料、当前简历、求职偏好。
- `jd` JD分析：单条 JD、批量 JD、招聘趋势。
- `resume` 简历优化：简历解析、岗位匹配、目标 JD 改简历、能力缺口。
- `decision` 求职决策：岗位排序、机会评估、岗位比较、投递管理。
- `report` 面试报告：面试记录、面试复盘、数据看板。

## 功能映射

| 旧 Streamlit 功能 | Web 模块 | 旧入口或相关函数 | 建议 API | 本轮状态 |
| --- | --- | --- | --- | --- |
| 用户资料和求职偏好 | `settings` | `render_settings_workspace_tab`, `render_settings_profile_section`, `render_settings_preferences_tab`, `load_target_preferences`, `save_target_preferences` | `GET /api/me`, `GET /api/settings/summary`, planned `GET /api/profile`, planned `GET /api/preferences` | active 只返回启动摘要 |
| 当前简历和简历解析 | `settings`, `resume` | `render_settings_resume_section`, `render_resume_tab`, `parse_resume_content`, `parsed_resume_from_upload`, `load_user_resumes` | planned `GET /api/resumes`, planned `POST /api/resume/parse` | 后续规划 |
| 单条 JD 分析 | `jd` | `render_jd_tab`, `analyze_jd`, `set_current_target_jd`, `single_jd_result_panel_data` | `POST /api/jd/analyze` | active mock |
| 批量 JD 筛选 | `jd` | `render_batch_jd_tab`, `split_batch_jd_text`, `analyze_batch_jd_records`, `batch_recommendation` | planned `POST /api/jd/batch-screen` | 后续规划 |
| 招聘趋势 | `jd` | `render_recruitment_monitor_tab`, `scan_exported_jd_records`, `fetch_jd_url`, `crawl_jd_urls` | planned `GET /api/jd/trends` | 后续规划 |
| 简历与岗位匹配 | `resume` | `match_resume_to_jd`, `render_resume_match_snapshot`, `resume_match_overall_score` | planned `POST /api/resume/match` | 后续规划 |
| 目标 JD 改简历 | `resume` | `render_custom_resume_tab`, `build_custom_resume`, `generate_targeted_resume_revision` | planned `POST /api/resume/rewrite` | 后续规划 |
| 能力缺口分析 | `resume` | `render_gap_tab`, `build_gap_analysis`, `build_resume_shortcoming_rows_v2` | planned `POST /api/resume/gap` | 后续规划 |
| 岗位排序 | `decision`, `jd` | `batch_rank_score_column`, `sort_batch_rank_rows`, `render_batch_overview` | `GET /api/jobs/ranked` | active mock |
| 岗位比较 | `decision` | 旧版主要通过批量列表和决策卡片间接比较 | planned `POST /api/jobs/compare` | 后续规划 |
| 机会评估 | `decision` | `render_offer_prediction_tab`, `predict_offer_probabilities`, `job_decision_label`, `build_job_action_plan` | planned `POST /api/jobs/evaluate` | 后续规划 |
| 投递管理 | `decision` | `render_applications_tab`, `add_application`, `load_applications`, `save_application_edits`, `delete_application` | planned `GET /api/applications` | 后续规划 |
| 面试记录 | `report` | `render_interview_tab`, `analyze_interview`, `extract_interview_questions_v2` | planned `GET /api/interviews`, planned `POST /api/interviews/record` | 后续规划 |
| 面试复盘和报告 | `report` | `generate_interview_questions`, `build_interview_gap_rows_v2`, `build_personalized_interview_answers_v2` | planned `POST /api/interviews/report` | 后续规划 |
| 数据看板 | `report` | `render_dashboard_tab`, `build_report_frames`, `build_report_readiness_state`, `careerpilot.report_exports` | planned `GET /api/reports/dashboard` | 后续规划 |

## 本轮最小纵切

本轮只实现启动数据、岗位列表和 JD 分析占位：

- `GET /api/health`
- `GET /api/bootstrap`
- `GET /api/me`
- `GET /api/workspaces`
- `GET /api/settings/summary`
- `GET /api/jobs/ranked`
- `POST /api/jd/analyze`

这些接口只返回通用 mock，不接真实算法。其他能力只在文档中规划，避免一次性铺满没人调用的接口代码。
