export const feedbackCopy = {
  resumeMissing: {
    title: "尚未上传简历",
    body: "请先粘贴或上传简历内容。系统需要真实经历片段，才能判断岗位匹配和证据强度。",
    action: "上传简历",
  },
  jdMissing: {
    title: "尚未添加JD",
    body: "请粘贴目标岗位描述。至少需要岗位职责、任职要求和关键词，才能开始分析。",
    action: "添加JD",
  },
  analyzingJd: {
    title: "正在分析JD",
    body: "正在拆解岗位职责、硬性要求和关键词。完成后会继续匹配简历证据。",
  },
  matchingResume: {
    title: "正在匹配简历",
    body: "正在核对简历片段是否能支撑岗位要求，不会把缺失证据写成优势。",
  },
  generatingRewrite: {
    title: "正在生成改写建议",
    body: "正在基于目标JD和已有简历证据生成建议；证据不足的内容会标记为需补充。",
  },
  noPriorityJobs: {
    title: "没有找到优先投递岗位",
    body: "当前岗位池里没有同时满足硬性匹配、偏好和证据强度的岗位。建议补充JD或调整筛选条件后再看。",
    action: "添加JD",
  },
  allCautiousJobs: {
    title: "所有岗位都需要谨慎投递",
    body: "当前岗位都有证据短板、偏好冲突或JD信息不足。建议先补充项目证据，再决定投递顺序。",
    action: "返回岗位列表",
  },
  insufficientEvidence: {
    title: "证据不足，无法给出确定判断",
    body: "请补充对应JD要求的真实简历片段，例如项目背景、个人负责范围、工具方法和可验证结果。",
    action: "补充证据",
  },
  missingProjectEvidence: {
    title: "简历缺少真实项目证据",
    body: "当前内容不足以直接写进简历。请先补充真实项目经历，再生成可投递版本。",
    action: "补充项目证据",
  },
  apiUnavailable: {
    title: "API暂时不可用",
    body: "当前无法连接分析服务。可以先查看示例结果，或稍后重新发起分析。",
    action: "查看示例结果",
  },
} as const;

export type FeedbackCopyKey = keyof typeof feedbackCopy;
