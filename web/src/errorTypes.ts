export type CareerPilotErrorType =
  | "missing_resume"
  | "missing_jd"
  | "insufficient_evidence"
  | "analysis_failed"
  | "api_unavailable"
  | "invalid_input"
  | "unsupported_file";

export type CareerPilotErrorCopy = {
  title: string;
  message: string;
  nextAction: string;
  recoverable: boolean;
};

export const errorMessages: Record<CareerPilotErrorType, CareerPilotErrorCopy> = {
  missing_resume: {
    title: "尚未上传简历",
    message: "请先粘贴或上传完整简历。需要真实经历片段，才能判断岗位匹配和证据强度。",
    nextAction: "上传简历",
    recoverable: true,
  },
  missing_jd: {
    title: "尚未添加JD",
    message: "请粘贴目标岗位描述。至少需要岗位职责、任职要求和关键词，才能开始分析。",
    nextAction: "添加JD",
    recoverable: true,
  },
  insufficient_evidence: {
    title: "证据不足，无法给出确定判断",
    message: "当前材料不足以支撑强结论。请补充项目背景、个人负责范围、工具方法和可验证结果。",
    nextAction: "补充证据",
    recoverable: true,
  },
  analysis_failed: {
    title: "分析未完成",
    message: "本次分析没有得到稳定结果。请检查输入内容是否完整，再重新发起分析。",
    nextAction: "重新分析",
    recoverable: true,
  },
  api_unavailable: {
    title: "API暂时不可用",
    message: "当前无法连接分析服务。可以先查看示例结果，或稍后重新发起分析。",
    nextAction: "重试分析",
    recoverable: true,
  },
  invalid_input: {
    title: "输入内容不足",
    message: "当前输入过短或缺少关键信息。请补充完整简历和JD后再分析。",
    nextAction: "补充内容",
    recoverable: true,
  },
  unsupported_file: {
    title: "暂不支持该文件",
    message: "当前文件格式无法解析。请改为粘贴文本，或上传可读取的简历文本内容。",
    nextAction: "改用文本",
    recoverable: true,
  },
};

export function createErrorState(
  type: CareerPilotErrorType,
  overrides: Partial<CareerPilotErrorCopy> = {},
): CareerPilotErrorCopy & { type: CareerPilotErrorType } {
  return {
    type,
    ...errorMessages[type],
    ...overrides,
  };
}
