(function (globalThis) {
  function repairPrivateDigits(text) {
    return (text || "").replace(/[\ue031-\ue03a]/g, (char) => String(char.charCodeAt(0) - 0xe031));
  }

  function cleanText(text) {
    return repairPrivateDigits(text || "")
      .replace(/\u00a0/g, " ")
      .replace(/[ \t]+/g, " ")
      .replace(/\n{3,}/g, "\n\n")
      .trim();
  }

  function compact(text) {
    return cleanText(text).replace(/\s+/g, "");
  }

  function nodeText(node) {
    if (!node) return "";
    return cleanText(
      node.innerText
      || node.textContent
      || (typeof node.getAttribute === "function" && (
        node.getAttribute("aria-label")
        || node.getAttribute("title")
        || node.getAttribute("alt")
        || node.getAttribute("placeholder")
      ))
      || ""
    );
  }

  function uniqueValues(items, limit) {
    const output = [];
    const seen = new Set();
    for (const item of items || []) {
      const value = cleanText(item);
      if (!value || seen.has(value)) continue;
      seen.add(value);
      output.push(value);
      if (limit && output.length >= limit) break;
    }
    return output;
  }

  const CAPTURE_SCHEMA_VERSION = "careerpilot.capture.v1";

  function sourceSiteFromUrl(url) {
    return siteKeyFromHost(url || (globalThis.location && globalThis.location.href) || "") || "";
  }

  function normalizeExportedJob(job, context) {
    const ctx = context || {};
    const sourceUrl = cleanText(
      job && (job.sourceUrl || job.source_url)
      || ctx.sourceUrl
      || ctx.url
      || (globalThis.location && globalThis.location.href)
      || ""
    );
    const rawText = cleanText(
      job && (job.rawText || job.raw_text)
      || job && job.text
      || job && job.content
      || job && job.detailText
      || job && job.detail_text
      || ""
    );
    const detailText = cleanText(job && (job.detailText || job.detail_text) || "");
    const text = cleanText(job && job.text || detailText || rawText);
    const detailUrl = cleanText(
      job && (job.detailUrl || job.detail_url || job.jobUrl || job.job_url || job.href || job.link)
      || job && job.url
      || ctx.detailUrl
      || ""
    );
    const capturedAt = cleanText(job && (job.capturedAt || job.savedAt) || ctx.capturedAt || new Date().toISOString());
    const normalized = {
      ...(job || {}),
      schemaVersion: cleanText(job && job.schemaVersion || ctx.schemaVersion || CAPTURE_SCHEMA_VERSION),
      captureMode: cleanText(job && job.captureMode || ctx.captureMode || ""),
      sourceSite: cleanText(job && job.sourceSite || ctx.sourceSite || sourceSiteFromUrl(sourceUrl || detailUrl)),
      title: cleanText(job && (job.title || job.detailTitle) || ""),
      company: cleanText(job && job.company || ""),
      salary: cleanText(job && job.salary || ""),
      location: cleanText(job && job.location || ""),
      experience: cleanText(job && job.experience || ""),
      education: cleanText(job && job.education || ""),
      url: cleanText(job && job.url || detailUrl || ""),
      detailUrl,
      sourceUrl,
      text,
      detailText,
      rawText,
      detailFetched: job && job.detailFetched === true,
      capturedAt,
    };
    return normalized;
  }

  function normalizeExportedJobs(jobs, context) {
    return (jobs || []).filter(Boolean).map((job) => normalizeExportedJob(job, context));
  }

  function normalizeExportedDetailPayload(detail, context) {
    const ctx = context || {};
    const sourceUrl = cleanText((detail && detail.sourceUrl) || ctx.sourceUrl || (detail && detail.url) || (globalThis.location && globalThis.location.href) || "");
    const detailText = cleanText((detail && (detail.detailText || detail.text || detail.content)) || "");
    return normalizeExportedJob({
      ...(detail || {}),
      captureMode: (detail && detail.captureMode) || ctx.captureMode || "detail",
      sourceUrl,
      detailUrl: (detail && (detail.detailUrl || detail.url)) || sourceUrl,
      url: (detail && detail.url) || sourceUrl,
      detailText,
      rawText: (detail && (detail.rawText || detail.content || detail.text)) || detailText,
      text: (detail && detail.text) || detailText,
      detailFetched: true,
    }, {
      ...ctx,
      sourceUrl,
      captureMode: ctx.captureMode || "detail",
    });
  }

  function salaryPatterns() {
    return [
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*[kK](?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[kK]\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*[kK]?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*千\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*万(?:\s*\/?\s*月)?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*千(?:\s*\/?\s*月)?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*[万wW]\s*\/?\s*年(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[万wW]\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*[万wW]?\s*\/?\s*年(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*[万wW](?:\s*\/?\s*月)?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[万wW]\s*(?:以上|\+)?(?:\s*\/?\s*(?:月|年))?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[kK]\s*(?:以上|\+)?(?:\s*[·*xX]\s*\d+\s*薪)?/,
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*元\s*\/?\s*天/,
      /\d+(?:\.\d+)?\s*[-~～—至到]\s*\d+(?:\.\d+)?\s*\/\s*天/,
      /\d+(?:\.\d+)?\s*元?\s*\/\s*天/,
      /\d+\s*薪/,
      /(?:月薪|年薪)\s*[:：]?\s*\d+(?:\.\d+)?\s*[-~～—至到]?\s*\d*(?:\.\d+)?\s*[kK千万元wW]*/,
      /(?:薪资|工资|待遇)\s*(?:面议|可议|详谈)/,
      /(?:薪资)?面议/,
    ];
  }

  function findSalary(text) {
    for (const pattern of salaryPatterns()) {
      const match = String(text || "").match(pattern);
      if (match) return cleanText(match[0]);
    }
    return "";
  }

  function isVisible(node) {
    if (!node || !(node instanceof Element)) return false;
    const style = window.getComputedStyle(node);
    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
    const rect = node.getBoundingClientRect();
    if (rect.width > 18 && rect.height > 12) return true;
    return nodeText(node).length >= 12;
  }

  function resolveUrl(href, baseUrl) {
    if (!href) return "";
    try {
      return new URL(href, baseUrl || location.href).href;
    } catch (_error) {
      return "";
    }
  }

  function hostName(url) {
    try {
      return new URL(url || location.href, location.href).hostname.toLowerCase();
    } catch (_error) {
      return "";
    }
  }

  function matchesHost(host, fragments) {
    return (fragments || []).some((fragment) => host === fragment || host.endsWith(`.${fragment}`));
  }

  function siteKeyFromHost(url) {
    const host = hostName(url);
    if (matchesHost(host, ["zhipin.com"])) return "boss";
    if (matchesHost(host, ["zhaopin.com"])) return "zhaopin";
    if (matchesHost(host, ["liepin.com"])) return "liepin";
    if (matchesHost(host, ["51job.com", "yingjiesheng.com"])) return "job51";
    if (matchesHost(host, ["shixiseng.com"])) return "shixiseng";
    return "";
  }

  function textBySelectors(root, selectors, options) {
    const opts = options || {};
    if (!root) return "";
    for (const selector of selectors || []) {
      const nodes = queryAllDeep(root, selector);
      for (const node of nodes) {
        const value = cleanText(
          opts.attribute ? node.getAttribute(opts.attribute) : nodeText(node)
        );
        if (value) return value;
      }
    }
    return "";
  }

  function textsBySelectors(root, selectors, limit) {
    if (!root) return [];
    const values = [];
    for (const selector of selectors || []) {
      queryAllDeep(root, selector).forEach((node) => {
        const value = nodeText(node);
        if (value) values.push(value);
      });
    }
    return uniqueValues(values, limit);
  }

  function bestAnchorHref(root, selectors, baseUrl) {
    const anchors = [];
    for (const selector of selectors || ["a[href]"]) {
      queryAllDeep(root, selector).forEach((node) => {
        if (node instanceof HTMLAnchorElement && node.getAttribute("href")) anchors.push(node);
      });
    }
    for (const anchor of anchors) {
      const href = anchor.getAttribute("href") || "";
      const resolved = resolveUrl(href, baseUrl);
      if (resolved && isLikelyJobUrl(href, baseUrl)) return resolved;
    }
    return "";
  }

  function findEducation(text) {
    const match = cleanText(text).match(/(\u535a\u58eb|\u7855\u58eb|\u7814\u7a76\u751f|\u672c\u79d1|\u5927\u4e13|\u5b66\u5386\u4e0d\u9650)/);
    return match ? match[0] : "";
  }

  function findExperience(text) {
    const match = cleanText(text).match(/((?:\d+\s*[-~\u81f3\u5230]\s*)?\d+\s*\u5e74(?:\u4ee5\u4e0a)?|(?:\d+\s*\u5e74\u4ee5\u4e0b)|\u7ecf\u9a8c\u4e0d\u9650|\u5e94\u5c4a|\u6821\u62db|\u5b9e\u4e60)/);
    return match ? match[0] : "";
  }

  function findLocation(text) {
    const value = cleanText(text);
    const cities = [
      "北京", "上海", "天津", "重庆", "石家庄", "唐山", "秦皇岛", "邯郸", "邢台", "保定", "张家口", "承德", "沧州", "廊坊", "衡水",
      "太原", "大同", "阳泉", "长治", "晋城", "朔州", "晋中", "运城", "忻州", "临汾", "吕梁",
      "沈阳", "大连", "鞍山", "抚顺", "本溪", "丹东", "锦州", "营口", "阜新", "辽阳", "盘锦", "铁岭", "朝阳", "葫芦岛",
      "长春", "吉林", "四平", "辽源", "通化", "白山", "松原", "白城",
      "哈尔滨", "齐齐哈尔", "鸡西", "鹤岗", "双鸭山", "大庆", "伊春", "佳木斯", "七台河", "牡丹江", "黑河", "绥化",
      "南京", "无锡", "徐州", "常州", "苏州", "南通", "连云港", "淮安", "盐城", "扬州", "镇江", "泰州", "宿迁",
      "杭州", "宁波", "温州", "嘉兴", "湖州", "绍兴", "金华", "衢州", "舟山", "台州", "丽水",
      "合肥", "芜湖", "蚌埠", "淮南", "马鞍山", "淮北", "铜陵", "安庆", "黄山", "滁州", "阜阳", "宿州", "六安", "亳州", "池州", "宣城",
      "福州", "厦门", "莆田", "三明", "泉州", "漳州", "南平", "龙岩", "宁德",
      "南昌", "景德镇", "萍乡", "九江", "新余", "鹰潭", "赣州", "吉安", "宜春", "抚州", "上饶",
      "济南", "青岛", "淄博", "枣庄", "东营", "烟台", "潍坊", "济宁", "泰安", "威海", "日照", "临沂", "德州", "聊城", "滨州", "菏泽",
      "郑州", "开封", "洛阳", "平顶山", "安阳", "鹤壁", "新乡", "焦作", "濮阳", "许昌", "漯河", "三门峡", "南阳", "商丘", "信阳", "周口", "驻马店",
      "武汉", "黄石", "十堰", "宜昌", "襄阳", "鄂州", "荆门", "孝感", "荆州", "黄冈", "咸宁", "随州",
      "长沙", "株洲", "湘潭", "衡阳", "邵阳", "岳阳", "常德", "张家界", "益阳", "郴州", "永州", "怀化", "娄底",
      "广州", "韶关", "深圳", "珠海", "汕头", "佛山", "江门", "湛江", "茂名", "肇庆", "惠州", "梅州", "汕尾", "河源", "阳江", "清远", "东莞", "中山", "潮州", "揭阳", "云浮",
      "海口", "三亚", "成都", "自贡", "攀枝花", "泸州", "德阳", "绵阳", "广元", "遂宁", "内江", "乐山", "南充", "眉山", "宜宾", "广安", "达州", "雅安", "巴中", "资阳",
      "贵阳", "六盘水", "遵义", "安顺", "毕节", "铜仁", "昆明", "曲靖", "玉溪", "保山", "昭通", "丽江", "普洱", "临沧",
      "西安", "铜川", "宝鸡", "咸阳", "渭南", "延安", "汉中", "榆林", "安康", "商洛",
      "兰州", "嘉峪关", "金昌", "白银", "天水", "武威", "张掖", "平凉", "酒泉", "庆阳", "定西", "陇南",
      "呼和浩特", "包头", "乌海", "赤峰", "通辽", "鄂尔多斯", "呼伦贝尔", "巴彦淖尔", "乌兰察布",
      "南宁", "柳州", "桂林", "梧州", "北海", "防城港", "钦州", "贵港", "玉林", "百色", "贺州", "河池", "来宾", "崇左",
      "银川", "石嘴山", "吴忠", "固原", "中卫", "乌鲁木齐", "克拉玛依",
      "香港", "澳门", "台北", "全国", "远程"
    ];
    const city = cities.find((item) => value.includes(item));
    if (!city) return "";
    const cityIndex = value.indexOf(city);
    const lineStart = Math.max(0, value.lastIndexOf("\n", cityIndex) + 1);
    const lineEndRaw = value.indexOf("\n", cityIndex);
    const lineEnd = lineEndRaw >= 0 ? lineEndRaw : value.length;
    const line = cleanText(value.slice(lineStart, lineEnd));
    if (line && line.length <= 28) return line;
    const suffix = value.slice(cityIndex + city.length).match(/^(?:[-·\s]*(?:[\u4e00-\u9fa5A-Za-z0-9]+区|[\u4e00-\u9fa5A-Za-z0-9]+县|[\u4e00-\u9fa5A-Za-z0-9]+路|[\u4e00-\u9fa5A-Za-z0-9]+街道)){0,2}/);
    return cleanText(city + (suffix ? suffix[0] : ""));
  }

  const JOB_TITLE_KEYWORDS = /(?:工程师|开发|前端|后端|全栈|客户端|算法|测试|运维|数据|分析|产品|运营|市场|销售|商务|设计|视觉|交互|用户研究|项目|经理|主管|专员|助理|实习|管培|校招|招聘|人事|HR|财务|会计|法务|顾问|咨询|编辑|文案|内容|教师|讲师|研究员|架构|安全|风控|Java|Python|Go|C\+\+|Android|iOS|Web|AI|AIGC|LLM|BI|SQL)/i;
  const NON_TITLE_LINE = /^(?:筛选|排序|推荐|立即投递|申请职位|查看详情|职位详情|薪资|地点|城市|经验|学历|公司|福利|发布时间|更新|收藏|沟通|登录|注册|上一页|下一页|加载更多|全部|默认|综合排序|相关度|最新|清空|展开|收起)$/;

  function isLikelyFallbackTitle(line) {
    const value = cleanText(line);
    if (!value || value.length > 80) return false;
    if (findSalary(value) || findLocation(value) === value || findEducation(value) === value || findExperience(value) === value) return false;
    if (NON_TITLE_LINE.test(value)) return false;
    if (/^(?:\d+|[·•\-_*#]|[<>])/.test(value)) return false;
    if (/(?:筛选|排序|推荐|投递|查看详情|薪资|地点|经验|学历)/.test(value) && value.length <= 16) return false;
    return true;
  }

  function fallbackFieldsFromText(text) {
    const lines = cleanText(text).split(/\n+/).map((line) => cleanText(line)).filter(Boolean);
    const title = lines.find((line) => isLikelyFallbackTitle(line) && JOB_TITLE_KEYWORDS.test(line))
      || lines.find(isLikelyFallbackTitle)
      || "";
    const company = lines.find((line) => /(\u516c\u53f8|\u96c6\u56e2|\u79d1\u6280|\u54a8\u8be2|\u6709\u9650)/.test(line) && line !== title) || "";
    return {
      title,
      company,
      salary: findSalary(text),
      location: findLocation(text),
      education: findEducation(text),
      experience: findExperience(text),
    };
  }

  function queryAllDeep(root, selector) {
    if (!root || !selector) return [];
    const nodes = [];
    const seen = new Set();
    const queue = [root];

    while (queue.length) {
      const current = queue.shift();
      if (!current || seen.has(current)) continue;
      seen.add(current);

      if (current instanceof Element && typeof current.matches === "function") {
        try {
          if (current.matches(selector)) nodes.push(current);
        } catch (_error) {
        }
      }

      if (typeof current.querySelectorAll === "function") {
        current.querySelectorAll(selector).forEach((node) => nodes.push(node));
        current.querySelectorAll("*").forEach((node) => {
          if (node && node.shadowRoot) queue.push(node.shadowRoot);
          if (node instanceof HTMLIFrameElement) {
            try {
              if (node.contentDocument && node.contentDocument.body) queue.push(node.contentDocument);
            } catch (_error) {
            }
          }
        });
      }
    }

    return Array.from(new Set(nodes));
  }

  function bodyText(doc) {
    if (!doc) return "";
    const texts = [];
    const mainBody = doc.body;
    if (mainBody) texts.push(nodeText(mainBody));
    queryAllDeep(doc, "iframe").forEach((frame) => {
      if (!(frame instanceof HTMLIFrameElement)) return;
      try {
        const frameDoc = frame.contentDocument;
        if (frameDoc && frameDoc.body) texts.push(nodeText(frameDoc.body));
      } catch (_error) {
      }
    });
    return cleanText(texts.join("\n\n"));
  }

  function throwIfAborted(signal) {
    if (!signal) return;
    if ((typeof signal.aborted === "function" && signal.aborted()) || signal.aborted === true) {
      const error = new Error("Capture stopped by user.");
      error.name = "AbortError";
      throw error;
    }
  }

  async function pause(ms, signal) {
    throwIfAborted(signal);
    await new Promise((resolve) => setTimeout(resolve, ms));
    throwIfAborted(signal);
  }

  function salaryFromStructuredValue(value) {
    if (!value) return "";
    if (typeof value === "string" || typeof value === "number") return cleanText(String(value));
    const minValue = value.minValue ?? value.value?.minValue;
    const maxValue = value.maxValue ?? value.value?.maxValue;
    const unitText = cleanText(String(value.unitText || value.value?.unitText || ""));
    const currency = cleanText(String(value.currency || value.value?.currency || ""));
    if (minValue != null && maxValue != null) {
      return cleanText(`${minValue}-${maxValue}${currency ? ` ${currency}` : ""}${unitText ? ` / ${unitText}` : ""}`);
    }
    if (value.value != null && (typeof value.value === "string" || typeof value.value === "number")) {
      return cleanText(`${value.value}${currency ? ` ${currency}` : ""}${unitText ? ` / ${unitText}` : ""}`);
    }
    return "";
  }

  function locationFromStructuredValue(value) {
    if (!value) return "";
    if (typeof value === "string") return cleanText(value);
    if (Array.isArray(value)) {
      return uniqueValues(value.map(locationFromStructuredValue), 4).join(" | ");
    }
    return cleanText([
      value.addressLocality,
      value.addressRegion,
      value.streetAddress,
      value.addressCountry,
      value.name,
      value.address && locationFromStructuredValue(value.address),
    ].filter(Boolean).join(" "));
  }

  function companyFromStructuredValue(value) {
    if (!value) return "";
    if (typeof value === "string") return cleanText(value);
    if (Array.isArray(value)) return uniqueValues(value.map(companyFromStructuredValue), 3).join(" | ");
    return cleanText(value.name || value.legalName || value.alternateName || "");
  }

  function textFromStructuredValue(value) {
    function visit(item, seen, depth) {
      if (item === null || item === undefined || depth > 8) return [];
      if (typeof item === "string" || typeof item === "number" || typeof item === "boolean") {
        const text = cleanText(String(item));
        return text ? [text] : [];
      }
      if (Array.isArray(item)) {
        return item.flatMap((entry) => visit(entry, seen, depth + 1));
      }
      if (typeof item !== "object") return [];
      if (seen.has(item)) return [];
      seen.add(item);

      const preferred = [];
      ["name", "description", "text", "value", "label"].forEach((key) => {
        if (Object.prototype.hasOwnProperty.call(item, key)) {
          preferred.push(...visit(item[key], seen, depth + 1));
        }
      });
      if (preferred.length) return preferred;
      return Object.values(item).flatMap((entry) => visit(entry, seen, depth + 1));
    }

    return uniqueValues(visit(value, new Set(), 0), 12).join("\n");
  }

  function tryParseJson(text) {
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (_error) {
      return null;
    }
  }

  function collectStructuredDataObjects(doc) {
    const objects = [];
    queryAllDeep(doc, "script[type='application/ld+json']").forEach((script) => {
      const parsed = tryParseJson(script.textContent || "");
      if (!parsed) return;
      objects.push(parsed);
    });
    return objects;
  }

  function visitStructuredNodes(value, visitor, seen, depth) {
    if (!value || depth > 18) return;
    if (!seen) seen = new Set();
    if (typeof value === "object") {
      if (seen.has(value)) return;
      seen.add(value);
    }
    visitor(value);
    if (Array.isArray(value)) {
      value.forEach((item) => visitStructuredNodes(item, visitor, seen, depth + 1));
      return;
    }
    if (value && typeof value === "object") {
      Object.values(value).forEach((item) => visitStructuredNodes(item, visitor, seen, depth + 1));
    }
  }

  function structuredNodeLooksLikeJob(value) {
    if (!value || typeof value !== "object") return false;
    const typeValue = value["@type"];
    const types = Array.isArray(typeValue) ? typeValue : [typeValue];
    if (types.some((item) => /jobposting/i.test(String(item || "")))) return true;
    return Boolean(
      (value.title || value.name)
      && (value.description || value.jobLocation || value.baseSalary || value.hiringOrganization || value.employmentType)
    );
  }

  function jobFromStructuredNode(value, baseUrl, pageIndex) {
    if (!structuredNodeLooksLikeJob(value)) return null;
    const title = cleanText(value.title || value.name || "");
    const company = companyFromStructuredValue(value.hiringOrganization || value.organization || value.company);
    const salary = salaryFromStructuredValue(value.baseSalary || value.salary);
    const locationText = locationFromStructuredValue(value.jobLocation || value.location || value.address);
    const education = textFromStructuredValue(value.educationRequirements);
    const experience = textFromStructuredValue(value.experienceRequirements || value.qualifications);
    const detailUrl = resolveUrl(value.url || value.sameAs || "", baseUrl);
    const content = cleanText(
      value.description
      || textFromStructuredValue(value.responsibilities)
      || textFromStructuredValue(value.skills)
      || ""
    );
    const text = structuredDetailText({
      title,
      company,
      salary,
      location: locationText,
      education,
      experience,
      content,
    });
    if (!title && !company && !content) return null;
    return normalizeCardFields({
      title,
      company,
      salary,
      location: locationText,
      education,
      experience,
    }, text || content || title, detailUrl || baseUrl || globalThis.location.href);
  }

  function collectStructuredJobsFromDocument(doc, baseUrl, pageIndex) {
    const items = [];
    const seen = new Set();
    collectStructuredDataObjects(doc).forEach((entry) => {
      visitStructuredNodes(entry, (node) => {
        const job = jobFromStructuredNode(node, baseUrl, pageIndex);
        if (!job) return;
        const dedupeKey = `${job.url || ""}|${compact(job.title || job.text).slice(0, 220)}`;
        if (seen.has(dedupeKey)) return;
        seen.add(dedupeKey);
        items.push(normalizeExportedJob({
          ...job,
          pageIndex,
          captureMode: "structured",
          sourceUrl: baseUrl || (globalThis.location && globalThis.location.href) || "",
          rawText: job.text || "",
          score: 24 + scoreJobCard(job.text || "") + (job.salary ? 5 : 0) + (job.company ? 3 : 0),
        }, {
          captureMode: "structured",
          sourceUrl: baseUrl || (globalThis.location && globalThis.location.href) || "",
        }));
      }, new Set(), 0);
    });
    return items;
  }

  function detailFromStructuredData(doc, pageUrl, pageTitle) {
    let best = null;
    collectStructuredDataObjects(doc).forEach((entry) => {
      visitStructuredNodes(entry, (node) => {
        const job = jobFromStructuredNode(node, pageUrl, 1);
        if (!job) return;
        const score = scoreJobCard(job.text || "") + (job.salary ? 4 : 0) + (job.company ? 3 : 0) + (job.location ? 2 : 0);
        if (!best || score > best.score) best = { job, score };
      }, new Set(), 0);
    });
    if (!best) return null;
    const detail = {
      type: "detail",
      title: cleanText(best.job.title || pageTitle || ""),
      company: cleanText(best.job.company || ""),
      salary: cleanText(best.job.salary || ""),
      location: cleanText(best.job.location || ""),
      experience: cleanText(best.job.experience || ""),
      education: cleanText(best.job.education || ""),
      url: best.job.url || pageUrl,
      savedAt: new Date().toISOString(),
      content: cleanText(best.job.text || bodyText(doc)),
    };
    detail.text = structuredDetailText(detail);
    return detail;
  }

  function removeNoisyNodes(root) {
    if (!root) return;
    [
      "script",
      "style",
      "noscript",
      "svg",
      "canvas",
      "nav",
      "footer",
      "aside",
      "[role='navigation']",
      "[role='complementary']",
      "[aria-hidden='true']",
      ".advertisement",
      ".ads",
      ".ad",
      ".banner",
      ".cookie",
      ".modal-mask",
      ".drawer-mask",
    ].forEach((selector) => {
      if (typeof root.querySelectorAll !== "function") return;
      root.querySelectorAll(selector).forEach((node) => node.remove());
    });
  }

  async function waitForDomSettled(timeoutMs, idleMs) {
    const start = Date.now();
    const root = document.documentElement || document.body;
    if (!root) return;

    if (document.readyState === "loading") {
      await new Promise((resolve) => {
        const done = () => {
          document.removeEventListener("readystatechange", onReady);
          resolve();
        };
        const onReady = () => {
          if (document.readyState !== "loading") done();
        };
        document.addEventListener("readystatechange", onReady);
        setTimeout(done, Math.max(1200, timeoutMs || 2500));
      });
    }

    let lastMutationAt = Date.now();
    const observer = new MutationObserver(() => {
      lastMutationAt = Date.now();
    });
    observer.observe(root, {
      subtree: true,
      childList: true,
      characterData: true,
      attributes: true,
    });
    try {
      while (Date.now() - start < Math.max(800, timeoutMs || 2400)) {
        if (Date.now() - lastMutationAt >= Math.max(250, idleMs || 500)) break;
        await new Promise((resolve) => setTimeout(resolve, 120));
      }
    } finally {
      observer.disconnect();
    }
  }

  function findScrollableContainers(doc) {
    const candidates = [];
    queryAllDeep(doc, "main, section, div, ul").forEach((node) => {
      if (!(node instanceof HTMLElement)) return;
      const style = window.getComputedStyle(node);
      if (!/(auto|scroll)/.test(`${style.overflow} ${style.overflowY}`)) return;
      if (node.clientHeight < 160) return;
      if (node.scrollHeight < node.clientHeight + 120) return;
      let score = 0;
      const cls = (node.className || "").toString().toLowerCase();
      if (/job|list|card|search|recommend|result|wrap|container/.test(cls)) score += 6;
      score += Math.min(8, Math.floor((node.scrollHeight - node.clientHeight) / 600));
      if (nodeText(node).length >= 60) score += 2;
      candidates.push({ node, score });
    });
    candidates.sort((a, b) => b.score - a.score);
    return candidates.slice(0, 4).map((item) => item.node);
  }

  async function scrollScrollableContainers(containers, signal) {
    let moved = false;
    for (const container of containers || []) {
      throwIfAborted(signal);
      if (!(container instanceof HTMLElement)) continue;
      const before = container.scrollTop;
      const maxTop = Math.max(0, container.scrollHeight - container.clientHeight);
      const step = Math.max(320, Math.floor(container.clientHeight * 0.82));
      const target = Math.min(before + step, maxTop);
      container.scrollTop = target;
      if (container.scrollTop !== before) moved = true;
      if (container.scrollTop !== before) {
        await waitForDomSettled(1400, 260);
      }
    }
    if (moved) {
      await pause(250, signal);
    }
    return moved;
  }

  const SITE_LIST_CONFIGS = {
    boss: {
      cards: [
        ".job-card-wrapper",
        ".job-card-box",
        ".search-job-result li",
        ".rec-job-list li",
        "[ka='search_list_item']",
      ],
      title: [".job-name", ".job-title", "[class*='job-name']", "[class*='job-title']"],
      company: [".company-name", ".boss-name", ".brand-name", "[class*='company-name']"],
      salary: [".salary", "[class*='salary']"],
      location: [".job-area", ".company-location", "[class*='job-area']"],
      meta: [".job-info span", ".tag-list li", ".labels-tag span", ".info-public em"],
      anchors: ["a[href]"],
    },
    zhaopin: {
      cards: [
        ".joblist-box__item",
        ".positionlist__list .joblist-box__item",
        ".positionlist__list li",
        "[class*='joblist-box__item']",
      ],
      title: [".jobinfo__name", ".jobtitle", "[class*='job-name']", "[class*='position-name']", "a[title]"],
      company: [".company__name", ".company-title", "[class*='company-name']", "[class*='brand-name']"],
      salary: [".jobinfo__salary", ".salary", "[class*='salary']"],
      location: [".jobinfo__other-info span", ".job-area", "[class*='job-area']"],
      meta: [".jobinfo__other-info span", ".labels__list span", ".jobinfo__tag span"],
      anchors: ["a[href]"],
    },
    liepin: {
      cards: [
        ".job-card-pc-container",
        ".job-card-box",
        ".sojob-item-main",
        ".job-list-box li",
        ".job-item",
      ],
      title: [".job-title", ".ellipsis-1", "[class*='job-title']", "a[title]"],
      company: [".company-name", ".company-nick-name", "[class*='company-name']"],
      salary: [".job-salary", ".salary", "[class*='salary']"],
      location: [".job-dq", ".job-labels-box span", "[class*='job-dq']"],
      meta: [".job-other span", ".labels-box span", ".job-labels-box span"],
      anchors: ["a[href]"],
    },
    job51: {
      cards: [
        ".joblist-item",
        ".joblist-item-top",
        ".el",
        ".j_joblist .e",
        ".job-item",
      ],
      title: [".jobname", ".jname", ".job-title", ".job-title a", ".joblist-item-jobname", ".e_job_name", "a[title]"],
      company: [".cname", ".company_name", ".company-name", ".dc", ".e_company_name", "[class*='company']"],
      salary: [".sal", ".salary", ".job-salary", ".e_salary", "[class*='salary']"],
      location: [".d.at span", ".d.at", ".area", ".job-area", ".workarea", ".e_area", "[class*='area']"],
      meta: [".tags span", ".attribute span", ".labels span", ".joblist-item-tags span", ".job-info span"],
      anchors: ["a[href]"],
    },
    shixiseng: {
      cards: [
        ".intern-wrap",
        ".intern-item",
        ".position-item",
        ".position-list-item",
        ".job-item",
      ],
      title: [".intern-title", ".job-name", ".position-name", ".title.ellipsis", ".job-title", ".name-box a", "a[title]"],
      company: [".company-name", ".company", ".company-title", ".title.ellipsis", ".com-name", "[class*='company-name']"],
      salary: [".day-salary", ".salary", ".job_money", ".money", "[class*='salary']"],
      location: [".area", ".city", ".addr", ".job_position", "[class*='city']", "[class*='location']"],
      meta: [".more span", ".meta span", ".job-msg span", ".job-info span", ".job_academic span", ".job_comment span"],
      anchors: ["a[href]"],
    },
  };

  const SITE_DETAIL_CONFIGS = {
    boss: {
      title: [".job-name", ".name", "[class*='job-name']"],
      company: [".company-name", ".name", ".company-info a"],
      salary: [".salary", "[class*='salary']"],
      meta: [".job-info span", ".job-primary .tag-list li", ".job-detail-box .tag-list li"],
      content: [".job-sec-text", ".job-detail-section", ".job-detail-box", ".job-detail-content", ".job-card-body"],
    },
    zhaopin: {
      title: [".job-detail__title", ".job-name", "h1"],
      company: [".company__title", ".company-name", ".company-brand a"],
      salary: [".job-detail__salary", ".salary", "[class*='salary']"],
      meta: [".job-detail__requirements span", ".summary-plane__info span", ".describtion__labels span"],
      content: [".jobdetail-box", ".describtion__detail-content", ".job-detail", ".job-desc"],
    },
    liepin: {
      title: [".title-new", ".job-title-box h1", "h1"],
      company: [".company-name", ".company-title", ".company-info a"],
      salary: [".job-salary", ".salary"],
      meta: [".basic-infor span", ".job-qualifications-box span", ".job-title-box .labels span"],
      content: [".job-detail-content", ".content-word", ".job-description-container", ".job-item-main"],
    },
    job51: {
      title: [".job-title", ".jobname", ".jname", "h1", ".cn h1"],
      company: [".company-name", ".com-name", ".company_name", ".com_tag a"],
      salary: [".salary", ".job-salary", ".sal", ".cn strong"],
      meta: [".job-msg span", ".job-tags span", ".jtag span", ".d.at span", ".attribute span"],
      content: [".jobdetail-box", ".bmsg", ".jobdetail", ".job-description", ".job_msg", ".tCompany_main"],
    },
    shixiseng: {
      title: [".new_job_name", ".job_name", ".intern-title", ".title.ellipsis", "h1"],
      company: [".com-name", ".company_name", ".enterprise_name", ".company-name", ".company"],
      salary: [".job_money", ".salary", ".day_salary", ".day-salary"],
      meta: [".job_academic span", ".job_comment span", ".job_msg span", ".more span", ".meta span"],
      content: [".job_detail", ".desc", ".job_intro", ".job-content", ".job_detail_module"],
    },
  };

  function isLikelyJobUrl(href, baseUrl) {
    if (!href || /^(javascript:|mailto:|tel:)/i.test(href)) return false;
    try {
      const url = new URL(href, baseUrl || location.href);
      const value = `${url.hostname}${url.pathname}${url.search}`.toLowerCase();
      if (/\/(search|list|index|home)(\/|\?|$)/i.test(url.pathname) && !/(job|position|intern)/i.test(value)) return false;
      return /(job_detail|\/job\/|jobs\.|\/intern\/|\/interns\/inn_|\/position\/|\/positions\/|jobid|job_id|positionid|postid|recruitid|campus\/position|xiaoyuan)/i.test(value);
    } catch (_error) {
      return false;
    }
  }

  function usableDetailUrl(url, sourceUrl) {
    if (!url || /^(javascript:|mailto:|tel:)/i.test(url)) return false;
    try {
      const parsed = new URL(url, sourceUrl || location.href);
      if (!/^https?:$/.test(parsed.protocol)) return false;
      if (sourceUrl && parsed.href.split("#")[0] === sourceUrl.split("#")[0]) return false;
      const value = `${parsed.hostname}${parsed.pathname}${parsed.search}`.toLowerCase();
      if (/\/(search|list|index|home)(\/|\?|$)/i.test(parsed.pathname) && !/(job|position|intern)/i.test(value)) return false;
      return /(job_detail|\/job\/|jobs\.|\/intern\/|jobid|job_id|positionid|postid|recruitid|campus\/position|xiaoyuan)/i.test(value);
    } catch (_error) {
      return false;
    }
  }

  function scoreJobCard(text) {
    const keywords = ["岗位", "职位", "薪资", "公司", "实习", "全职", "经验", "本科", "硕士"];
    let score = 0;
    keywords.forEach((keyword) => { if (String(text || "").includes(keyword)) score += 1; });
    if (findSalary(text)) score += 6;
    if (String(text || "").length >= 35 && String(text || "").length <= 2400) score += 2;
    return score;
  }

  function normalizeCardFields(fields, text, url) {
    const fallback = fallbackFieldsFromText(text);
    const merged = {
      title: cleanText(fields.title || fallback.title).slice(0, 120),
      company: cleanText(fields.company || fallback.company).slice(0, 120),
      salary: cleanText(fields.salary || fallback.salary),
      location: cleanText(fields.location || fallback.location),
      education: cleanText(fields.education || fallback.education),
      experience: cleanText(fields.experience || fallback.experience),
      url: url || "",
      text: cleanText(text),
    };
    if (!merged.title && merged.company && merged.text) {
      merged.title = merged.text.split(/\n+/)[0].slice(0, 120);
    }
    return merged;
  }

  function parseMeta(metaTexts, text) {
    const values = uniqueValues(metaTexts, 12);
    const combined = values.join(" | ");
    return {
      location: values.find((item) => findLocation(item)) || findLocation(combined || text),
      education: values.find((item) => findEducation(item)) || findEducation(combined || text),
      experience: values.find((item) => findExperience(item)) || findExperience(combined || text),
    };
  }

  function collectSiteJobsFromDocument(doc, baseUrl, pageIndex, useVisibility) {
    const siteKey = siteKeyFromHost(baseUrl);
    const config = SITE_LIST_CONFIGS[siteKey];
    if (!config) return [];
    const items = [];
    const seen = new Set();
    const visibleCheck = useVisibility ? isVisible : () => true;
    for (const selector of config.cards) {
      queryAllDeep(doc, selector).forEach((node) => {
        if (!(node instanceof Element) || !visibleCheck(node)) return;
        const text = nodeText(node);
        if (text.length < 20 || text.length > 5000) return;
        const href = bestAnchorHref(node, config.anchors, baseUrl);
        const metaTexts = textsBySelectors(node, config.meta, 12);
        const meta = parseMeta(metaTexts, text);
        const fields = normalizeCardFields({
          title: textBySelectors(node, config.title) || textBySelectors(node, config.title, { attribute: "title" }),
          company: textBySelectors(node, config.company),
          salary: textBySelectors(node, config.salary) || findSalary(text),
          location: textBySelectors(node, config.location) || meta.location,
          education: meta.education,
          experience: meta.experience,
        }, text, href);
        const dedupeKey = `${fields.url || ""}|${compact(fields.title || fields.text).slice(0, 180)}`;
        if (seen.has(dedupeKey)) return;
        seen.add(dedupeKey);
        const score = 20 + scoreJobCard(text) + (fields.title ? 4 : 0) + (fields.company ? 4 : 0) + (fields.salary ? 5 : 0);
        items.push({ ...fields, score, pageIndex });
      });
    }
    return items;
  }

  function bestAncestor(node) {
    let current = node;
    let best = node;
    for (let i = 0; i < 8 && current && current !== document.body; i += 1) {
      const text = nodeText(current);
      const bestText = nodeText(best);
      if (text.length >= 40 && text.length <= 2200 && scoreJobCard(text) >= scoreJobCard(bestText)) {
        best = current;
      }
      current = current.parentElement;
    }
    return best;
  }

  function collectGenericJobsFromDocument(doc, baseUrl, pageIndex, useVisibility) {
    const selector = [
      "[class*='job']",
      "[class*='Job']",
      "[class*='position']",
      "[class*='Position']",
      "[class*='card']",
      "[class*='Card']",
      "[class*='item']",
      "[class*='Item']",
      "[class*='list']",
      "[data-jobid]",
      "[data-jid]",
      "[data-position-id]",
      "[data-job-id]",
      "[role='listitem']",
      "li",
      "article",
    ].join(",");
    const bucket = [];
    const localSeen = new Set();
    const visibleCheck = useVisibility ? isVisible : () => true;

    function addCandidate(node, forcedUrl) {
      if (!node || !visibleCheck(node)) return;
      const text = nodeText(node);
      if (text.length < 8 || text.length > 5000) return;
      const score = scoreJobCard(text);
      if (score < 3 && !forcedUrl) return;
      bucket.push({ node, text, score: forcedUrl ? score + 8 : score, forcedUrl: forcedUrl || "" });
    }

    queryAllDeep(doc, selector).slice(0, 600).forEach((node) => addCandidate(node, ""));
    queryAllDeep(doc, "a[href]").slice(0, 600).forEach((anchor) => {
      const href = anchor.getAttribute("href") || "";
      if (!isLikelyJobUrl(href, baseUrl)) return;
      const resolved = resolveUrl(href, baseUrl);
      const container = doc === document ? bestAncestor(anchor) : (anchor.closest("article, li, section, div") || anchor.parentElement || anchor);
      addCandidate(container, resolved);
    });

    return bucket
      .sort((a, b) => b.score - a.score)
      .map(({ node, text, score, forcedUrl }) => {
        const linkNode = node.querySelector("a[href]") || node.closest("a[href]");
        const href = forcedUrl || (linkNode ? resolveUrl(linkNode.getAttribute("href"), baseUrl) : "");
        const dedupeKey = `${href || ""}|${compact(text).slice(0, 260)}`;
        if (localSeen.has(dedupeKey)) return null;
        localSeen.add(dedupeKey);
        const linkText = cleanText((linkNode && nodeText(linkNode)) || "");
        const fields = normalizeCardFields({
          title: textBySelectors(node, ["[class*='job-name']", "[class*='position-name']", "[class*='job-title']", "h1", "h2", "h3", "[class*='title']"]) || linkText,
          company: textBySelectors(node, ["[class*='company']", "[class*='Company']", "[class*='brand']", "[class*='corp']"]),
          salary: findSalary(text),
          location: findLocation(text),
          education: findEducation(text),
          experience: findExperience(text),
        }, text, href);
        return { ...fields, score, pageIndex };
      })
      .filter(Boolean);
  }

  function collectJobsFromDocument(doc, baseUrl, pageIndex, useVisibility) {
    const siteJobs = collectSiteJobsFromDocument(doc, baseUrl, pageIndex, useVisibility);
    const genericJobs = collectGenericJobsFromDocument(doc, baseUrl, pageIndex, useVisibility);
    const structuredJobs = collectStructuredJobsFromDocument(doc, baseUrl, pageIndex);
    const merged = [];
    const seen = new Set();
    [...siteJobs, ...genericJobs, ...structuredJobs].forEach((job) => {
      const key = `${job.url || ""}|${compact(job.title || job.text).slice(0, 180)}`;
      if (seen.has(key)) return;
      seen.add(key);
      merged.push(job);
    });
    return merged;
  }

  function structuredDetailText(detail) {
    const sections = [
      detail.title ? `岗位：${detail.title}` : "",
      detail.company ? `公司：${detail.company}` : "",
      detail.salary ? `薪资：${detail.salary}` : "",
      detail.location ? `地点：${detail.location}` : "",
      detail.experience ? `经验：${detail.experience}` : "",
      detail.education ? `学历：${detail.education}` : "",
      detail.content || "",
    ].filter(Boolean);
    return cleanText(sections.join("\n"));
  }

  function extractSiteDetail(doc, pageUrl, pageTitle) {
    const siteKey = siteKeyFromHost(pageUrl);
    const config = SITE_DETAIL_CONFIGS[siteKey];
    if (!config) return null;
    const pageText = bodyText(doc);
    const metaTexts = textsBySelectors(doc, config.meta, 18);
    const meta = parseMeta(metaTexts, pageText);
    const contentBlocks = uniqueValues(
      textsBySelectors(doc, config.content, 10).filter((item) => item.length >= 80),
      6,
    );
    const detail = {
      type: "detail",
      title: cleanText(textBySelectors(doc, config.title) || pageTitle || doc.title || ""),
      company: cleanText(textBySelectors(doc, config.company)),
      salary: cleanText(textBySelectors(doc, config.salary) || findSalary(pageText)),
      location: cleanText(meta.location),
      experience: cleanText(meta.experience),
      education: cleanText(meta.education),
      url: pageUrl,
      savedAt: new Date().toISOString(),
      content: cleanText(contentBlocks.join("\n\n") || pageText),
    };
    detail.text = structuredDetailText(detail);
    return detail;
  }

  function extractGenericDetail(doc, pageUrl, pageTitle) {
    if (!doc || !doc.body) {
      const fallbackText = cleanText(doc && doc.documentElement ? nodeText(doc.documentElement) : "");
      const detail = {
        type: "detail",
        title: cleanText(pageTitle || (doc && doc.title) || ""),
        company: "",
        salary: findSalary(fallbackText),
        location: findLocation(fallbackText),
        experience: findExperience(fallbackText),
        education: findEducation(fallbackText),
        url: pageUrl,
        savedAt: new Date().toISOString(),
        content: fallbackText,
      };
      detail.text = structuredDetailText(detail);
      return detail;
    }
    const clone = doc.body.cloneNode(true);
    removeNoisyNodes(clone);
    const candidates = Array.from(
      clone.querySelectorAll("main, article, section, [class*='job'], [class*='Job'], [class*='detail'], [class*='position'], [class*='description']")
    )
      .map((node) => nodeText(node))
      .filter((text) => text.length > 120)
      .sort((a, b) => b.length - a.length)
      .slice(0, 8);
    const fallback = cleanText(nodeText(clone) || bodyText(doc));
    const detail = {
      type: "detail",
      title: cleanText(pageTitle || doc.title || ""),
      company: "",
      salary: findSalary(fallback),
      location: findLocation(fallback),
      experience: findExperience(fallback),
      education: findEducation(fallback),
      url: pageUrl,
      savedAt: new Date().toISOString(),
      content: cleanText(candidates.length ? candidates.join("\n\n") : fallback),
    };
    detail.text = structuredDetailText(detail);
    return detail;
  }

  function extractDetailFromDocument(doc, pageUrl, pageTitle) {
    return detailFromStructuredData(doc, pageUrl, pageTitle)
      || extractSiteDetail(doc, pageUrl, pageTitle)
      || extractGenericDetail(doc, pageUrl, pageTitle);
  }

  function currentPageDetailPayloadIfStrong() {
    if (!isLikelyJobUrl(location.href, location.href)) return null;
    const detail = extractDetailFromDocument(document, location.href, document.title || "");
    const detailText = cleanText(detail && detail.text);
    if (detailText.length < 80) return null;
    return normalizeExportedDetailPayload(detail, { captureMode: "detail", sourceUrl: location.href });
  }

  function findNextHref(doc, baseUrl) {
    const candidates = [];
    queryAllDeep(doc, "a[href], button[data-url], [rel='next']").forEach((node) => {
      const text = nodeText(node);
      const rel = (node.getAttribute("rel") || "").toLowerCase();
      const aria = (node.getAttribute("aria-label") || "").toLowerCase();
      const cls = (node.className || "").toString().toLowerCase();
      const href = node.getAttribute("href") || node.getAttribute("data-url") || "";
      let score = 0;
      if (text === "下一页" || text === "下页") score += 10;
      if (text === ">" || text === "›" || text === "»") score += 8;
      if (text.toLowerCase() === "next") score += 8;
      if (aria.includes("next") || aria.includes("下一页")) score += 8;
      if (rel === "next") score += 8;
      if (cls.includes("next")) score += 6;
      if (score > 0 && href) candidates.push({ href: resolveUrl(href, baseUrl), score });
    });
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0] ? candidates[0].href : "";
  }

  function buildPagedUrl(baseUrl, pageNumber) {
    try {
      const url = new URL(baseUrl || location.href, location.href);
      const siteKey = siteKeyFromHost(url.href);
      const parameterMap = {
        boss: "page",
        zhaopin: "p",
        liepin: "currentPage",
        job51: "pageno",
        shixiseng: "page",
      };
      const parameterName = parameterMap[siteKey];
      if (!parameterName) return "";
      url.searchParams.set(parameterName, String(pageNumber));
      return url.href;
    } catch (_error) {
      return "";
    }
  }

  function findNextAction(doc) {
    const candidates = [];
    queryAllDeep(doc, "a[href], button, [role='button'], [rel='next'], .next, [class*='next']").forEach((node) => {
      if (!(node instanceof Element) || !isVisible(node)) return;
      const text = nodeText(node);
      const rel = (node.getAttribute("rel") || "").toLowerCase();
      const aria = (node.getAttribute("aria-label") || "").toLowerCase();
      const cls = (node.className || "").toString().toLowerCase();
      const href = node.getAttribute("href") || node.getAttribute("data-url") || "";
      const disabled = node.getAttribute("disabled") !== null || node.getAttribute("aria-disabled") === "true" || cls.includes("disabled");
      if (disabled) return;
      let score = 0;
      if (text === "下一页" || text === "下页") score += 12;
      if (text === ">" || text === "›" || text === "»") score += 8;
      if (text.toLowerCase() === "next") score += 8;
      if (aria.includes("next") || aria.includes("下一页")) score += 8;
      if (rel === "next") score += 8;
      if (cls.includes("next")) score += 6;
      if (score <= 0) return;
      candidates.push({ node, href, score });
    });
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0] || null;
  }

  function findLoadMoreAction(doc) {
    const candidates = [];
    queryAllDeep(doc, "button, a[href], [role='button'], [class*='more'], [class*='load']").forEach((node) => {
      if (!(node instanceof Element) || !isVisible(node)) return;
      const text = nodeText(node);
      const cls = (node.className || "").toString().toLowerCase();
      const aria = (node.getAttribute("aria-label") || "").toLowerCase();
      const disabled = node.getAttribute("disabled") !== null || node.getAttribute("aria-disabled") === "true" || cls.includes("disabled");
      if (disabled) return;
      let score = 0;
      if (/加载更多|更多职位|展开更多|显示更多|查看更多/.test(text)) score += 12;
      if (/load more|show more|view more|more jobs/.test(text.toLowerCase())) score += 12;
      if (cls.includes("load-more") || cls.includes("show-more") || cls.includes("more")) score += 6;
      if (aria.includes("more")) score += 4;
      if (score > 0) candidates.push({ node, score });
    });
    candidates.sort((a, b) => b.score - a.score);
    return candidates[0] || null;
  }

  async function clickLoadMoreInPlace(maxWaitMs, signal) {
    throwIfAborted(signal);
    const candidate = findLoadMoreAction(document);
    if (!candidate || !(candidate.node instanceof HTMLElement)) return false;
    const beforeSnapshot = compact(bodyText(document)).slice(0, 1800);
    const beforeHeight = Math.max(
      document.body ? document.body.scrollHeight : 0,
      document.documentElement ? document.documentElement.scrollHeight : 0
    );
    candidate.node.scrollIntoView({ block: "center", inline: "center" });
    candidate.node.click();
    await waitForDomSettled(Math.min(3000, maxWaitMs || 3000), 500);
    const started = Date.now();
    while (Date.now() - started < maxWaitMs) {
      await pause(300, signal);
      const afterSnapshot = compact(bodyText(document)).slice(0, 1800);
      const afterHeight = Math.max(
        document.body ? document.body.scrollHeight : 0,
        document.documentElement ? document.documentElement.scrollHeight : 0
      );
      if (afterSnapshot !== beforeSnapshot || afterHeight > beforeHeight + 40) {
        await waitForDomSettled(2600, 500);
        return true;
      }
    }
    return false;
  }

  function mergeJobs(target, incoming, seen, maxJobs) {
    let added = 0;
    (incoming || []).forEach((job) => {
      if (maxJobs && target.length >= maxJobs) return;
      const key = `${job.url || ""}|${compact(job.title || job.text).slice(0, 260)}`;
      if (seen.has(key)) return;
      seen.add(key);
      target.push(job);
      added += 1;
    });
    return added;
  }

  async function collectJobsWhileScrolling(targetJobs, seen, options) {
    const signal = options && options.signal;
    const siteKey = siteKeyFromHost(location.href);
    const maxScrollRounds = Math.max(4, Number(options.maxScrollRounds) || (siteKey === "boss" ? 32 : 18));
    const maxJobs = Math.max(1, Number(options.maxJobs) || 2000);
    let stagnantRounds = 0;
    let lastHeight = 0;
    const scrollContainers = findScrollableContainers(document);

    for (let round = 0; round < maxScrollRounds; round += 1) {
      throwIfAborted(signal);
      const added = mergeJobs(targetJobs, collectJobsFromDocument(document, location.href, 1, true), seen, maxJobs);
      if (targetJobs.length >= maxJobs) break;
      const currentHeight = Math.max(
        document.body ? document.body.scrollHeight : 0,
        document.documentElement ? document.documentElement.scrollHeight : 0
      );
      if (round < maxScrollRounds - 1) {
        const movedContainers = await scrollScrollableContainers(scrollContainers, signal);
        window.scrollTo(0, currentHeight || document.body.scrollHeight || 0);
        await waitForDomSettled(2600, 450);
        const loadedMore = await clickLoadMoreInPlace(2400, signal);
        const newHeight = Math.max(
          document.body ? document.body.scrollHeight : 0,
          document.documentElement ? document.documentElement.scrollHeight : 0
        );
        const grew = newHeight > currentHeight + 40 || currentHeight > lastHeight + 40;
        if (added <= 0 && !grew && !loadedMore && !movedContainers) {
          stagnantRounds += 1;
        } else {
          stagnantRounds = 0;
        }
        lastHeight = Math.max(lastHeight, newHeight, currentHeight);
        if (stagnantRounds >= 2) break;
      }
    }
    window.scrollTo(0, 0);
  }

  async function clickNextPageInPlace(maxWaitMs, signal) {
    throwIfAborted(signal);
    const candidate = findNextAction(document);
    if (!candidate || !(candidate.node instanceof HTMLElement)) return false;
    const beforeUrl = location.href;
    const beforeSnapshot = compact(bodyText(document)).slice(0, 1200);
    candidate.node.scrollIntoView({ block: "center", inline: "center" });
    candidate.node.click();
    await waitForDomSettled(2800, 500);
    const started = Date.now();
    while (Date.now() - started < maxWaitMs) {
      await pause(350, signal);
      const afterUrl = location.href;
      const afterSnapshot = compact(bodyText(document)).slice(0, 1200);
      if ((afterUrl && afterUrl !== beforeUrl) || (afterSnapshot && afterSnapshot !== beforeSnapshot)) {
        await waitForDomSettled(3000, 700);
        return true;
      }
    }
    return false;
  }

  async function fetchDocument(url, signal, timeoutMs) {
    throwIfAborted(signal);
    const controller = new AbortController();
    const timeout = Math.max(3000, Number(timeoutMs) || 9000);
    const timeoutId = setTimeout(() => controller.abort(), timeout);
    const abortFromParent = () => controller.abort();
    if (signal && typeof signal.addEventListener === "function") {
      signal.addEventListener("abort", abortFromParent, { once: true });
    }
    try {
      const response = await fetch(url, { credentials: "include", signal: controller.signal });
      if (!response.ok) throw new Error(`打开 ${url} 失败 (${response.status})`);
      const html = await response.text();
      const doc = new DOMParser().parseFromString(html, "text/html");
      return { doc, url: response.url || url, title: doc.title || "" };
    } finally {
      clearTimeout(timeoutId);
      if (signal && typeof signal.removeEventListener === "function") {
        signal.removeEventListener("abort", abortFromParent);
      }
    }
  }

  async function collectCurrentAndNextPages(options) {
    const signal = options && options.signal;
    const allJobs = [];
    const seen = new Set();
    const maxPages = Math.max(1, Number(options.maxPages) || 8);
    const maxJobs = Math.max(1, Number(options.maxJobs) || 2000);
    let lastTitle = document.title || "";
    let lastUrl = location.href;

    throwIfAborted(signal);
    await waitForDomSettled(3000, 600);
    await collectJobsWhileScrolling(allJobs, seen, options);

    let nextHref = findNextHref(document, location.href);
    let pageIndex = 2;
    while (pageIndex <= maxPages && allJobs.length < maxJobs) {
      throwIfAborted(signal);
      let clicked = false;
      if (!nextHref) {
        clicked = await clickNextPageInPlace(6000, signal);
        if (!clicked) {
          const pagedUrl = buildPagedUrl(lastUrl || location.href, pageIndex);
          if (!pagedUrl || pagedUrl === lastUrl || pagedUrl === location.href) break;
          nextHref = pagedUrl;
        }
      }
      if (nextHref) {
        const current = new URL(location.href);
        const next = new URL(nextHref, location.href);
        if (current.origin !== next.origin) break;
        const fetched = await fetchDocument(next.href, signal, 9000);
        lastTitle = fetched.title || lastTitle;
        lastUrl = fetched.url || lastUrl;
        mergeJobs(allJobs, collectJobsFromDocument(fetched.doc, fetched.url, pageIndex, false), seen, maxJobs);
        nextHref = findNextHref(fetched.doc, fetched.url) || buildPagedUrl(fetched.url, pageIndex + 1);
        pageIndex += 1;
        continue;
      }
      if (clicked) {
        lastTitle = document.title || lastTitle;
        lastUrl = location.href || lastUrl;
        await collectJobsWhileScrolling(allJobs, seen, options);
        nextHref = findNextHref(document, location.href);
        pageIndex += 1;
        continue;
      }
      break;
    }
    return { allJobs, lastTitle, lastUrl };
  }

  async function enrichWithDetailPages(allJobs, listUrl, detailLimit) {
    const signal = arguments[3];
    const detailConcurrency = Math.max(1, Math.min(6, Number(arguments[4]) || 4));
    const currentOrigin = new URL(location.href).origin;
    let enrichedCount = 0;
    const detailJobs = allJobs
      .map((job, index) => ({ job, index }))
      .filter(({ job }) => usableDetailUrl(job.url, listUrl))
      .filter(({ job }) => {
        try {
          return new URL(job.url, listUrl).origin === currentOrigin;
        } catch (_error) {
          return false;
        }
      })
      .slice(0, Math.max(0, Number(detailLimit) || 0));

    let cursor = 0;
    async function enrichNextDetail() {
      while (cursor < detailJobs.length) {
      throwIfAborted(signal);
      const { job, index } = detailJobs[cursor];
      cursor += 1;
      try {
        const fetched = await fetchDocument(job.url, signal, 8000);
        const detail = extractDetailFromDocument(fetched.doc, fetched.url, fetched.title);
        const detailUrl = usableDetailUrl(detail.url || fetched.url || job.url, listUrl)
          ? (detail.url || fetched.url || job.url)
          : "";
        const detailText = cleanText(detail.text || "");
        if (!detailUrl || detailText.length < 30) {
          allJobs[index] = {
            ...job,
            detailFetched: false,
            detailFetchError: "empty_detail_page",
          };
          continue;
        }
        allJobs[index] = {
          ...job,
          title: detail.title || job.title,
          company: detail.company || job.company,
          salary: detail.salary || job.salary,
          location: detail.location || job.location,
          education: detail.education || job.education,
          experience: detail.experience || job.experience,
          detailTitle: detail.title || "",
          detailUrl,
          url: detailUrl,
          detailFetched: true,
          detailText,
        };
        allJobs[index].text = [job.text || "", "", "Detail page:", detailText].filter(Boolean).join("\n");
        enrichedCount += 1;
      } catch (_error) {
        allJobs[index] = {
          ...job,
          detailFetched: false,
          detailFetchError: "fetch_failed",
        };
      }
    }
    }
    await Promise.all(Array.from({ length: Math.min(detailConcurrency, detailJobs.length) }, enrichNextDetail));
    return enrichedCount;
  }

  async function collectDetailPayload(options) {
    await waitForDomSettled(2800, 500);
    throwIfAborted(options && options.signal);
    return normalizeExportedDetailPayload(
      extractDetailFromDocument(document, location.href, document.title || ""),
      { captureMode: "detail", sourceUrl: location.href }
    );
  }

  async function collectListPayload(options) {
    const signal = options && options.signal;
    const normalized = {
      maxPages: Math.max(1, Number(options && options.maxPages) || 2),
      maxScrollRounds: Math.max(2, Number(options && (options.maxScrollRounds ?? options.scrollSteps)) || 6),
      maxJobs: Math.max(1, Number(options && options.maxJobs) || 2000),
      detailLimit: Math.max(0, Number(options && options.detailLimit) || 0),
      detailConcurrency: Math.max(1, Number(options && options.detailConcurrency) || 4),
      signal,
    };
    const { allJobs, lastTitle, lastUrl } = await collectCurrentAndNextPages(normalized);
    const limitedJobs = allJobs.slice(0, normalized.maxJobs);
    const detailCount = normalized.detailLimit > 0
      ? await enrichWithDetailPages(limitedJobs, lastUrl || location.href, Math.min(normalized.detailLimit, normalized.maxJobs), signal, normalized.detailConcurrency)
      : 0;
    const finalJobs = normalized.detailLimit > 0
      ? limitedJobs.map((job) => {
        if (!job) return job;
        if (job.detailFetched === true) return job;
        return {
          ...job,
          detailFetched: false,
        };
      })
      : limitedJobs;
    const capturedAt = new Date().toISOString();
    const sourceUrl = lastUrl || location.href;
    const exportedJobs = normalizeExportedJobs(finalJobs, {
      captureMode: "list",
      sourceUrl,
      capturedAt,
    });
    return {
      schemaVersion: CAPTURE_SCHEMA_VERSION,
      type: "list_paginated_with_details",
      captureMode: "list",
      sourceSite: sourceSiteFromUrl(sourceUrl),
      title: lastTitle || document.title || "paginated_jobs_with_details",
      url: sourceUrl,
      sourceUrl,
      savedAt: capturedAt,
      capturedAt,
      jobCount: exportedJobs.length,
      cardCount: limitedJobs.length,
      detailRequired: normalized.detailLimit > 0,
      detailCount,
      jobs: exportedJobs,
      text: cleanText((document.body && document.body.innerText) || "").slice(0, 120000),
    };
  }

  async function collectFastVisiblePayload(options) {
    const signal = options && options.signal;
    const maxJobs = Math.max(1, Number(options && options.maxJobs) || 300);
    throwIfAborted(signal);
    await waitForDomSettled(1200, 250);
    const jobs = collectJobsFromDocument(document, location.href, 1, true).slice(0, maxJobs);
    const text = cleanText((document.body && document.body.innerText) || bodyText(document)).slice(0, 120000);
    const capturedAt = new Date().toISOString();
    if (jobs.length) {
      const exportedJobs = normalizeExportedJobs(jobs, {
        captureMode: "fast_visible",
        sourceUrl: location.href,
        capturedAt,
      });
      return {
        schemaVersion: CAPTURE_SCHEMA_VERSION,
        type: "list_visible_fast",
        captureMode: "fast_visible",
        sourceSite: sourceSiteFromUrl(location.href),
        title: document.title || "visible_jobs",
        url: location.href,
        sourceUrl: location.href,
        savedAt: capturedAt,
        capturedAt,
        jobCount: exportedJobs.length,
        detailCount: 0,
        jobs: exportedJobs,
        text,
      };
    }
    return {
      ...normalizeExportedDetailPayload({
        type: "detail_visible_fast",
        title: document.title || "visible_page",
        url: location.href,
        sourceUrl: location.href,
        savedAt: capturedAt,
        capturedAt,
        text,
        detailText: text,
        rawText: text,
      }, { captureMode: "fast_visible_detail", sourceUrl: location.href, capturedAt }),
      type: "detail_visible_fast",
      savedAt: capturedAt,
    };
  }

  async function collectAutoPayload(options) {
    const signal = options && options.signal;
    const fastMode = options && options.fastMode;
    const wantsDetails = Math.max(0, Number(options && options.detailLimit) || 0) > 0;
    throwIfAborted(signal);
    const currentDetail = currentPageDetailPayloadIfStrong();
    if (currentDetail) return currentDetail;
    if (!options || fastMode !== false) {
      const quickPayload = await collectFastVisiblePayload(options || {});
      const quickJobCount = quickPayload.jobs && quickPayload.jobs.length ? quickPayload.jobs.length : 0;
      if (quickJobCount >= 2 && fastMode !== "visible") {
        try {
          const listPayload = await collectListPayload(options || {});
          const listJobCount = listPayload.jobs && listPayload.jobs.length ? listPayload.jobs.length : 0;
          if (wantsDetails) return listPayload;
          if (listJobCount >= quickJobCount) return listPayload;
        } catch (error) {
          if (wantsDetails) {
            return {
              type: "list_detail_capture_failed",
              title: document.title || "capture_failed",
              url: location.href,
              savedAt: new Date().toISOString(),
              jobCount: 0,
              cardCount: quickJobCount,
              detailCount: 0,
              detailRequired: true,
              jobs: quickPayload.jobs || [],
              error: String(error && error.message ? error.message : error || "detail_capture_failed"),
              text: cleanText(quickPayload.text || "").slice(0, 120000),
            };
          }
          if (quickJobCount) return quickPayload;
        }
      }
      if (quickJobCount && wantsDetails && fastMode !== "visible") {
        try {
          return await collectListPayload(options || {});
        } catch (error) {
          return {
            type: "list_detail_capture_failed",
            title: document.title || "capture_failed",
            url: location.href,
            savedAt: new Date().toISOString(),
            jobCount: 0,
            cardCount: quickJobCount,
            detailCount: 0,
            detailRequired: true,
            jobs: quickPayload.jobs || [],
            error: String(error && error.message ? error.message : error || "detail_capture_failed"),
            text: cleanText(quickPayload.text || "").slice(0, 120000),
          };
        }
      }
      if (quickJobCount && fastMode === "visible") return quickPayload;
      if (!quickJobCount) {
        try {
          const detailPayload = await collectDetailPayload(options || {});
          if (cleanText(detailPayload && detailPayload.text).length >= 30) return detailPayload;
        } catch (_error) {
        }
      }
      if (quickJobCount || cleanText(quickPayload.text).length >= 30) {
        return quickPayload;
      }
    }
    await waitForDomSettled(1800, 350);
    const firstPass = collectJobsFromDocument(document, location.href, 1, true);
    if (firstPass.length >= 2) {
      const payload = await collectListPayload(options || {});
      if (payload.jobs && payload.jobs.length) return payload;
    }
    return collectDetailPayload();
  }

  globalThis.CareerPilotExtractor = {
    cleanText,
    compact,
    collectAutoPayload,
    collectDetailPayload,
    collectFastVisiblePayload,
    collectListPayload,
    normalizeExportedJob,
  };
})(globalThis);
