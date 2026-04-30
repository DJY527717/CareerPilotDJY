const SETTINGS_KEYS = ["serverUrl", "uploadToken", "saveLocalCopy", "maxPages", "scrollSteps", "detailLimit"];

function sanitizeFileName(input) {
  return (input || "jd")
    .replace(/[\\/:*?"<>|]+/g, "_")
    .replace(/\s+/g, "_")
    .slice(0, 80);
}

async function loadSettings() {
  const stored = await chrome.storage.local.get(SETTINGS_KEYS);
  document.getElementById("serverUrl").value = stored.serverUrl || "";
  document.getElementById("uploadToken").value = stored.uploadToken || "";
  document.getElementById("saveLocalCopy").checked = stored.saveLocalCopy !== false;
  if (stored.maxPages) document.getElementById("maxPages").value = stored.maxPages;
  if (stored.scrollSteps !== undefined) document.getElementById("scrollSteps").value = stored.scrollSteps;
  if (stored.detailLimit !== undefined) document.getElementById("detailLimit").value = stored.detailLimit;
}

async function saveSettings() {
  await chrome.storage.local.set({
    serverUrl: document.getElementById("serverUrl").value.trim(),
    uploadToken: document.getElementById("uploadToken").value.trim(),
    saveLocalCopy: document.getElementById("saveLocalCopy").checked,
    maxPages: document.getElementById("maxPages").value,
    scrollSteps: document.getElementById("scrollSteps").value,
    detailLimit: document.getElementById("detailLimit").value
  });
}

function configuredServerUrl() {
  return (document.getElementById("serverUrl").value || "").trim();
}

function configuredUploadToken() {
  return (document.getElementById("uploadToken").value || "").trim();
}

async function uploadPayload(serverUrl, uploadToken, payload, prefix) {
  const response = await fetch(serverUrl, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-CareerPilot-Upload-Token": uploadToken
    },
    body: JSON.stringify({ payload, prefix })
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || data.ok === false) {
    throw new Error(data.error || `Upload failed with HTTP ${response.status}`);
  }
  return data;
}

function activeTab() {
  return chrome.tabs.query({ active: true, currentWindow: true }).then(([tab]) => {
    if (!tab || !tab.id) throw new Error("No active tab found.");
    return tab;
  });
}

function extractPage() {
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

  const clone = document.body.cloneNode(true);
  ["script", "style", "noscript", "svg", "canvas", "iframe", "nav", "footer", "[role='navigation']"]
    .forEach((selector) => clone.querySelectorAll(selector).forEach((node) => node.remove()));

  const candidates = Array.from(
    clone.querySelectorAll("main, article, section, [class*='job'], [class*='Job'], [class*='detail'], [class*='position'], [class*='description']")
  )
    .map((node) => cleanText(node.innerText || ""))
    .filter((text) => text.length > 120)
    .sort((a, b) => b.length - a.length)
    .slice(0, 8);

  const fallback = cleanText(clone.innerText || document.body.innerText || "");
  return {
    type: "detail",
    title: document.title || "",
    url: location.href,
    savedAt: new Date().toISOString(),
    text: cleanText(candidates.length ? candidates.join("\n\n") : fallback)
  };
}

function pageSnapshot(pageIndex, scrollSteps) {
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

  function isVisible(node) {
    if (!node || !(node instanceof Element)) return false;
    const style = window.getComputedStyle(node);
    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 24 && rect.height > 18;
  }

  const salaryPatterns = [
    /\d+(?:\.\d+)?\s*[-~\u2014\u81f3\u5230]\s*\d+(?:\.\d+)?\s*[kK](?:\s*[\u00b7*xX]\s*\d+\s*\u85aa)?/,
    /\d+(?:\.\d+)?\s*[-~\u2014\u81f3\u5230]\s*\d+(?:\.\d+)?\s*\u5143\s*\/?\s*\u5929/,
    /\d+(?:\.\d+)?\s*[kK]\s*(?:\u4ee5\u4e0a|\+)?/,
    /\d+(?:\.\d+)?\s*[-~\u2014\u81f3\u5230]\s*\d+(?:\.\d+)?\s*[\u4e07wW]/
  ];

  function findSalary(text) {
    for (const pattern of salaryPatterns) {
      const match = text.match(pattern);
      if (match) return match[0];
    }
    return "";
  }

  function isLikelyJobUrl(href) {
    if (!href || /^(javascript:|mailto:|tel:)/i.test(href)) return false;
    try {
      const url = new URL(href, location.href);
      const value = `${url.hostname}${url.pathname}${url.search}`.toLowerCase();
      if (/\/(search|list|index|home)(\/|\?|$)/i.test(url.pathname) && !/(job|position|intern)/i.test(value)) return false;
      return /(job_detail|\/job\/|jobs\.|\/intern\/|\/interns\/inn_|\/position\/|\/positions\/|jobid|job_id|positionid|postid|recruitid|campus\/position|xiaoyuan)/i.test(value);
    } catch (_error) {
      return false;
    }
  }

  function scoreJobCard(text) {
    const keywords = [
      "\u5c97\u4f4d", "\u804c\u4f4d", "\u85aa", "\u5143/\u5929", "\u4e0a\u6d77",
      "\u5b9e\u4e60", "\u5168\u804c", "\u7ecf\u9a8c", "\u672c\u79d1", "\u7855\u58eb",
      "\u516c\u53f8", "\u987e\u95ee", "\u7ecf\u7406", "\u52a9\u7406", "\u4e0a\u5e02",
      "LCA", "ESG", "CBAM", "EPD", "ISO", "Python", "SimaPro", "GaBi", "openLCA"
    ];
    let score = 0;
    keywords.forEach((keyword) => { if (text.includes(keyword)) score += 1; });
    if (findSalary(text)) score += 6;
    if (text.length >= 35 && text.length <= 2200) score += 2;
    return score;
  }

  function bestAncestor(node) {
    let current = node;
    let best = node;
    for (let i = 0; i < 8 && current && current !== document.body; i += 1) {
      const text = cleanText(current.innerText || "");
      const bestText = cleanText(best.innerText || "");
      if (text.length >= 40 && text.length <= 2200 && scoreJobCard(text) >= scoreJobCard(bestText)) {
        best = current;
      }
      current = current.parentElement;
    }
    return best;
  }

  function fieldFromSelector(node, selectors) {
    for (const selector of selectors) {
      const target = node.querySelector(selector);
      if (target && cleanText(target.innerText || target.textContent || "")) {
        return cleanText(target.innerText || target.textContent || "");
      }
    }
    return "";
  }

  function extractFields(node, text, linkText) {
    const lines = cleanText(text).split(/\n+/).map((line) => cleanText(line)).filter(Boolean);
    const salary = findSalary(text);

    function isSalaryLine(line) {
      return Boolean(findSalary(line));
    }

    function isMetaLine(line) {
      return /^(?:经验不限|在校\/应届|应届|博士|硕士|研究生|本科|大专|学历不限|\d+\s*[-~至到]\s*\d+\s*年|\d+\s*年以上|\d+\s*天\/周|\d+\s*个月)$/.test(line);
    }

    function isLocationLine(line) {
      return /^(?:上海|北京|深圳|广州|苏州|杭州|南京|宁波|无锡|常州|嘉兴|成都|武汉|重庆|厦门|郑州|合肥|天津|芜湖|东莞|泉州|鄂尔多斯|昆明|宁德)(?:$|·)/.test(line);
    }

    function isBadCompanyLine(line) {
      return /^(?:收藏|立即沟通|举报|微信扫码分享|职位描述|任职要求|去App|前往App|查看更多信息|求职工具|热门职位|热门城市|附近城市|满意|不满意|一般|提交|在线)$/.test(line)
        || /(?:岗位|职位|职责|薪资|经验|学历|招聘|首页|搜索|扫码|沟通)/.test(line);
    }

    function structuredFieldsFromLines() {
      for (let i = 0; i < lines.length - 3; i += 1) {
        const maybeTitle = lines[i];
        if (maybeTitle.length < 2 || maybeTitle.length > 100 || isSalaryLine(maybeTitle) || isMetaLine(maybeTitle) || isLocationLine(maybeTitle)) continue;
        let cursor = i + 1;
        let parsedSalary = "";
        if (cursor < lines.length && isSalaryLine(lines[cursor])) {
          parsedSalary = findSalary(lines[cursor]);
          cursor += 1;
        } else if (cursor + 1 < lines.length && isSalaryLine(lines[cursor + 1])) {
          parsedSalary = findSalary(lines[cursor + 1]);
          cursor += 2;
        } else {
          continue;
        }
        let locationBeforeMeta = "";
        if (cursor < lines.length && isLocationLine(lines[cursor])) {
          locationBeforeMeta = lines[cursor];
          cursor += 1;
        }
        const meta = [];
        while (cursor < lines.length && meta.length < 6 && isMetaLine(lines[cursor])) {
          meta.push(lines[cursor]);
          cursor += 1;
        }
        if (cursor >= lines.length || isBadCompanyLine(lines[cursor]) || isLocationLine(lines[cursor]) || isSalaryLine(lines[cursor]) || isMetaLine(lines[cursor])) continue;
        const parsedCompany = lines[cursor];
        const parsedLocation = (cursor + 1 < lines.length && isLocationLine(lines[cursor + 1])) ? lines[cursor + 1] : locationBeforeMeta;
        const parsedEducation = meta.find((line) => /博士|硕士|研究生|本科|大专|学历不限/.test(line)) || "";
        const parsedExperience = meta.filter((line) => line !== parsedEducation).join(" / ");
        return {
          title: maybeTitle,
          company: parsedCompany,
          salary: parsedSalary,
          location: parsedLocation,
          education: parsedEducation,
          experience: parsedExperience
        };
      }
      return null;
    }

    const structured = structuredFieldsFromLines();
    if (structured) return structured;

    const title = cleanText(
      fieldFromSelector(node, [
        "[class*='job-name']", "[class*='position-name']", "[class*='job-title']",
        "h1", "h2", "h3", "[class*='title']"
      ]) ||
      linkText ||
      lines.find((line) => line.length <= 80 && !findSalary(line)) ||
      ""
    ).slice(0, 120);

    const companyByClass = fieldFromSelector(node, [
      "[class*='company']", "[class*='Company']", "[class*='brand']", "[class*='corp']"
    ]);
    const companyByLine = lines.find((line) =>
      line.length <= 70 &&
      /(\u516c\u53f8|\u96c6\u56e2|\u79d1\u6280|\u54a8\u8be2|\u68c0\u6d4b|\u8ba4\u8bc1|\u80a1\u4efd|\u6709\u9650)/.test(line) &&
      line !== title
    );
    const company = cleanText(companyByClass || companyByLine || "").slice(0, 90);

    const cityMatch = text.match(/(\u4e0a\u6d77|\u5317\u4eac|\u6df1\u5733|\u5e7f\u5dde|\u82cf\u5dde|\u676d\u5dde|\u5357\u4eac|\u5b81\u6ce2|\u65e0\u9521|\u5e38\u5dde|\u5609\u5174|\u6210\u90fd|\u6b66\u6c49|\u91cd\u5e86)/);
    const educationMatch = text.match(/(\u535a\u58eb|\u7855\u58eb|\u7814\u7a76\u751f|\u672c\u79d1|\u5927\u4e13|\u5b66\u5386\u4e0d\u9650)/);
    const experienceMatch = text.match(/((?:\d+\s*[-~\u81f3\u5230]\s*)?\d+\s*\u5e74(?:\u4ee5\u4e0a)?(?:\u5de5\u4f5c)?\u7ecf\u9a8c|\u7ecf\u9a8c\u4e0d\u9650|\u5e94\u5c4a\u751f|\u6821\u62db|\u79cb\u62db|\u5b9e\u4e60)/);

    return {
      title,
      company,
      salary,
      location: cityMatch ? cityMatch[0] : "",
      education: educationMatch ? educationMatch[0] : "",
      experience: experienceMatch ? experienceMatch[0] : ""
    };
  }

  function collectOnce() {
    const bucket = [];
    const selector = [
      "[class*='job']", "[class*='Job']", "[class*='position']", "[class*='Position']",
      "[class*='card']", "[class*='Card']", "[class*='item']", "[class*='Item']",
      "[class*='list']", "[data-jobid]", "[data-jid]", "li", "article"
    ].join(",");

    function addCandidate(node, forcedUrl = "") {
      if (!node || !isVisible(node)) return;
      const text = cleanText(node.innerText || "");
      if (text.length < 8 || text.length > 5000) return;
      const score = scoreJobCard(text);
      if (score < 5 && !forcedUrl) return;
      bucket.push({ node, text, score: forcedUrl ? score + 8 : score, forcedUrl });
    }

    document.querySelectorAll(selector).forEach(addCandidate);
    document.querySelectorAll("a[href]").forEach((anchor) => {
      const href = anchor.getAttribute("href") || "";
      if (!isLikelyJobUrl(href)) return;
      addCandidate(bestAncestor(anchor), new URL(href, location.href).href);
    });
    Array.from(document.querySelectorAll("body *")).forEach((node) => {
      if (!isVisible(node)) return;
      const text = cleanText(node.innerText || node.textContent || "");
      if (findSalary(text)) addCandidate(bestAncestor(node));
    });

    const localSeen = new Set();
    return bucket
      .sort((a, b) => b.score - a.score)
      .map(({ node, text, score, forcedUrl }) => {
        const linkNode = node.querySelector("a[href]") || node.closest("a[href]");
        const href = forcedUrl || (linkNode ? new URL(linkNode.getAttribute("href"), location.href).href : "");
        const key = (href || "") + "|" + compact(text).slice(0, 260);
        if (localSeen.has(key)) return null;
        localSeen.add(key);
        const linkText = cleanText((linkNode && linkNode.innerText) || "");
        const fields = extractFields(node, text, linkText);
        return { ...fields, url: href, text, score, pageIndex };
      })
      .filter(Boolean);
  }

  function findNextButton() {
    const nodes = Array.from(document.querySelectorAll("button, a, li, span, div"))
      .filter(isVisible);
    const candidates = nodes
      .map((node) => {
        const text = cleanText(node.innerText || node.textContent || "");
        const aria = (node.getAttribute("aria-label") || "").toLowerCase();
        const rel = (node.getAttribute("rel") || "").toLowerCase();
        const cls = (node.className || "").toString().toLowerCase();
        const disabled = node.disabled || node.getAttribute("aria-disabled") === "true" || cls.includes("disabled");
        if (disabled || text.includes("\u4e0a\u4e00\u9875") || aria.includes("prev")) return null;
        let score = 0;
        if (text === "\u4e0b\u4e00\u9875" || text === "\u4e0b\u9875") score += 10;
        if (text === ">" || text === "\u203a" || text === "\u00bb") score += 8;
        if (text.toLowerCase() === "next") score += 8;
        if (aria.includes("next") || aria.includes("\u4e0b\u4e00\u9875")) score += 8;
        if (rel === "next") score += 8;
        if (cls.includes("next")) score += 6;
        const href = node.tagName.toLowerCase() === "a" && node.getAttribute("href")
          ? new URL(node.getAttribute("href"), location.href).href
          : "";
        return score > 0 ? { node, score, text, href } : null;
      })
      .filter(Boolean)
      .sort((a, b) => b.score - a.score);
    return candidates[0] || null;
  }

  function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  return new Promise(async (resolve) => {
    const jobs = [];
    const seen = new Set();
    const steps = Math.max(0, Math.min(Number(scrollSteps) || 0, 8));
    for (let step = 0; step <= steps; step += 1) {
      collectOnce().forEach((job) => {
        const key = (job.url || "") + "|" + compact(job.text).slice(0, 260);
        if (!seen.has(key)) {
          seen.add(key);
          jobs.push(job);
        }
      });
      if (step < steps) {
        window.scrollBy(0, Math.max(500, window.innerHeight * 0.75));
        await wait(500);
      }
    }
    const next = findNextButton();
    resolve({
      type: "list",
      title: document.title || "",
      url: location.href,
      savedAt: new Date().toISOString(),
      jobCount: jobs.length,
      jobs: jobs.slice(0, 250),
      next: next ? { found: true, text: next.text, href: next.href } : { found: false }
    });
  });
}

function clickNextButtonInPage() {
  function cleanText(text) {
    return (text || "").replace(/\s+/g, " ").trim();
  }
  function isVisible(node) {
    if (!node || !(node instanceof Element)) return false;
    const style = window.getComputedStyle(node);
    if (style.display === "none" || style.visibility === "hidden" || style.opacity === "0") return false;
    const rect = node.getBoundingClientRect();
    return rect.width > 24 && rect.height > 18;
  }
  const candidates = Array.from(document.querySelectorAll("button, a, li, span, div"))
    .filter(isVisible)
    .map((node) => {
      const text = cleanText(node.innerText || node.textContent || "");
      const aria = (node.getAttribute("aria-label") || "").toLowerCase();
      const rel = (node.getAttribute("rel") || "").toLowerCase();
      const cls = (node.className || "").toString().toLowerCase();
      const disabled = node.disabled || node.getAttribute("aria-disabled") === "true" || cls.includes("disabled");
      if (disabled || text.includes("\u4e0a\u4e00\u9875") || aria.includes("prev")) return null;
      let score = 0;
      if (text === "\u4e0b\u4e00\u9875" || text === "\u4e0b\u9875") score += 10;
      if (text === ">" || text === "\u203a" || text === "\u00bb") score += 8;
      if (text.toLowerCase() === "next") score += 8;
      if (aria.includes("next") || aria.includes("\u4e0b\u4e00\u9875")) score += 8;
      if (rel === "next") score += 8;
      if (cls.includes("next")) score += 6;
      return score > 0 ? { node, score } : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);
  if (!candidates.length) return false;
  const node = candidates[0].node;
  node.scrollIntoView({ block: "center", inline: "center" });
  node.dispatchEvent(new MouseEvent("mouseover", { bubbles: true }));
  node.dispatchEvent(new MouseEvent("mousedown", { bubbles: true }));
  node.dispatchEvent(new MouseEvent("mouseup", { bubbles: true }));
  node.click();
  return true;
}

async function runExtractor(func, args = []) {
  const tab = await activeTab();
  const [result] = await chrome.scripting.executeScript({ target: { tabId: tab.id }, func, args });
  return result.result;
}

async function runExtractorInTab(tabId, func, args = []) {
  const [result] = await chrome.scripting.executeScript({ target: { tabId }, func, args });
  return result.result;
}

async function runPageSnapshot(pageIndex, scrollSteps) {
  return runExtractor(pageSnapshot, [pageIndex, scrollSteps]);
}

async function clickNextPage() {
  return runExtractor(clickNextButtonInPage);
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForTabLoad(tabId, fallbackMs = 2200) {
  await new Promise((resolve) => {
    let done = false;
    const timer = setTimeout(() => {
      if (!done) {
        done = true;
        chrome.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    }, fallbackMs);
    function listener(updatedTabId, info) {
      if (updatedTabId === tabId && info.status === "complete" && !done) {
        done = true;
        clearTimeout(timer);
        chrome.tabs.onUpdated.removeListener(listener);
        resolve();
      }
    }
    chrome.tabs.onUpdated.addListener(listener);
  });
  await wait(900);
}

function usableDetailUrl(url, sourceUrl = "") {
  if (!url || /^(javascript:|mailto:|tel:)/i.test(url)) return false;
  try {
    const parsed = new URL(url);
    if (!/^https?:$/.test(parsed.protocol)) return false;
    if (sourceUrl && parsed.href.split("#")[0] === sourceUrl.split("#")[0]) return false;
    const value = `${parsed.hostname}${parsed.pathname}${parsed.search}`.toLowerCase();
    if (/\/(search|list|index|home)(\/|\?|$)/i.test(parsed.pathname) && !/(job|position|intern)/i.test(value)) return false;
    return /(job_detail|\/job\/|jobs\.|\/intern\/|jobid|job_id|positionid|postid|recruitid|campus\/position|xiaoyuan)/i.test(value);
  } catch (_error) {
    return false;
  }
}

async function readDetailPageInHiddenTab(url, index, total, status) {
  let tab = null;
  try {
    status.textContent = `Reading detail ${index}/${total}...`;
    tab = await chrome.tabs.create({ url, active: false });
    await waitForTabLoad(tab.id, 5000);
    const detail = await runExtractorInTab(tab.id, extractPage);
    if (detail && detail.text && detail.text.length >= 40) return detail;
    return null;
  } catch (_error) {
    return null;
  } finally {
    if (tab && tab.id) {
      try { await chrome.tabs.remove(tab.id); } catch (_error) {}
    }
  }
}

async function downloadPayload(payload, prefix) {
  const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const now = new Date();
  const stamp = now.toISOString().replace(/[:.]/g, "-");
  const localDate = [
    now.getFullYear(),
    String(now.getMonth() + 1).padStart(2, "0"),
    String(now.getDate()).padStart(2, "0")
  ].join("-");
  const filename = `CareerPilot_JD/${localDate}/${stamp}_${prefix}_${sanitizeFileName(payload.title)}.json`;
  await chrome.downloads.download({ url, filename, saveAs: false, conflictAction: "uniquify" });
}

async function persistPayload(payload, prefix, status) {
  await saveSettings();
  const serverUrl = configuredServerUrl();
  const uploadToken = configuredUploadToken();
  const saveLocalCopy = document.getElementById("saveLocalCopy").checked;
  let uploadResult = null;
  let localSaved = false;

  if (serverUrl && uploadToken) {
    status.textContent = "Uploading to CareerPilot cloud...";
    uploadResult = await uploadPayload(serverUrl, uploadToken, payload, prefix);
  }

  if (!uploadResult || saveLocalCopy) {
    await downloadPayload(payload, prefix);
    localSaved = true;
  }

  return { uploadResult, localSaved };
}

function numericInput(id, fallback, min, max) {
  const raw = Number(document.getElementById(id).value);
  if (!Number.isFinite(raw)) return fallback;
  return Math.max(min, Math.min(max, Math.round(raw)));
}

async function saveDetailPage() {
  const button = document.getElementById("savePage");
  const status = document.getElementById("status");
  button.disabled = true;
  status.textContent = "Reading detail page...";
  try {
    const payload = await runExtractor(extractPage);
    if (!payload || !payload.text || payload.text.length < 30) throw new Error("Page text is too short.");
    const result = await persistPayload(payload, "detail", status);
    if (result.uploadResult && result.localSaved) {
      status.textContent = `Uploaded and saved locally: ${payload.text.length} chars`;
    } else if (result.uploadResult) {
      status.textContent = `Uploaded detail page: ${payload.text.length} chars`;
    } else {
      status.textContent = `Saved detail page locally: ${payload.text.length} chars`;
    }
  } catch (error) {
    status.textContent = `Failed: ${error.message}`;
  } finally {
    button.disabled = false;
  }
}

async function collectNextPagesWithDetailsAndSave() {
  const button = document.getElementById("nextPagesDetailSave");
  const status = document.getElementById("status");
  const maxPages = numericInput("maxPages", 4, 1, 20);
  const scrollSteps = numericInput("scrollSteps", 2, 0, 8);
  const detailLimit = numericInput("detailLimit", 20, 0, 80);
  const tab = await activeTab();
  const allJobs = [];
  const seen = new Set();
  let lastTitle = "";
  let lastUrl = "";

  button.disabled = true;
  status.textContent = `Collecting up to ${maxPages} pages...`;
  try {
    for (let pageIndex = 1; pageIndex <= maxPages; pageIndex += 1) {
      const payload = await runPageSnapshot(pageIndex, scrollSteps);
      lastTitle = payload.title || lastTitle;
      lastUrl = payload.url || lastUrl || tab.url || "";
      (payload.jobs || []).forEach((job) => {
        const key = (job.url || "") + "|" + (job.text || "").replace(/\s+/g, "").slice(0, 260);
        if (!seen.has(key)) {
          seen.add(key);
          allJobs.push(job);
        }
      });
      status.textContent = `Page ${pageIndex}: found ${allJobs.length} jobs`;
      if (pageIndex >= maxPages || !payload.next || !payload.next.found) break;
      const clicked = await clickNextPage();
      if (!clicked) break;
      await waitForTabLoad(tab.id, 2600);
    }

    const detailJobs = allJobs
      .map((job, index) => ({ job, index }))
      .filter(({ job }) => usableDetailUrl(job.url, lastUrl))
      .slice(0, detailLimit);
    for (let i = 0; i < detailJobs.length; i += 1) {
      const { job, index } = detailJobs[i];
      const detail = await readDetailPageInHiddenTab(job.url, i + 1, detailJobs.length, status);
      if (!detail) continue;
      allJobs[index] = {
        ...job,
        detailTitle: detail.title || "",
        detailUrl: detail.url || job.url,
        detailText: detail.text || "",
        text: [
          job.text || "",
          "",
          "详情页信息：",
          detail.title ? `标题：${detail.title}` : "",
          detail.url ? `链接：${detail.url}` : "",
          detail.text || ""
        ].filter(Boolean).join("\n")
      };
    }

    const finalPayload = {
      type: "list_paginated_with_details",
      title: lastTitle || "paginated_jobs_with_details",
      url: lastUrl || tab.url || "",
      savedAt: new Date().toISOString(),
      jobCount: allJobs.length,
      detailCount: detailJobs.length,
      jobs: allJobs.slice(0, 800)
    };
    if (!finalPayload.jobs.length) throw new Error("No jobs detected across pages.");
    const result = await persistPayload(finalPayload, "list_next_pages_details", status);
    if (result.uploadResult && result.localSaved) {
      status.textContent = `Uploaded + saved locally: ${finalPayload.jobs.length} jobs`;
    } else if (result.uploadResult) {
      status.textContent = `Uploaded ${finalPayload.jobs.length} jobs to cloud`;
    } else {
      status.textContent = `Saved ${finalPayload.jobs.length} jobs locally`;
    }
  } catch (error) {
    status.textContent = `Failed: ${error.message}`;
  } finally {
    button.disabled = false;
  }
}

document.getElementById("savePage").addEventListener("click", saveDetailPage);
document.getElementById("nextPagesDetailSave").addEventListener("click", collectNextPagesWithDetailsAndSave);
["serverUrl", "uploadToken", "saveLocalCopy", "maxPages", "scrollSteps", "detailLimit"].forEach((id) => {
  document.getElementById(id).addEventListener("change", saveSettings);
});
loadSettings().catch((error) => {
  document.getElementById("status").textContent = `Failed to load settings: ${error.message}`;
});
