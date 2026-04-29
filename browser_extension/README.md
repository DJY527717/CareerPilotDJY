# CareerPilot JD Saver

本地 Edge / Chrome 扩展，用于把已登录招聘网站里的岗位列表和详情页保存为 CareerPilot 可读取的 JSON。

## 安装

1. 打开 `edge://extensions/` 或 `chrome://extensions/`
2. 开启“开发人员模式”
3. 点击“加载解压缩的扩展”
4. 选择 `browser_extension` 文件夹

如果已经安装过旧版，修改后需要在扩展管理页点击“重新加载”。

## 使用

插件只保留两个入口：

- `Collect list + detail pages`
  - 用在招聘搜索结果页。
  - 会滚动当前页、尝试翻页，并收集岗位详情链接。
  - `Max pages` 控制最多翻几页。
  - `Scroll/page` 控制每页滚动次数。
  - `Detail pages` 控制最多打开多少个详情页补全文本；设为 `0` 时只保存列表。

- `Save current detail page`
  - 用在单个岗位详情页。
  - 适合某个岗位识别不准时，单独保存完整详情页。

导出文件会保存到：

```text
Downloads/CareerPilot_JD/YYYY-MM-DD/
```

回到 CareerPilot 的“批量JD筛选”后，扫描插件导出文件，再在候选表里勾选要导入分析的岗位。
