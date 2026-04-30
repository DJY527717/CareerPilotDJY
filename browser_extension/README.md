# CareerPilot JD Saver

用于 Edge / Chrome 的本地扩展，负责把招聘网站里的岗位列表页和详情页提取为 CareerPilot 可读取的 JSON。

## 安装

1. 打开 `edge://extensions/` 或 `chrome://extensions/`
2. 开启“开发者模式”
3. 点击“加载已解压的扩展程序”
4. 选择 `browser_extension` 文件夹

## 使用

- `Collect list + detail pages`
  - 用在招聘搜索结果页
  - 会滚动当前页、尝试翻页，并抓取岗位详情页文本
- `Save current detail page`
  - 用在单个岗位详情页

## 本地保存

如果没有配置云端上传，插件会把 JSON 保存到：

```text
Downloads/CareerPilot_JD/YYYY-MM-DD/
```

## 云端上传

如果在插件里填了下面两项：

- `Upload URL`
- `Upload token`

插件会直接把 JSON 上传到 CareerPilot 云端；如果勾选了 `Also save a local copy to Downloads`，还会同时保留本地副本。

上传地址和上传令牌可以在 CareerPilot 登录后的侧边栏“浏览器插件云上传”里复制。
