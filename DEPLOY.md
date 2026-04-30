# CareerPilot 公网部署说明

本地启动脚本只能让本机或局域网访问。要让任何电脑、任何网络都能打开，需要把项目部署到公网云平台，并配置云数据库。

## 已支持的线上能力

- 用户注册、登录、退出。
- 每个用户独立保存求职目标、简历、批量岗位库、投递队列和设置。
- 新用户首次打开时没有内置简历、目标意向或示例文本，需要自行填写。
- 默认使用本地 SQLite；配置 `DATABASE_URL` 后使用 Postgres 云数据库。

## 推荐方案

比较省事的组合：

- 代码托管：GitHub
- 网站部署：Render / Railway / Fly.io / 自有云服务器 Docker
- 云数据库：Supabase Postgres / Neon Postgres / Render Postgres / Railway Postgres

Streamlit Community Cloud 也可以跑页面，但正式多人使用更建议选支持长期服务和环境变量管理的平台。

## 部署步骤

1. 把 `C:\Code\CareerPilot` 上传到 GitHub 仓库。
2. 不要上传本地数据文件和隐私文件，例如 `careerpilot.db`、`carboncareer.db`、`.venv_cp313/`、简历文件、导出的 Excel/PDF。
3. 在云平台新建一个 Postgres 数据库。
4. 复制数据库连接串，通常长这样：

```text
postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

5. 在云平台的环境变量里添加：

```text
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
```

6. 用 Docker 部署本项目。项目已经包含 `Dockerfile`，平台会执行：

```bash
docker build -t careerpilot .
docker run -p 8503:8503 -e DATABASE_URL="$DATABASE_URL" careerpilot
```

如果平台自动识别 Dockerfile，只需要填好仓库、分支和环境变量即可。

## 自动更新

以后你在本机修改代码后，提交并推送到 GitHub：

```bash
git add .
git commit -m "Update CareerPilot"
git push
```

Render、Railway、Fly.io 等平台可以设置为监听 GitHub 分支；推送后会自动重新部署，别人打开的公网网址也会使用新版本。

## 本地与线上数据的区别

本地没有配置 `DATABASE_URL` 时，数据保存在：

```text
careerpilot.db
```

线上配置 `DATABASE_URL` 后，用户、简历、目标、岗位和投递记录都会保存到云端 Postgres。这样重新部署或换服务器时，用户数据不会跟着容器文件丢失。

## 浏览器插件说明

公网网页不能直接读取你本机浏览器插件导出的 `Downloads/CareerPilot_JD` 文件。只有当插件导出目录本身就在服务器上时，线上应用才可以直接扫描。

如果你已经把导出目录同步到了服务器，例如：

```text
/root/Downloads/CareerPilot_JD
```

可以在服务端环境变量里显式指定：

```text
JD_EXPORT_DIR=/root/Downloads/CareerPilot_JD
```

如果要配置多个扫描目录，可使用：

```text
JD_EXPORT_DIRS=/root/Downloads/CareerPilot_JD;/app/CareerPilot_JD
```

如果插件导出仍然保存在你自己的电脑上，云服务器依然读不到；这种情况仍然需要通过网页上传，或后续把插件改成直接上传到云端接口。

现在项目已经支持“插件直接上传到云端接口”。

部署时建议同时配置：

```text
APP_PUBLIC_URL=http://你的域名或IP:8503
UPLOAD_API_PORT=8765
UPLOAD_API_PUBLIC_URL=http://你的域名或IP:8765/api/plugin-upload
```

如果你使用 Docker 直接跑在阿里云服务器上，还需要：

1. 放通 `8503` 和 `8765` 端口
2. 用 `python serve.py` 或 Docker 默认启动方式一起拉起 Streamlit 和上传 API
3. 登录 CareerPilot 后，在侧边栏“浏览器插件云上传”里复制上传地址和上传令牌
4. 粘贴到浏览器插件里，之后插件抓到的 JD 会直接保存到服务器上的当前用户目录

服务端保存位置类似：

```text
/app/uploaded_jd/user_用户ID/YYYY-MM-DD/
```
