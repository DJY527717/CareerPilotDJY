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

公网网页不能直接读取你本机浏览器插件导出的 `Downloads/CareerPilot_JD` 文件。插件批量导出的岗位如果要在线上分析，需要通过网页上传，或后续把插件改成直接上传到云端接口。
