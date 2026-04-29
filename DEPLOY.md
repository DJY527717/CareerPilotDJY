# CareerPilot 公网部署说明

本项目本地启动脚本只能让本机或局域网访问。要让任何电脑、任何网络都能打开，需要把项目部署到公网云平台。

## 推荐方案：Streamlit Community Cloud

适合：最快上线、低成本演示、自动跟随代码更新。

流程：

1. 把 `C:\Code\CareerPilot` 上传到 GitHub 仓库。
2. 注意不要上传本地数据文件：`careerpilot.db`、`carboncareer.db`、`.venv_cp313/`、简历文件、导出的 Excel/PDF。项目的 `.gitignore` 已经默认排除这些文件。
3. 打开 Streamlit Community Cloud，新建 App。
4. 选择 GitHub 仓库、分支和入口文件：

```text
app.py
```

5. 部署完成后会得到一个公网地址，例如：

```text
https://your-app-name.streamlit.app
```

以后你在本机修改代码后，只要提交并推送到 GitHub：

```bash
git add .
git commit -m "Update CareerPilot"
git push
```

线上网页会自动重新部署更新。

## 正式多人使用必须注意

当前版本的数据存储是本地 SQLite：

```text
careerpilot.db
```

如果直接部署成公网网页，所有访问者会共用同一份服务器数据，而且云平台重启或重新部署后，本地文件型数据可能丢失。涉及简历、投递记录、求职目标时，这不适合正式多人使用。

新部署环境不会内置任何简历、目标意向或示例用户文本；用户第一次打开时需要自行填写当前简历和求职目标。

正式多人版建议增加：

- 用户登录
- 每个用户独立数据
- 云数据库，例如 Supabase Postgres、Neon Postgres、Render Postgres
- 文件存储，例如 S3、Cloudflare R2、Supabase Storage

## Docker / Render / 云服务器方案

项目已经提供 `Dockerfile`。适合需要更稳定服务、绑定域名或后续接入数据库的部署。

通用启动命令：

```bash
docker build -t careerpilot .
docker run -p 8503:8503 careerpilot
```

部署到 Render、Railway、Fly.io、腾讯云、阿里云、华为云等平台时，使用 Docker 部署，并让平台暴露容器端口。`Dockerfile` 会读取平台提供的 `PORT` 环境变量。

## 浏览器插件说明

公网网页本身不能直接读取你本机浏览器插件导出的 `Downloads/CareerPilot_JD` 文件。插件批量导出的文件如果要在云端分析，需要通过网页上传，或后续把插件改成直接上传到云端接口。
