# CareerPilot 阿里云长期部署方案

这套方案的目标很简单：

- 你平时只管在本地开发
- 代码照常 `git push origin main`
- GitHub Actions 自动把最新代码同步到阿里云
- 服务器自动安装依赖并重启服务

这样以后不需要再手动 SSH 到服务器执行 `git pull`，也不会再被服务器到 GitHub 的网络问题卡住。

## 推荐架构

- 代码仓库：GitHub
- 自动部署：GitHub Actions
- 线上服务器：阿里云 ECS
- 进程守护：systemd
- 应用启动：`python serve.py`

`serve.py` 会同时启动：

- Streamlit 主应用
- 插件上传 API

## 一次性配置

### 1. 服务器准备目录

在阿里云上准备项目目录：

```bash
mkdir -p /www/CareerPilotDJY
```

### 2. 服务器安装基础环境

确保服务器具备：

- `python3`
- `python3-venv`
- `rsync`
- `systemd`

例如在常见 Linux 发行版上安装：

```bash
dnf install -y python3 python3-pip python3-virtualenv rsync
```

### 3. 首次把仓库放到服务器

首次需要先把仓库目录放到服务器一次。可以先手动上传当前代码，或者直接让 GitHub Actions 首次同步。

如果目录里还没有代码，可以先创建一个空目录即可，后续由 Actions 覆盖同步。

### 4. 创建服务器环境变量文件

在服务器创建：

```bash
vi /www/CareerPilotDJY/.env
```

示例内容：

```text
APP_HOST=0.0.0.0
APP_PORT=8000
UPLOAD_API_HOST=0.0.0.0
UPLOAD_API_PORT=8765
APP_PUBLIC_URL=http://你的域名或IP:8000
UPLOAD_API_PUBLIC_URL=http://你的域名或IP:8765/api/plugin-upload
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
JD_EXPORT_DIR=/root/Downloads/CareerPilot_JD
APP_BUILD_LABEL=production
```

如果暂时不用 Postgres，可以先不填 `DATABASE_URL`。

### 5. 安装 systemd 服务

仓库里已经准备好了脚本：

```bash
cd /www/CareerPilotDJY
bash scripts/install_systemd_service.sh /www/CareerPilotDJY careerpilot
```

安装后你可以用下面的命令查看状态：

```bash
systemctl status careerpilot
journalctl -u careerpilot -n 100 --no-pager
```

### 6. 在 GitHub 仓库里配置 Actions Secrets

打开 GitHub 仓库：

`Settings -> Secrets and variables -> Actions`

新增这些 secrets：

- `ALIYUN_HOST`：阿里云服务器 IP
- `ALIYUN_PORT`：通常填 `22`
- `ALIYUN_USER`：通常是 `root`
- `ALIYUN_PROJECT_DIR`：填 `/www/CareerPilotDJY`
- `ALIYUN_SSH_KEY`：用于登录阿里云的私钥内容

注意：

- 这里放的是私钥，不是公钥
- 建议专门给 GitHub Actions 生成一对新的部署密钥

## 仓库里已经加好的自动部署文件

### GitHub Actions 工作流

文件：

```text
.github/workflows/deploy-aliyun.yml
```

作用：

- 监听 `main` 分支 push
- 把仓库代码通过 SSH + rsync 同步到阿里云
- 在服务器执行 `scripts/deploy_server.sh`

### 服务器部署脚本

文件：

```text
scripts/deploy_server.sh
```

作用：

- 创建或复用 `.venv_server`
- 安装 `requirements.txt`
- 自动重启 `careerpilot.service`
- 如果 systemd 服务尚未安装，则退回 `nohup python serve.py`

### systemd 安装脚本

文件：

```text
scripts/install_systemd_service.sh
```

作用：

- 把项目注册成 `careerpilot.service`
- 设置开机自启
- 统一以后所有部署的重启方式

## 以后怎么更新

以后你只需要正常提交代码：

```bash
git add .
git commit -m "Update CareerPilot"
git push origin main
```

之后会自动发生这些事：

1. GitHub Actions 启动
2. 最新代码同步到阿里云
3. 阿里云自动安装新增依赖
4. 阿里云自动重启 `careerpilot` 服务

你不用再手动：

- SSH 登录服务器
- `git pull`
- `pkill`
- 手动重启 Streamlit

## 常用运维命令

### 查看线上服务状态

```bash
systemctl status careerpilot
```

### 查看部署后日志

```bash
journalctl -u careerpilot -n 100 --no-pager
```

### 手动重启服务

```bash
systemctl restart careerpilot
```

### 查看端口监听

```bash
ss -lntp | grep -E "8000|8765"
```

## 为什么这套方案更适合长期使用

相比“服务器自己 `git pull`”的旧流程，这套方案的优势是：

- 服务器不再依赖访问 GitHub
- 更新路径稳定，不容易碰到 `curl 52`
- 每次部署动作固定，减少人为操作失误
- 线上进程统一交给 systemd 管
- 后续新增依赖时也会自动安装

## 额外建议

### 1. 正式环境固定端口

建议长期固定：

- Web：`8000`
- Upload API：`8765`

### 2. 插件云上传

登录 CareerPilot 后，在左侧栏复制：

- 上传地址
- 上传令牌

填到浏览器插件后，插件抓到的 JD 会直接上传到服务器，不再依赖本机 `Downloads`。

### 3. 不要把这些文件提交到 Git

已经在 `.gitignore` 里排除了：

- `.env`
- `*.db`
- `*.log`
- `uploaded_jd/`
- 本地虚拟环境

## 首次上线建议顺序

1. 先把这次仓库改动 push 到 GitHub
2. 在阿里云上创建 `/www/CareerPilotDJY/.env`
3. 在 GitHub 配置 Actions Secrets
4. 首次手动运行一次 `scripts/install_systemd_service.sh`
5. 之后只用 `git push origin main`

这就是后续最省心、也最适合持续更新的软件部署方式。
