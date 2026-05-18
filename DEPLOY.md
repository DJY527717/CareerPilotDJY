# CareerPilot 部署说明

推荐部署方式：

- 代码托管到 GitHub
- GitHub Actions 自动同步到服务器
- 服务器通过 `serve.py` 启动 Web 应用和采集上传接口

## 服务组成

`serve.py` 会同时启动：

- Streamlit 主应用
- 一键网页采集上传接口

## 首次部署

### 1. 准备目录

```bash
mkdir -p /www/CareerPilotDJY
```

### 2. 安装基础环境

确保服务器具备：

- `python3`
- `python3-venv`
- `rsync`
- `systemd`

### 3. 创建环境变量文件

```bash
vi /www/CareerPilotDJY/.env
```

示例：

```text
APP_HOST=0.0.0.0
APP_PORT=8000
UPLOAD_API_HOST=0.0.0.0
UPLOAD_API_PORT=8765
APP_PUBLIC_URL=https://你的域名
UPLOAD_API_PUBLIC_URL=https://你的域名/api/capture-upload
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DBNAME
JD_EXPORT_DIR=/root/Downloads/CareerPilot_JD
APP_BUILD_LABEL=production
```

### 4. 安装 systemd 服务

```bash
cd /www/CareerPilotDJY
bash scripts/install_systemd_service.sh /www/CareerPilotDJY careerpilot
```

### 5. 查看状态

```bash
systemctl status careerpilot
journalctl -u careerpilot -n 100 --no-pager
```

## GitHub Actions Secrets

在仓库里配置：

- `ALIYUN_HOST`
- `ALIYUN_PORT`
- `ALIYUN_USER`
- `ALIYUN_PROJECT_DIR`
- `ALIYUN_SSH_KEY`

## 更新流程

日常更新只需要：

```bash
git add .
git commit -m "Update CareerPilot"
git push origin main
```

之后会自动完成：

1. GitHub Actions 启动
2. 代码同步到服务器
3. 安装新增依赖
4. 重启 `careerpilot` 服务

## 一键网页采集上传

线上部署后，CareerPilot 的 `一键网页采集` 会把采集结果直接上传到：

```text
/api/capture-upload
```

如果通过 `serve.py` 对外提供服务，`/api/capture-upload` 会与主应用同源，书签可直接在 HTTPS 招聘页面使用。公网访问请确保：

- `APP_PUBLIC_URL` 指向用户实际访问的 HTTPS 地址
- `UPLOAD_API_PUBLIC_URL` 优先使用同一个 HTTPS 域名下的 `/api/capture-upload`
- 如果单独暴露上传端口，`UPLOAD_API_PUBLIC_URL` 必须是浏览器可访问的 HTTPS 地址

## 健康检查

`serve.py` 提供健康检查接口：

```text
/api/health
```

GitHub Actions 部署脚本会在重启后等待该接口返回 200；Docker 镜像也会用它做容器健康检查。

## 常用运维命令

查看状态：

```bash
systemctl status careerpilot
```

查看日志：

```bash
journalctl -u careerpilot -n 100 --no-pager
```

手动重启：

```bash
systemctl restart careerpilot
```

查看端口：

```bash
ss -lntp | grep -E "8000|8765"
```
