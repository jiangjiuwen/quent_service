# 腾讯云 OpenCloudOS / RHEL 系无脑部署手册

这份文档的目标只有一个：

- 你照着执行，就能把当前项目部署到腾讯云 OpenCloudOS 9.4 / RHEL / CentOS 系服务器
- 后续只要 `git push origin main`，GitHub Actions 就会自动发布

本文默认以下条件成立：

- 代码仓库已经在 GitHub：
  `https://github.com/jiangjiuwen/quent_service.git`
- 腾讯云服务器系统是 OpenCloudOS 9.4
- 或者其他兼容的 RHEL / CentOS 系 Linux
- 你当前至少能通过一种方式登录服务器：
  - 方式 A：直接用 `root`
  - 方式 B：用一个有 `sudo` 权限的普通用户

如果你现在不确定自己属于哪种情况，先按最常见的 `root` 场景执行。

## 0. 你最终会得到什么

部署完成后，系统会变成这样：

- 代码工作区：`/home/deploy/quent_service`
- 生产目录：`/home/deploy/quant_service_prod`
- 生产服务管理：`systemd`
- 服务端口：`18000`
- 自动部署触发方式：`git push origin main`

## 1. 先准备本地机器

这一段在你自己的电脑上执行，不是在服务器上。

### 1.1 生成一把专门用于 GitHub 自动部署的 SSH 密钥

```bash
ssh-keygen -t ed25519 -f ~/.ssh/tencent_quant_deploy -C "github-actions-deploy"
```

一路回车即可，不要覆盖你原有常用密钥。

执行完成后，你会得到两个文件：

- 私钥：`~/.ssh/tencent_quant_deploy`
- 公钥：`~/.ssh/tencent_quant_deploy.pub`

### 1.2 记下后面要用到的两段内容

后面你会用到下面两条命令的输出：

```bash
cat ~/.ssh/tencent_quant_deploy.pub
cat ~/.ssh/tencent_quant_deploy
```

说明：

- `.pub` 是公钥，要放到服务器
- 不带 `.pub` 的是私钥，要放到 GitHub Secret

## 2. 服务器首次初始化

下面分两种情况，二选一。

## 2A. 场景一：服务器现在只能用 root 登录

这是最常见的腾讯云新机情况。直接在你本地终端执行：

```bash
ssh root@<你的服务器公网IP>
```

登录服务器后，执行下面整段命令：

```bash
git clone https://github.com/jiangjiuwen/quent_service.git /root/quent_service
cd /root/quent_service
bash scripts/bootstrap_tencent_from_root.sh --user deploy --create-user
```

这一步会自动完成：

- 创建 `deploy` 用户
- 在 Debian 系加入 `sudo`，在 RHEL 系加入 `wheel`
- 安装部署依赖：
  `git`、`curl`、`python3`、`python3-pip`、`rsync`、`sqlite`/`sqlite3`、`sudo`
- 创建目录：
  `/home/deploy/quent_service`
  `/home/deploy/quant_service_prod`
- 把当前仓库复制到 `/home/deploy/quent_service`
- 给 `deploy` 配置自动部署所需的免密 `sudo`

执行完成后，不要关闭终端，继续执行第 3 步。

## 2B. 场景二：你已经可以用有 sudo 的普通用户登录

如果你已经能用一个普通用户登录服务器，例如 `deploy`，就在你本地终端执行：

```bash
ssh deploy@<你的服务器公网IP>
```

登录服务器后，执行：

```bash
git clone https://github.com/jiangjiuwen/quent_service.git /home/deploy/quent_service
cd /home/deploy/quent_service
sudo bash scripts/bootstrap_tencent_linux.sh --user deploy
```

这一步会自动完成：

- 自动识别 `dnf/yum` 或 `apt`
- 安装部署依赖
- 创建工作目录和生产目录
- 给 `deploy` 配置自动部署所需的免密 `sudo`

## 3. 把 SSH 公钥放到服务器

这一段回到你自己的电脑执行。

### 3.1 自动写入公钥

```bash
ssh-copy-id -i ~/.ssh/tencent_quant_deploy.pub deploy@<你的服务器公网IP>
```

如果 `ssh-copy-id` 不存在，也可以手动追加：

```bash
cat ~/.ssh/tencent_quant_deploy.pub
```

复制输出内容，然后登录服务器执行：

```bash
mkdir -p /home/deploy/.ssh
chmod 700 /home/deploy/.ssh
echo "<把公钥整行粘贴到这里>" >> /home/deploy/.ssh/authorized_keys
chmod 600 /home/deploy/.ssh/authorized_keys
chown -R deploy:deploy /home/deploy/.ssh
```

### 3.2 验证 deploy 用户 SSH 登录

在你自己的电脑执行：

```bash
ssh -i ~/.ssh/tencent_quant_deploy deploy@<你的服务器公网IP>
```

能正常登录就说明这一步成功。

## 4. 在服务器上做一次手动首 deploy

这一段在服务器上执行。

如果你当前还在 root 会话里，可以直接执行：

```bash
sudo -u deploy bash -lc 'cd /home/deploy/quent_service && ./scripts/prod_deploy.sh'
```

如果你已经登录的是 `deploy` 用户，就执行：

```bash
cd /home/deploy/quent_service
./scripts/prod_deploy.sh
```

正常情况下，最后你会看到类似输出：

```text
生产部署完成
平台: Linux systemd
API: http://127.0.0.1:18000
服务名: quant-service.service
```

### 4.1 检查服务状态

在服务器上执行：

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh status
```

### 4.2 查看日志

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh logs
```

按 `Ctrl + C` 退出日志查看。

### 4.3 验证接口可访问

在服务器上执行：

```bash
curl http://127.0.0.1:18000/health
```

如果返回健康检查结果，说明服务已起来。

## 5. 开放腾讯云安全组端口

如果你希望在外网访问这个服务，需要在腾讯云控制台放行端口：

- `22`：SSH
- `18000`：项目 API 端口

如果你只打算通过反向代理或内网访问，也可以不直接开放 `18000` 到公网。

## 6. 配置 GitHub Actions 自动部署

打开 GitHub 仓库：

- `Settings`
- `Secrets and variables`
- `Actions`

## 6.1 配置 Repository Variables

新增这些变量，名字和值照着填：

```text
TENCENT_CVM_HOST=<你的服务器公网IP或域名>
TENCENT_CVM_PORT=22
TENCENT_CVM_USER=deploy
TENCENT_CVM_WORKSPACE=/home/deploy/quent_service
QUANT_PROD_ROOT=/home/deploy/quant_service_prod
QUANT_PROD_API_PORT=18000
QUANT_PROD_SERVICE_NAME=quant-service
QUANT_PROD_SERVICE_USER=deploy
```

仓库里也有同样的模板文件：

- [tencent-cloud-actions-vars.example](/Users/jiangjiuwen/repos/quant_service/docs/tencent-cloud-actions-vars.example)

## 6.2 配置 Repository Secret

新增一个 Secret：

```text
TENCENT_CVM_SSH_KEY
```

值填你本地私钥内容，也就是下面命令的输出：

```bash
cat ~/.ssh/tencent_quant_deploy
```

注意：

- 填的是私钥，不是 `.pub`
- 包括 `BEGIN OPENSSH PRIVATE KEY` 到 `END OPENSSH PRIVATE KEY` 整段内容

## 7. 触发第一次 GitHub 自动部署

在你自己的电脑上，进入项目目录后执行：

```bash
git status
git push origin main
```

然后到 GitHub 查看：

- `Actions`
- 找到 `Deploy Tencent Cloud`
- 打开最新一次运行日志

正常情况下，工作流会依次完成：

- 检查部署变量是否存在
- 建立 SSH 连接
- 用 `rsync` 同步仓库到服务器
- 在服务器执行 `./scripts/prod_deploy.sh`
- 在服务器执行 `./scripts/prod_ctl.sh status`

## 8. 以后怎么发布

以后你每次改完代码，只要：

```bash
git add .
git commit -m "feat: your change"
git push origin main
```

GitHub Actions 就会自动把最新代码部署到腾讯云。

## 9. 常用运维命令

以下命令都在服务器上执行。

### 查看服务状态

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh status
```

### 重启服务

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh restart
```

### 查看日志

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh logs
```

### 手动重新部署当前代码

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh deploy
```

## 10. 故障排查

## 10.1 GitHub Actions 报 SSH 连接失败

优先检查：

- `TENCENT_CVM_HOST` 是否填对
- `TENCENT_CVM_USER` 是否填成 `deploy`
- `TENCENT_CVM_SSH_KEY` 是否填的是私钥
- 服务器 22 端口是否放通
- `deploy` 用户的 `authorized_keys` 是否已写入公钥

## 10.2 GitHub Actions 报 sudo 权限失败

在服务器上执行：

```bash
sudo -u deploy sudo -n systemctl --version
```

如果失败，重新执行首装脚本：

```bash
cd /root/quent_service
bash scripts/bootstrap_tencent_from_root.sh --user deploy
```

或者：

```bash
cd /home/deploy/quent_service
sudo bash scripts/bootstrap_tencent_linux.sh --user deploy
```

## 10.3 服务启动失败

在服务器上执行：

```bash
cd /home/deploy/quent_service
./scripts/prod_ctl.sh status
./scripts/prod_ctl.sh logs
```

如果需要直接看 `systemd` 日志：

```bash
sudo journalctl -u quant-service.service -n 200 -f
```

## 10.4 外网访问不到

优先检查：

- 腾讯云安全组是否放通 `18000`
- 服务器本地是否能访问：
  `curl http://127.0.0.1:18000/health`
- GitHub Actions 日志里部署是否成功

## 11. 最短执行清单

如果你只想看最短路径，按这个顺序做：

1. 本地生成部署密钥：

```bash
ssh-keygen -t ed25519 -f ~/.ssh/tencent_quant_deploy -C "github-actions-deploy"
```

2. 登录腾讯云 root：

```bash
ssh root@<你的服务器公网IP>
```

3. 在服务器执行：

```bash
git clone https://github.com/jiangjiuwen/quent_service.git /root/quent_service
cd /root/quent_service
bash scripts/bootstrap_tencent_from_root.sh --user deploy --create-user
sudo -u deploy bash -lc 'cd /home/deploy/quent_service && ./scripts/prod_deploy.sh'
```

4. 回到本地，把公钥写到服务器：

```bash
ssh-copy-id -i ~/.ssh/tencent_quant_deploy.pub deploy@<你的服务器公网IP>
```

5. 在 GitHub 配置 Variables 和 Secret：

- Variables 见第 6.1 节
- Secret `TENCENT_CVM_SSH_KEY` 填：

```bash
cat ~/.ssh/tencent_quant_deploy
```

6. 推送代码触发自动部署：

```bash
git push origin main
```

做到这里，后面就只剩：

```bash
git push origin main
```

## 12. 对应文件

这套部署流程依赖这些仓库文件：

- [deploy.sh](/Users/jiangjiuwen/repos/quant_service/deploy.sh)
- [prod_deploy.sh](/Users/jiangjiuwen/repos/quant_service/scripts/prod_deploy.sh)
- [prod_ctl.sh](/Users/jiangjiuwen/repos/quant_service/scripts/prod_ctl.sh)
- [bootstrap_tencent_from_root.sh](/Users/jiangjiuwen/repos/quant_service/scripts/bootstrap_tencent_from_root.sh)
- [bootstrap_tencent_linux.sh](/Users/jiangjiuwen/repos/quant_service/scripts/bootstrap_tencent_linux.sh)
- [bootstrap_tencent_ubuntu.sh](/Users/jiangjiuwen/repos/quant_service/scripts/bootstrap_tencent_ubuntu.sh)
- [deploy-tencent-cloud.yml](/Users/jiangjiuwen/repos/quant_service/.github/workflows/deploy-tencent-cloud.yml)
