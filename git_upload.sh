#!/bin/sh
## Step1 建立git仓库
git init
## Step2 将项目的所有文件添加到仓库中
git add .
## Step3 添加 README.md
git add README.md
## Step4 提交到仓库
git commit -m "说明语句"
## Step5 将本地的仓库关联到GitHub
git remote add origin https://github.com/GS-ChengChen/GSCAP_Script.git
## Step6 上传github之前pull一下
git pull origin master
## Step7 上传代码到GitHub远程仓库
git push -u origin master
