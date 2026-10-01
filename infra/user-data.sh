#!/bin/bash
# EC2 user-data for the AirBreda VM (Amazon Linux 2023).
set -euxo pipefail
dnf update -y
dnf install -y docker git cronie   # AL2023 ships without cron
systemctl enable --now docker
systemctl enable --now crond
usermod -aG docker ec2-user
# 1 GB swap: t3.micro has 1 GB RAM and docker builds of pandas/sklearn images need headroom.
if [ ! -f /swapfile ]; then
  fallocate -l 1G /swapfile && chmod 600 /swapfile && mkswap /swapfile && swapon /swapfile
  echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi
mkdir -p /home/ec2-user/airbreda
chown ec2-user:ec2-user /home/ec2-user/airbreda
