#!/bin/bash
set -euxo pipefail

apt-get update -y
apt-get install -y unzip jq curl less

# Install AWS CLI v2
if ! command -v aws >/dev/null 2>&1; then
  curl -sL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip"
  unzip -q /tmp/awscliv2.zip -d /tmp
  /tmp/aws/install
fi

# Make sure the SSM agent is present and running
if ! systemctl status amazon-ssm-agent >/dev/null 2>&1; then
  snap install amazon-ssm-agent --classic || true
fi
systemctl enable snap.amazon-ssm-agent.amazon-ssm-agent.service || true
systemctl start  snap.amazon-ssm-agent.amazon-ssm-agent.service || true
