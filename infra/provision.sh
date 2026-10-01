#!/usr/bin/env bash
# Provisions the AirBreda infrastructure in AWS eu-north-1 with the AWS CLI.
# Idempotent: re-running skips resources that already exist.
# Terraform was deliberately not used: this is a one-off, 5-day course deployment
# (see ADR-004). The script is the reproducibility record instead.
set -euo pipefail
cd "$(dirname "$0")"

export AWS_REGION=eu-north-1
NAME=airbreda
OWNER=maxsteshkin
BUCKET="${NAME}-${OWNER}-raw"
VPC_ID=$(aws ec2 describe-vpcs --filters Name=isDefault,Values=true --query 'Vpcs[0].VpcId' --output text)
SUBNET_ID=$(aws ec2 describe-subnets --filters Name=vpc-id,Values="$VPC_ID" Name=availability-zone,Values=eu-north-1a --query 'Subnets[0].SubnetId' --output text)
MY_IP=$(curl -s https://checkip.amazonaws.com)
AMI_ID=$(aws ssm get-parameter --name /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 --query 'Parameter.Value' --output text)
DB_PASSWORD=$(grep '^DB_PASSWORD=' ../.env | cut -d= -f2-)

log() { printf '\n==> %s\n' "$*"; }

# ---------- S3 ----------
log "S3 bucket $BUCKET"
if ! aws s3api head-bucket --bucket "$BUCKET" 2>/dev/null; then
  aws s3api create-bucket --bucket "$BUCKET" --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
fi
aws s3api put-public-access-block --bucket "$BUCKET" --public-access-block-configuration \
  BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# ---------- Security groups ----------
sg_id() { aws ec2 describe-security-groups --filters Name=group-name,Values="$1" Name=vpc-id,Values="$VPC_ID" --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null; }
allow() { aws ec2 authorize-security-group-ingress --group-id "$1" --protocol tcp --port "$2" ${3:+--cidr "$3"} ${4:+--source-group "$4"} >/dev/null 2>&1 || true; }

log "Security group for VM"
VM_SG=$(sg_id "${NAME}-vm-sg")
if [ "$VM_SG" = "None" ] || [ -z "$VM_SG" ]; then
  VM_SG=$(aws ec2 create-security-group --group-name "${NAME}-vm-sg" --description "AirBreda VM: SSH from admin IP, dashboard 8000 public" --vpc-id "$VPC_ID" --query GroupId --output text)
fi
allow "$VM_SG" 22   "${MY_IP}/32"
allow "$VM_SG" 8000 "0.0.0.0/0"

log "Security group for RDS"
DB_SG=$(sg_id "${NAME}-db-sg")
if [ "$DB_SG" = "None" ] || [ -z "$DB_SG" ]; then
  DB_SG=$(aws ec2 create-security-group --group-name "${NAME}-db-sg" --description "AirBreda RDS: Postgres from VM SG and admin IP" --vpc-id "$VPC_ID" --query GroupId --output text)
fi
allow "$DB_SG" 5432 "${MY_IP}/32"
allow "$DB_SG" 5432 "" "$VM_SG"

# ---------- RDS ----------
log "RDS instance ${NAME}-db"
if ! aws rds describe-db-instances --db-instance-identifier "${NAME}-db" >/dev/null 2>&1; then
  aws rds create-db-instance \
    --db-instance-identifier "${NAME}-db" \
    --engine postgres --engine-version 18.6 \
    --db-instance-class db.t3.micro \
    --allocated-storage 20 --storage-type gp3 \
    --master-username airbreda --master-user-password "$DB_PASSWORD" \
    --db-name airbreda \
    --vpc-security-group-ids "$DB_SG" \
    --publicly-accessible \
    --backup-retention-period 1 \
    --no-multi-az --no-deletion-protection \
    --tags Key=Project,Value=AirBreda >/dev/null
fi

# ---------- Key pair ----------
log "Key pair ${NAME}-key"
KEY_FILE="$HOME/.ssh/${NAME}-key.pem"
if ! aws ec2 describe-key-pairs --key-names "${NAME}-key" >/dev/null 2>&1; then
  aws ec2 create-key-pair --key-name "${NAME}-key" --key-type ed25519 --query KeyMaterial --output text > "$KEY_FILE"
  chmod 400 "$KEY_FILE"
fi

# ---------- IAM role / instance profile ----------
log "IAM role ${NAME}-vm-role"
if ! aws iam get-role --role-name "${NAME}-vm-role" >/dev/null 2>&1; then
  aws iam create-role --role-name "${NAME}-vm-role" --assume-role-policy-document file://ec2-trust-policy.json >/dev/null
fi
aws iam put-role-policy --role-name "${NAME}-vm-role" --policy-name "${NAME}-s3-rw-own-bucket" --policy-document file://vm-s3-policy.json
if ! aws iam get-instance-profile --instance-profile-name "${NAME}-vm-profile" >/dev/null 2>&1; then
  aws iam create-instance-profile --instance-profile-name "${NAME}-vm-profile" >/dev/null
  aws iam add-role-to-instance-profile --instance-profile-name "${NAME}-vm-profile" --role-name "${NAME}-vm-role"
  sleep 10  # IAM is eventually consistent; EC2 needs to see the profile
fi

# ---------- EC2 ----------
log "EC2 instance ${NAME}-vm"
INSTANCE_ID=$(aws ec2 describe-instances --filters Name=tag:Name,Values="${NAME}-vm" Name=instance-state-name,Values=pending,running --query 'Reservations[0].Instances[0].InstanceId' --output text)
if [ "$INSTANCE_ID" = "None" ] || [ -z "$INSTANCE_ID" ]; then
  INSTANCE_ID=$(aws ec2 run-instances \
    --image-id "$AMI_ID" --instance-type t3.micro \
    --key-name "${NAME}-key" \
    --security-group-ids "$VM_SG" --subnet-id "$SUBNET_ID" \
    --iam-instance-profile Name="${NAME}-vm-profile" \
    --user-data file://user-data.sh \
    --block-device-mappings 'DeviceName=/dev/xvda,Ebs={VolumeSize=8,VolumeType=gp3}' \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=${NAME}-vm},{Key=Project,Value=AirBreda}]" \
    --query 'Instances[0].InstanceId' --output text)
fi

log "Waiting for EC2 to run and RDS to become available"
aws ec2 wait instance-running --instance-ids "$INSTANCE_ID"
VM_IP=$(aws ec2 describe-instances --instance-ids "$INSTANCE_ID" --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
aws rds wait db-instance-available --db-instance-identifier "${NAME}-db"
DB_HOST=$(aws rds describe-db-instances --db-instance-identifier "${NAME}-db" --query 'DBInstances[0].Endpoint.Address' --output text)

# ---------- Record ----------
cat > resources.env <<EOR
VPC_ID=$VPC_ID
SUBNET_ID=$SUBNET_ID
VM_SG=$VM_SG
DB_SG=$DB_SG
INSTANCE_ID=$INSTANCE_ID
VM_IP=$VM_IP
DB_HOST=$DB_HOST
BUCKET=$BUCKET
KEY_FILE=$KEY_FILE
AMI_ID=$AMI_ID
EOR
# write DB_HOST into .env (replace the PENDING placeholder or the previous host)
sed -i.bak "s|^DB_HOST=.*|DB_HOST=$DB_HOST|" ../.env && rm -f ../.env.bak

log "Done"
cat resources.env
