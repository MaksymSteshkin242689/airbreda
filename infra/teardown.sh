#!/usr/bin/env bash
# Tear down everything infra/provision.sh created. Run within 48 h of submission (Day 5).
# Destructive and irreversible: asks for confirmation unless --yes is passed.
#   ./infra/teardown.sh [--yes] [--keep-bucket]
set -euo pipefail
cd "$(dirname "$0")"
export AWS_REGION=eu-north-1
NAME=airbreda
. ./resources.env

YES=0; KEEP_BUCKET=0
for a in "$@"; do case "$a" in --yes) YES=1;; --keep-bucket) KEEP_BUCKET=1;; esac; done
if [ "$YES" != 1 ]; then
  echo "This deletes EC2 $INSTANCE_ID, RDS ${NAME}-db (final snapshot kept), IAM role/profile, key pair, SGs"
  [ "$KEEP_BUCKET" = 1 ] || echo "and EMPTIES + DELETES bucket $BUCKET"
  read -r -p "Type 'destroy' to continue: " answer; [ "$answer" = destroy ] || { echo "aborted"; exit 1; }
fi
log() { printf '\n==> %s\n' "$*"; }

log "EC2"
aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" >/dev/null 2>&1 || true
aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" 2>/dev/null || true

log "RDS (final snapshot ${NAME}-db-final)"
aws rds delete-db-instance --db-instance-identifier "${NAME}-db" \
  --final-db-snapshot-identifier "${NAME}-db-final-$(date +%Y%m%d%H%M)" >/dev/null 2>&1 || true
aws rds wait db-instance-deleted --db-instance-identifier "${NAME}-db" 2>/dev/null || true

log "IAM"
aws iam remove-role-from-instance-profile --instance-profile-name "${NAME}-vm-profile" --role-name "${NAME}-vm-role" 2>/dev/null || true
aws iam delete-instance-profile --instance-profile-name "${NAME}-vm-profile" 2>/dev/null || true
aws iam delete-role-policy --role-name "${NAME}-vm-role" --policy-name "${NAME}-s3-rw-own-bucket" 2>/dev/null || true
aws iam delete-role --role-name "${NAME}-vm-role" 2>/dev/null || true

log "Key pair"
aws ec2 delete-key-pair --key-name "${NAME}-key" 2>/dev/null || true

log "Security groups (DB first: it references the VM SG)"
aws ec2 delete-security-group --group-id "$DB_SG" 2>/dev/null || true
aws ec2 delete-security-group --group-id "$VM_SG" 2>/dev/null || true

if [ "$KEEP_BUCKET" != 1 ]; then
  log "S3 (empty, then delete)"
  aws s3 rm "s3://$BUCKET" --recursive >/dev/null 2>&1 || true
  aws s3api delete-bucket --bucket "$BUCKET" 2>/dev/null || true
fi

log "Remaining billable resources in $AWS_REGION (should be empty)"
aws ec2 describe-instances --query 'Reservations[].Instances[?State.Name!=`terminated`].[InstanceId,State.Name]' --output text
aws rds describe-db-instances --query 'DBInstances[].DBInstanceIdentifier' --output text
aws ec2 describe-nat-gateways --filter Name=state,Values=available --query 'NatGateways[].NatGatewayId' --output text
aws s3 ls
echo "done"
