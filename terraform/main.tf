# =============================================================================
# terraform/main.tf
# AWS Infrastructure for Highway Toll Collection System
#
# Resources provisioned:
#   - VPC, subnets, security groups
#   - Amazon MSK (Managed Kafka)
#   - Amazon EMR (Spark cluster)
#   - Amazon Redshift cluster
#   - Amazon ElastiCache (Redis)
#   - S3 buckets (raw, staging, scripts)
#   - AWS Glue catalog database
#   - EC2 instance for Airflow
#
# Author : Mayukh Ghosh | Roll No : 23052334
# =============================================================================

terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# ── Variables ──────────────────────────────────────────────────────────────────
variable "aws_region"      { default = "ap-south-1" }
variable "project"         { default = "highway-toll" }
variable "redshift_pass"   { sensitive = true }

locals {
  tags = {
    Project   = var.project
    ManagedBy = "Terraform"
    Owner     = "Mayukh Ghosh"
  }
}

# ── VPC ────────────────────────────────────────────────────────────────────────
resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
  tags                 = merge(local.tags, { Name = "${var.project}-vpc" })
}

resource "aws_subnet" "private_a" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.1.0/24"
  availability_zone = "${var.aws_region}a"
  tags              = merge(local.tags, { Name = "${var.project}-private-a" })
}

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = "10.0.2.0/24"
  availability_zone = "${var.aws_region}b"
  tags              = merge(local.tags, { Name = "${var.project}-private-b" })
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${var.project}-igw" })
}

# ── Security groups ────────────────────────────────────────────────────────────
resource "aws_security_group" "kafka" {
  name   = "${var.project}-kafka-sg"
  vpc_id = aws_vpc.main.id
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  egress { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = local.tags
}

resource "aws_security_group" "redshift" {
  name   = "${var.project}-redshift-sg"
  vpc_id = aws_vpc.main.id
  ingress {
    from_port   = 5439
    to_port     = 5439
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  egress { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = local.tags
}

resource "aws_security_group" "redis" {
  name   = "${var.project}-redis-sg"
  vpc_id = aws_vpc.main.id
  ingress {
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  egress { from_port = 0; to_port = 0; protocol = "-1"; cidr_blocks = ["0.0.0.0/0"] }
  tags = local.tags
}

# ── S3 Buckets ─────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "raw" {
  bucket = "${var.project}-raw-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket" "staging" {
  bucket = "${var.project}-staging-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

resource "aws_s3_bucket" "scripts" {
  bucket = "${var.project}-scripts-${data.aws_caller_identity.current.account_id}"
  tags   = local.tags
}

data "aws_caller_identity" "current" {}

# ── Amazon MSK (Kafka) ─────────────────────────────────────────────────────────
resource "aws_msk_cluster" "toll" {
  cluster_name           = "${var.project}-kafka"
  kafka_version          = "3.6.0"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.m5.large"
    client_subnets  = [aws_subnet.private_a.id, aws_subnet.private_b.id]
    security_groups = [aws_security_group.kafka.id]
    storage_info {
      ebs_storage_info { volume_size = 100 }
    }
  }

  encryption_info {
    encryption_in_transit { client_broker = "TLS_PLAINTEXT" }
  }

  tags = local.tags
}

# ── Amazon ElastiCache Redis ───────────────────────────────────────────────────
resource "aws_elasticache_subnet_group" "redis" {
  name       = "${var.project}-redis-subnet"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
}

resource "aws_elasticache_cluster" "redis" {
  cluster_id           = "${var.project}-redis"
  engine               = "redis"
  engine_version       = "7.1"
  node_type            = "cache.t3.micro"
  num_cache_nodes      = 1
  subnet_group_name    = aws_elasticache_subnet_group.redis.name
  security_group_ids   = [aws_security_group.redis.id]
  tags                 = local.tags
}

# ── Amazon Redshift ────────────────────────────────────────────────────────────
resource "aws_redshift_subnet_group" "rs" {
  name       = "${var.project}-rs-subnet"
  subnet_ids = [aws_subnet.private_a.id, aws_subnet.private_b.id]
  tags       = local.tags
}

resource "aws_redshift_cluster" "toll" {
  cluster_identifier  = "${var.project}-redshift"
  database_name       = "tolldb"
  master_username     = "etl_user"
  master_password     = var.redshift_pass
  node_type           = "dc2.large"
  cluster_type        = "single-node"
  subnet_group_name   = aws_redshift_subnet_group.rs.name
  vpc_security_group_ids = [aws_security_group.redshift.id]
  skip_final_snapshot = true
  tags                = local.tags
}

# ── AWS Glue Database ──────────────────────────────────────────────────────────
resource "aws_glue_catalog_database" "toll" {
  name = "${var.project}_catalog"
}

resource "aws_glue_crawler" "raw" {
  name          = "${var.project}-raw-crawler"
  role          = aws_iam_role.glue.arn
  database_name = aws_glue_catalog_database.toll.name
  s3_target {
    path = "s3://${aws_s3_bucket.raw.bucket}/transactions/"
  }
  schedule = "cron(0 2 * * ? *)"   # 02:00 UTC daily
  tags     = local.tags
}

# ── IAM role for Glue ──────────────────────────────────────────────────────────
resource "aws_iam_role" "glue" {
  name = "${var.project}-glue-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "glue_s3" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# ── Outputs ────────────────────────────────────────────────────────────────────
output "kafka_bootstrap_brokers" {
  value = aws_msk_cluster.toll.bootstrap_brokers
}

output "redshift_endpoint" {
  value = aws_redshift_cluster.toll.endpoint
}

output "redis_endpoint" {
  value = aws_elasticache_cluster.redis.cache_nodes[0].address
}

output "raw_bucket" {
  value = aws_s3_bucket.raw.bucket
}
