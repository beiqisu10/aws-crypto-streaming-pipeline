# =================================================================
# 1. BASE NETWORKING (VPC)
# =================================================================
module "vpc" {
  source = "terraform-aws-modules/vpc/aws"

  name = "crypto-pipeline-vpc"
  cidr = "10.0.0.0/16"

  azs             = ["us-east-1a", "us-east-1b"]
  private_subnets = ["10.0.1.0/24", "10.0.2.0/24"]
  public_subnets  = ["10.0.101.0/24", "10.0.102.0/24"]

  # Enable NAT Gateway to allow MSK/MWAA in private subnets to access PyPI/APIs
  enable_nat_gateway = true
  single_nat_gateway = true
}

# =================================================================
# 2. STORAGE LAYER (S3 DATA LAKE)
# =================================================================
resource "aws_s3_bucket" "crypto_lake" {
  bucket = "sbq-crypto-data-lake-2026"
  force_destroy = true # Crucial for "terraform destroy" to remove buckets with data
}

resource "aws_s3_bucket_public_access_block" "block" {
  bucket = aws_s3_bucket.crypto_lake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# =================================================================
# 3. STREAMING LAYER (AMAZON MSK)
# =================================================================
resource "aws_security_group" "msk_sg" {
  name        = "msk-security-group"
  vpc_id      = module.vpc.vpc_id

  # Allow internal VPC traffic for Kafka (9092)
  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = [module.vpc.vpc_cidr_block]
  }

  # SELF-REFERENCING RULE: Fixes Glue/Redshift connectivity issues
  ingress {
    from_port = 0
    to_port   = 65535
    protocol  = "tcp"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_msk_cluster" "crypto_msk" {
  cluster_name           = "crypto-streaming-cluster"
  kafka_version          = "3.5.1"
  number_of_broker_nodes = 2

  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = module.vpc.private_subnets
    security_groups = [aws_security_group.msk_sg.id]
  }

  encryption_info {
    encryption_in_transit {
      client_broker = "PLAINTEXT" # Simplified for dev; use TLS for production
    }
  }
}

# =================================================================
# 4. INGESTION LAYER (ECS FARGATE)
# =================================================================
resource "aws_ecr_repository" "producer_repo" {
  name         = "crypto-producer"
  force_delete = true 
}

resource "aws_iam_role" "producer_role" {
  name = "crypto-producer-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "msk_policy" {
  role       = aws_iam_role.producer_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonMSKFullAccess" 
}

resource "aws_iam_role_policy_attachment" "ecs_execution_policy" {
  role       = aws_iam_role.producer_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_ecs_cluster" "crypto_cluster" {
  name = "crypto-data-pipeline"
}

resource "aws_cloudwatch_log_group" "producer_log_group" {
  name              = "/ecs/crypto-producer"
  retention_in_days = 7
}

resource "aws_ecs_task_definition" "producer_task" {
  family                   = "crypto-producer"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.producer_role.arn
  task_role_arn            = aws_iam_role.producer_role.arn

  container_definitions = jsonencode([{
    name      = "crypto-producer-container"
    image     = "${aws_ecr_repository.producer_repo.repository_url}:latest"
    essential = true
    environment = [
      { name = "KAFKA_BOOTSTRAP_SERVERS", value = aws_msk_cluster.crypto_msk.bootstrap_brokers },
      { name = "KAFKA_TOPIC", value = "crypto-trades" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = aws_cloudwatch_log_group.producer_log_group.name
        "awslogs-region"        = "us-east-1"
        "awslogs-stream-prefix" = "ecs"
      }
    }
  }])
}

resource "aws_ecs_service" "producer_service" {
  name            = "crypto-producer-service"
  cluster         = aws_ecs_cluster.crypto_cluster.id
  task_definition = aws_ecs_task_definition.producer_task.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = module.vpc.public_subnets # Public subnet for Binance API access
    assign_public_ip = true
    security_groups  = [aws_security_group.msk_sg.id]
  }
}

# =================================================================
# SECRETS MANAGEMENT (SAFE STORAGE)
# =================================================================

# Define the Secret container in AWS Secrets Manager
resource "aws_secretsmanager_secret" "redshift_creds" {
  name        = "crypto-redshift-credentials"
  description = "Credentials for Redshift Serverless admin"
}

# Store the actual username and password as a JSON string
resource "aws_secretsmanager_secret_version" "redshift_creds_val" {
  secret_id     = aws_secretsmanager_secret.redshift_creds.id
  secret_string = jsonencode({
    username = "admin"
    password = "<Put your password>"
  })
}

# DATA SOURCE: Fetch the secret content after it has been created
# This allows other resources to reference the sensitive values without hardcoding
data "aws_secretsmanager_secret_version" "creds" {
  secret_id  = aws_secretsmanager_secret.redshift_creds.id
  depends_on = [aws_secretsmanager_secret_version.redshift_creds_val]
}

# =================================================================
# 5. DATA WAREHOUSE (REDSHIFT SERVERLESS)
# =================================================================

resource "aws_redshiftserverless_namespace" "crypto_db" {
  namespace_name      = "crypto-dw-namespace"
  db_name             = "cryptodb"
  
  # Decode the JSON from Secrets Manager and extract specific keys
  admin_username      = jsondecode(data.aws_secretsmanager_secret_version.creds.secret_string)["username"]
  admin_user_password = jsondecode(data.aws_secretsmanager_secret_version.creds.secret_string)["password"]
}

resource "aws_redshiftserverless_workgroup" "crypto_wg" {
  workgroup_name = "crypto-dw-workgroup"
  namespace_name = aws_redshiftserverless_namespace.crypto_db.namespace_name
  base_capacity  = 8 
  
  subnet_ids         = module.vpc.private_subnets
  security_group_ids = [aws_security_group.msk_sg.id]
  publicly_accessible = false
}

# =================================================================
# 6. PROCESSING LAYER (AWS GLUE STREAMING)
# =================================================================
resource "aws_iam_role" "glue_role" {
  name = "crypto-glue-streaming-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_extra_policy" {
  name = "glue-extra-access"
  role = aws_iam_role.glue_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["kafka:*", "s3:*", "redshift-serverless:*"]
        Resource = "*"
      }
    ]
  })
}

resource "aws_glue_connection" "vpc_connection" {
  name = "glue-vpc-connector"
  connection_type = "NETWORK"
  connection_properties = {}

  physical_connection_requirements {
    availability_zone      = "us-east-1a"
    security_group_id_list = [aws_security_group.msk_sg.id]
    subnet_id              = module.vpc.private_subnets[0]
  }
}

resource "aws_glue_job" "crypto_streaming_job" {
  name     = "crypto-streaming-processor"
  role_arn = aws_iam_role.glue_role.arn
  glue_version = "4.0"
  
  command {
    name            = "gluestreaming"
    script_location = "s3://${aws_s3_bucket.crypto_lake.bucket}/scripts/spark_stream_processor.py"
    python_version  = "3"
  }

  connections = [aws_glue_connection.vpc_connection.name]
  worker_type       = "G.1X"
  number_of_workers = 2

  default_arguments = {
    "--bootstrap_servers"   = aws_msk_cluster.crypto_msk.bootstrap_brokers
    "--redshift_host"       = aws_redshiftserverless_workgroup.crypto_wg.endpoint[0].address
    "--checkpoint_location" = "s3://${aws_s3_bucket.crypto_lake.bucket}/checkpoint/"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

# =================================================================
# 7. ORCHESTRATION LAYER (AMAZON MWAA)
# =================================================================
resource "aws_iam_role" "mwaa_role" {
  name = "crypto-mwaa-execution-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = { Service = ["airflow.amazonaws.com", "airflow-env.amazonaws.com"] }
    }]
  })
}

resource "aws_iam_role_policy" "mwaa_policy" {
  name = "crypto-mwaa-policy-v2"
  role = aws_iam_role.mwaa_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{ Effect = "Allow", Action = "*", Resource = "*" }]
  })
}

resource "aws_security_group" "mwaa_sg" {
  name        = "mwaa-sg"
  vpc_id      = module.vpc.vpc_id

  ingress {
    from_port = 0
    to_port   = 0
    protocol  = "-1"
    self      = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_mwaa_environment" "crypto_airflow" {
  name               = "crypto-streaming-airflow"
  execution_role_arn = aws_iam_role.mwaa_role.arn
  airflow_version    = "2.8.1"
  environment_class  = "mw1.small"
  webserver_access_mode = "PUBLIC_ONLY"

  source_bucket_arn    = aws_s3_bucket.crypto_lake.arn
  dag_s3_path          = "airflow-assets/dags"
  requirements_s3_path = "airflow-assets/requirements.txt"

  network_configuration {
    security_group_ids = [aws_security_group.mwaa_sg.id]
    subnet_ids         = module.vpc.private_subnets
  }
}

# Cross-SG Rule: Allows MWAA to talk to Redshift
resource "aws_security_group_rule" "mwaa_to_redshift" {
  type                     = "ingress"
  from_port                = 5439
  to_port                  = 5439
  protocol                 = "tcp"
  security_group_id        = aws_security_group.msk_sg.id 
  source_security_group_id = aws_security_group.mwaa_sg.id 
}