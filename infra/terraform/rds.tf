# Source database. Publicly reachable only from var.admin_cidrs (so the generator can seed it).
# Databricks never connects to it: DMS lands the data in S3.

resource "random_password" "db" {
  length  = 32
  special = false
}

resource "aws_db_subnet_group" "this" {
  name       = "${local.name}-db"
  subnet_ids = aws_subnet.public[*].id
}

resource "aws_db_parameter_group" "pg" {
  name   = "${local.name}-pg16-logical"
  family = "postgres16"
  parameter {
    name         = "rds.logical_replication"
    value        = "1"
    apply_method = "pending-reboot"
  }
  parameter {
    name         = "wal_sender_timeout"
    value        = "0"
    apply_method = "immediate"
  }
}

resource "aws_db_instance" "src" {
  identifier              = "${local.name}-src"
  engine                  = "postgres"
  engine_version          = "16"
  instance_class          = var.db_instance_class
  allocated_storage       = 20
  storage_type            = "gp3"
  storage_encrypted       = true
  db_name                 = "energy_src"
  username                = "lake_admin"
  password                = random_password.db.result
  db_subnet_group_name    = aws_db_subnet_group.this.name
  vpc_security_group_ids  = [aws_security_group.rds.id]
  parameter_group_name    = aws_db_parameter_group.pg.name
  publicly_accessible     = true
  backup_retention_period = 1
  skip_final_snapshot     = true
  apply_immediately       = true
}

resource "aws_secretsmanager_secret" "db" {
  name                    = "${local.name}/source-db"
  recovery_window_in_days = 0
}

resource "aws_secretsmanager_secret_version" "db" {
  secret_id = aws_secretsmanager_secret.db.id
  secret_string = jsonencode({
    username = aws_db_instance.src.username
    password = random_password.db.result
    host     = aws_db_instance.src.address
    port     = aws_db_instance.src.port
    dbname   = aws_db_instance.src.db_name
  })
}
