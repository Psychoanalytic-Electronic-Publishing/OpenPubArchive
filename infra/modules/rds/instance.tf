resource "aws_db_instance" "mysql" {
  allocated_storage     = 100
  max_allocated_storage = 250
  identifier            = var.env
  db_name               = "opascentral"
  engine                = "mysql"
  engine_version        = "8.0.35"
  instance_class        = var.instance_class
  username              = var.username
  password              = var.password
  parameter_group_name  = "default.mysql8.0"
  skip_final_snapshot   = true
  db_subnet_group_name  = aws_db_subnet_group.main.name
  vpc_security_group_ids = [
    aws_security_group.db.id,
  ]
  backup_retention_period    = 7
  backup_window              = "03:50-04:20"
  auto_minor_version_upgrade = false
  availability_zone          = var.availability_zone
  deletion_protection        = true
  copy_tags_to_snapshot      = true
  publicly_accessible        = true

  tags = {
    stack = var.stack_name
    env   = var.env
  }
}
