resource "aws_db_proxy" "rds" {
  name                   = "${var.stack_name}-rds-proxy-${var.env}"
  debug_logging          = false
  engine_family          = "MYSQL"
  idle_client_timeout    = 1800
  require_tls            = false
  role_arn               = aws_iam_role.proxy.arn
  vpc_security_group_ids = [aws_security_group.db.id]
  vpc_subnet_ids         = data.aws_subnets.private.ids

  auth {
    auth_scheme = "SECRETS"
    iam_auth    = "DISABLED"
    secret_arn  = aws_secretsmanager_secret.credentials.arn
  }

  tags = {
    stack = var.stack_name
    env   = var.env
  }
}

resource "aws_db_proxy_default_target_group" "proxy_group" {
  depends_on = [aws_db_proxy.rds]

  db_proxy_name = aws_db_proxy.rds.name

  connection_pool_config {
    connection_borrow_timeout    = 120
    init_query                   = "SET x=1, y=2"
    max_connections_percent      = 100
    max_idle_connections_percent = 50
    session_pinning_filters      = ["EXCLUDE_VARIABLE_SETS"]
  }
}

resource "aws_db_proxy_target" "proxy_target" {
  depends_on = [aws_db_proxy_default_target_group.proxy_group]

  db_instance_identifier = aws_db_instance.mysql.id
  db_proxy_name          = aws_db_proxy.rds.name
  target_group_name      = aws_db_proxy_default_target_group.proxy_group.name
}
