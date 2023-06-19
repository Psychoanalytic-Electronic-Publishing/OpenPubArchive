data "aws_subnets" "private" {
  filter {
    name   = "vpc-id"
    values = [var.vpc_id]
  }
}

resource "aws_security_group" "solr" {
  name        = "${var.stack_name}-solr-sg-${var.env}"
  description = "Allow HTTP inbound traffic and all outbound traffic"
  vpc_id      = var.vpc_id

  ingress {
    description      = "HTTP From anywhere"
    from_port        = 80
    to_port          = 80
    protocol         = "tcp"
    cidr_blocks      = ["90.207.64.214/32"]
    ipv6_cidr_blocks = ["::/0"]
  }

  ingress {
    description      = "HTTP From anywhere"
    from_port        = 8983
    to_port          = 8983
    protocol         = "tcp"
    cidr_blocks      = ["90.207.64.214/32"]
    ipv6_cidr_blocks = ["::/0"]
  }


  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  tags = {
    Name = "${var.stack_name}-solr-sg-${var.env}"
  }
}
