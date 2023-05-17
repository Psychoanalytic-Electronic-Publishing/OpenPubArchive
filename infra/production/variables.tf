variable "env" {
  description = "Environment name"
  default     = "production"
}

variable "aws_region" {
  description = "AWS region"
  default     = "us-east-1"
}

variable "account_id" {
  description = "AWS account ID"
}

variable "stack_name" {
  description = "Root name for the stack"
  default     = "opas"
}

variable "security_group_ids" {
  description = "Security group ID"
  default     = ["sg-0b6da54293d1ad2e1"]
}