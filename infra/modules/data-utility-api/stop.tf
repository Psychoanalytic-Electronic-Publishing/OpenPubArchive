module "stop_task_lambda" {
  source  = "terraform-aws-modules/lambda/aws"
  version = "4.9.0"

  function_name           = "${var.stack_name}-stop-task-handler-${var.env}"
  source_path             = "../../dataUtility/api"
  handler                 = "stopTask.handler"
  runtime                 = "python3.8"
  ignore_source_code_hash = true

  tags = {
    stage = var.env
    stack = var.stack_name
  }
}

resource "aws_lambda_permission" "allow_stop_task" {
  statement_id  = "${var.stack_name}-allow-stop-task-${var.env}"
  action        = "lambda:InvokeFunction"
  function_name = module.stop_task_lambda.lambda_function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.api_gateway.execution_arn}/*/*/*"
}

locals {
  stop_task_integration = {
    post = {
      x-amazon-apigateway-integration = {
        httpMethod           = "POST"
        payloadFormatVersion = "1.0"
        type                 = "AWS_PROXY"
        uri                  = module.stop_task_lambda.lambda_function_invoke_arn
      }
    },
    options = local.options
  }
}
