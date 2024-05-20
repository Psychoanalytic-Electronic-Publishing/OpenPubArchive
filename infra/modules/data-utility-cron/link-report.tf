resource "aws_cloudwatch_event_rule" "weekly_link_report" {
  count = var.env == "production" ? 1 : 0

  name                = "${var.stack_name}-weekly-link-report-${var.env}"
  description         = "Run ${var.env} state machine every Friday at 03:00 UTC"
  schedule_expression = "cron(0 3 ? * 6 *)"
}

resource "aws_cloudwatch_event_target" "weekly_link_report_target" {
  count = var.env == "production" ? 1 : 0

  rule     = aws_cloudwatch_event_rule.weekly_link_report[0].name
  role_arn = aws_iam_role.allow_cloudwatch_to_execute_role.arn
  arn      = var.state_machine_arn
  input = jsonencode({
    "task" : [
      [
        {
          "limitRam" : false,
          "directory" : "opasDataLoader",
          "utility" : "opasDataLinker",
          "args" : "--verbose --report-links 7 --report-name weekly_reference_report",
        }
      ]
    ]
  })
}

