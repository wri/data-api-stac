terraform {

  backend "s3" {
    key     = "wri__data_api_stac.tfstate"
    region  = "us-east-1"
    encrypt = true
  }
}

module "container_registry" {
  source     = "git::https://github.com/wri/gfw-terraform-modules.git//terraform/modules/container_registry?ref=v0.4.2.2"
  image_name = "${local.project}${local.name_suffix}"
  root_dir   = "../${path.root}"
}


resource "aws_lambda_function" "data_api_stac" {
  function_name = substr("${local.project}-data_api_stac${local.name_suffix}", 0, 64)
  role          = aws_iam_role.data_api_stac_lambda.arn
  package_type  = "Image"
  image_uri     = "${module.container_registry.repository_url}:latest"
  timeout       = 900


  publish = true
  tags    = local.tags



  environment {
    variables = {
      ENV              = var.environment
      GFW_DATA_API_KEY = var.data_api_key
      STAC_BUCKET      = var.stac_bucket
      DATA_API_URL     = var.data_api_url
    }
  }
}

resource "aws_cloudwatch_event_rule" "everyday-4-am-est" {
  name                = substr("everyday-4-am-est${local.name_suffix}", 0, 64)
  description         = "Run everyday at 4 am EST"
  schedule_expression = "cron(0 9 ? * * *)"
  tags                = local.tags
}

resource "aws_cloudwatch_event_target" "nightly-sync-integrated" {
  rule      = aws_cloudwatch_event_rule.everyday-3-am-est.name
  target_id = substr("${local.project}-nightly-sync${local.name_suffix}", 0, 64)
  arn       = aws_lambda_function.data_api_stac.arn
  input     = "{\"datasets\": [\"gfw_integrated_alerts\"]}"
  # count     = var.environment == "production" ? 1 : 0
}

resource "aws_lambda_permission" "allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.data_api_stac.function_name
  principal     = "events.amazonaws.com"
}
