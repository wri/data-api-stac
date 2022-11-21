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


