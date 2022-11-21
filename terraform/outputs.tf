output "image_url" {
  value = module.container_registry.repository_url
}

output "data_api_stac_lambda_name" {
  value = aws_lambda_function.data_api_stac.function_name
}

output "data_api_stac_lambda_arn" {
  value = aws_lambda_function.data_api_stac.arn
}
