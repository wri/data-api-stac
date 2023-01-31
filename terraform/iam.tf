resource "aws_iam_policy" "data_api_stac" {
  name   = substr("${local.project}-${local.name_suffix}", 0, 64)
  path   = "/"
  policy = data.template_file.data_api_stac_policy.rendered
}


resource "aws_iam_role" "data_api_stac_lambda" {
  name               = substr("${local.project}-lambda${local.name_suffix}", 0, 64)
  assume_role_policy = data.template_file.sts_assume_role_lambda.rendered
}

resource "aws_iam_role_policy_attachment" "data_api_stac_lambda" {
  role       = aws_iam_role.data_api_stac_lambda.name
  policy_arn = aws_iam_policy.data_api_stac.arn
}
