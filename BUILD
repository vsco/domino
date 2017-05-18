load("@io_bazel_rules_go//go:def.bzl", "go_prefix", "go_library", "go_test")

go_prefix("github.com/vsco/domino")


go_library(
  	name = "domino",
  	srcs=["domino.go", "expression.go"],
	deps=[
		"@com_github_aws_aws_sdk_go//aws:go_default_library",
		"@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
		"@com_github_aws_aws_sdk_go//service/dynamodb:go_default_library",
		"@com_github_aws_aws_sdk_go//service/dynamodb/dynamodbattribute:go_default_library",
	]
)
 
go_test(
 	name  ="domino_test",
 	srcs = ["domino_test.go"],
	library = ":domino",
 	deps = [
		"@com_github_aws_aws_sdk_go//aws:go_default_library",
                "@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
                "@com_github_aws_aws_sdk_go//service/dynamodb:go_default_library",
                "@com_github_aws_aws_sdk_go//service/dynamodb/dynamodbattribute:go_default_library",
		"@com_github_stretchr_testify//assert:go_default_library",		
		"@com_github_aws_aws_sdk_go//aws/credentials:go_default_library",
		"@com_github_aws_aws_sdk_go//aws/session:go_default_library",
		]
 )