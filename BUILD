load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    importpath = "github.com/vsco/domino",
    srcs = [
        "domino.go",
        "expression.go",
    ],
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_aws_aws_sdk_go//aws:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/request:go_default_library",
        "@com_github_aws_aws_sdk_go//service/dynamodb:go_default_library",
        "@com_github_aws_aws_sdk_go//service/dynamodb/dynamodbattribute:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["domino_test.go"],
    data = ["//:dynamodb"],
    embed = [":go_default_library"],
    deps = [
        "@com_github_aws_aws_sdk_go//aws:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/credentials:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/session:go_default_library",
        "@com_github_aws_aws_sdk_go//service/dynamodb:go_default_library",
        "@com_github_aws_aws_sdk_go//service/dynamodb/dynamodbattribute:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

#### Atlassian / Localstack ####
genrule(
    name = "dynamodb",
    outs = ["dynamodb-local-run-id"],
    cmd = "docker stop dynamodb-local || true; docker rm -f dynamodb-local || true; docker run -d -p 4569:8000 --name dynamodb-local amazon/dynamodb-local:latest > $@",
    local = 1,
    message = "Spinning up dynamo-db container...",
    visibility = ["//visibility:public"],
)
