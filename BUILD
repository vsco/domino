load("@io_bazel_rules_go//go:def.bzl", "go_prefix", "go_library", "go_test")

go_prefix("github.com/vsco/domino")

go_library(
    name = "domino",
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
    name = "domino_test",
    srcs = ["domino_test.go"],
    data = ["//:dynamodb"],
    library = ":domino",
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
    outs = ["localstack-run-id"],
    cmd = "docker rm -f localstack || true; docker run -d -p 4567-4576:4567-4576 --name localstack atlassianlabs/localstack:0.4.1 > $@",
    local = 1,
    message = "Spinning up localstack container...",
    visibility = ["//visibility:public"],
)
