git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    tag = "0.4.4",
)
load("@io_bazel_rules_go//go:def.bzl", "go_repositories", "new_go_repository")

go_repositories()

new_go_repository(
        name = "com_github_aws_aws_sdk_go",
        importpath="github.com/aws/aws-sdk-go",
        commit="v1.8.16"
)
new_go_repository(
        name = "com_github_stretchr_testify",
        importpath="github.com/stretchr/testify",
        commit="976c720a22c8eb4eb6a0b4348ad85ad12491a506"
)
new_go_repository(
        name = "com_github_go_ini_ini",
        importpath="github.com/go-ini/ini",
        commit="6e4869b434bd001f6983749881c7ead3545887d8"
)
new_go_repository(
        name = "com_github_pmezard_go_difflib",
        importpath="github.com/pmezard/go-difflib",
        commit="792786c7400a136282c1664665ae0a8db921c6c2"
)
new_go_repository(
        name = "com_github_davecgh_go_spew",
        importpath="github.com/davecgh/go-spew",
        commit="6d212800a42e8ab5c146b8ace3490ee17e5225f9"
)
new_go_repository(
        name = "com_github_jmespath_go_jmespath",
        importpath="github.com/jmespath/go-jmespath",
        commit="bd40a432e4c76585ef6b72d3fd96fb9b6dc7b68d"
)
