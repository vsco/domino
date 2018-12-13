git_repository(
    name = "io_bazel_rules_go",
    remote = "https://github.com/bazelbuild/rules_go.git",
    tag = "0.15.8"
)
git_repository(
    name = "bazel_gazelle",
    remote = "https://github.com/bazelbuild/bazel-gazelle.git",
    tag = "0.15.0",
)
load(
    "@io_bazel_rules_go//go:def.bzl",
    "go_rules_dependencies",
    "go_register_toolchains",
)
go_rules_dependencies()
go_register_toolchains()
load(
    "@bazel_gazelle//:deps.bzl",
    "gazelle_dependencies",
    "go_repository",
)
gazelle_dependencies()

go_repository(
    name = "com_github_aws_aws_sdk_go",
    importpath="github.com/aws/aws-sdk-go",
    commit="v1.16.4"
)
go_repository(
    name = "com_github_stretchr_testify",
    importpath="github.com/stretchr/testify",
    commit="f6abca593680b2315d2075e0f5e2a9751e3f431a"
)
go_repository(
    name = "com_github_go_ini_ini",
    importpath="github.com/go-ini/ini",
    commit="6e4869b434bd001f6983749881c7ead3545887d8"
)
go_repository(
    name = "com_github_pmezard_go_difflib",
    importpath="github.com/pmezard/go-difflib",
    commit="792786c7400a136282c1664665ae0a8db921c6c2"
)
go_repository(
    name = "com_github_davecgh_go_spew",
    importpath="github.com/davecgh/go-spew",
    commit="782f4967f2dc4564575ca782fe2d04090b5faca8"
)
go_repository(
    name = "com_github_jmespath_go_jmespath",
    importpath="github.com/jmespath/go-jmespath",
    commit="bd40a432e4c76585ef6b72d3fd96fb9b6dc7b68d"
)
