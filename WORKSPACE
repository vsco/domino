http_archive(
    name = "io_bazel_rules_go",
    sha256 = "90bb270d0a92ed5c83558b2797346917c46547f6f7103e648941ecdb6b9d0e72",
    url = "https://github.com/bazelbuild/rules_go/releases/download/0.8.1/rules_go-0.8.1.tar.gz",
)

load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains", "go_repository")

go_rules_dependencies()

go_register_toolchains()

go_repository(
    name = "com_github_aws_aws_sdk_go",
    commit = "v1.8.16",
    importpath = "github.com/aws/aws-sdk-go",
)

go_repository(
    name = "com_github_stretchr_testify",
    commit = "f6abca593680b2315d2075e0f5e2a9751e3f431a",
    importpath = "github.com/stretchr/testify",
)

go_repository(
    name = "com_github_go_ini_ini",
    commit = "6e4869b434bd001f6983749881c7ead3545887d8",
    importpath = "github.com/go-ini/ini",
)

go_repository(
    name = "com_github_pmezard_go_difflib",
    commit = "792786c7400a136282c1664665ae0a8db921c6c2",
    importpath = "github.com/pmezard/go-difflib",
)

go_repository(
    name = "com_github_davecgh_go_spew",
    commit = "782f4967f2dc4564575ca782fe2d04090b5faca8",
    importpath = "github.com/davecgh/go-spew",
)

go_repository(
    name = "com_github_jmespath_go_jmespath",
    commit = "bd40a432e4c76585ef6b72d3fd96fb9b6dc7b68d",
    importpath = "github.com/jmespath/go-jmespath",
)
