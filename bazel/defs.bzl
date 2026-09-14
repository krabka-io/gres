"""Bazel rules for Cargo workspace members.

Names, features, editions, and dependencies come from the Cargo metadata that
rules_rs generates. BUILD files state only source layout and test data.
"""

load("@crates//:data.bzl", "DEP_DATA")
load("@crates//:defs.bzl", "all_crate_deps", "crate_name", "edition")
load("@rules_rs//rs:rust_binary.bzl", "rust_binary")
load("@rules_rs//rs:rust_library.bzl", "rust_library")
load("@rules_rs//rs:rust_test.bzl", "rust_test")
load("@rules_rs_mutants//mutants:cargo_mutants_test.bzl", "cargo_mutants_test")
load("@rules_rust//rust:defs.bzl", "rust_doc_test")

WORKSPACE_RUSTC_FLAGS = ["-Funsafe_code"]

def _features():
    return DEP_DATA[native.package_name()]["crate_features"]

def _aliases(kinds):
    data = DEP_DATA[native.package_name()]
    labels = {}
    for kind in kinds:
        for dep in data.get(kind, []):
            labels[dep] = True
        for platform_deps in data.get(kind + "_by_platform", {}).values():
            for dep in platform_deps:
                labels[dep] = True
    return {
        label: name
        for label, name in data["aliases"].items()
        if label in labels
    }

def crate_library(name, srcs = None, **kwargs):
    rust_library(
        name = name,
        srcs = srcs if srcs != None else native.glob(
            ["src/**/*.rs"],
            exclude = ["src/bin/**"],
        ),
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_name = crate_name(),
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True),
        **kwargs
    )

def crate_binary(name, crate_root, lib, **kwargs):
    """Build a Cargo binary target and link its package library."""
    rust_binary(
        name = name,
        srcs = [crate_root],
        aliases = _aliases(["deps"]),
        crate_features = _features(),
        crate_name = crate_name(),
        crate_root = crate_root,
        edition = edition(),
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        visibility = ["//visibility:public"],
        deps = all_crate_deps(normal = True) + [lib],
        **kwargs
    )

def crate_tests(
        lib,
        compile_data = None,
        data = None,
        env = {},
        manual = [],
        mutants = True,
        mutants_jobs = 4,
        mutants_shards = 8,
        rustc_env = {}):
    """Emit unit, doc, integration, and integration-aware mutation targets."""
    unit = lib + "_test"
    integration_srcs = native.glob(["tests/*.rs"], allow_empty = True)
    integration_stems = [src[len("tests/"):-len(".rs")] for src in integration_srcs]

    rust_test(
        name = unit,
        aliases = _aliases(["deps", "dev_deps"]),
        crate = ":" + lib,
        compile_data = compile_data or [],
        crate_features = _features(),
        data = data or [],
        edition = edition(),
        env = env,
        rustc_env = rustc_env,
        rustc_flags = WORKSPACE_RUSTC_FLAGS,
        deps = all_crate_deps(normal_dev = True),
    )

    rust_doc_test(
        name = lib + "_doc_test",
        crate = ":" + lib,
        deps = all_crate_deps(normal_dev = True),
    )

    helpers = native.glob(
        ["tests/**/*.rs"],
        exclude = ["tests/*.rs"],
        allow_empty = True,
    )
    for src in integration_srcs:
        stem = src[len("tests/"):-len(".rs")]
        rust_test(
            name = stem + "_test",
            srcs = [src] + helpers,
            aliases = _aliases(["deps", "dev_deps"]),
            compile_data = compile_data or [],
            crate_features = _features(),
            crate_root = src,
            data = data or [],
            edition = edition(),
            env = env,
            rustc_env = rustc_env,
            rustc_flags = WORKSPACE_RUSTC_FLAGS,
            tags = ["manual"] if stem in manual else [],
            deps = all_crate_deps(normal = True, normal_dev = True) + [":" + lib],
        )

    if mutants:
        cargo_mutants_test(
            name = lib + "_mutants",
            integration_tests = [
                ":" + stem + "_test"
                for stem in integration_stems
                if stem not in manual
            ],
            jobs = mutants_jobs,
            library = ":" + lib,
            shard_count = mutants_shards,
            tags = ["manual"],
            test = ":" + unit,
            timeout = "long",
        )
