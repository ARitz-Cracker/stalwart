{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  name = "stalwart";

  packages = with pkgs; [
    # Rust toolchain
    cargo
    rustc
    rustfmt
    clippy

    # Code coverage. cargo-llvm-cov needs llvm-cov/llvm-profdata that match rustc's
    # bundled LLVM version (nixpkgs' rustc and llvmPackages.llvm both currently sit on
    # LLVM 21.1.8, so this is the pairing to keep in sync if either gets bumped).
    cargo-llvm-cov
    llvmPackages.llvm

    # Playground
    evcxr
    cargo-expand

    # rust-analyzer
    rust-analyzer

    # claude loves using this
    python3

    # nginx with mail proxy modules (IMAP/POP3/SMTP proxy support)
    (nginx.override { withMail = true; })

    # formatter for the *.nix files throughout the repo
    nixfmt

    # Native build dependencies. These cover every cargo feature in the
    # workspace, not just the default `rocks`/`enterprise` set, so that the
    # editor can run `cargo check --all-features` (see .vscode/settings.json).
    #
    # pkg-config resolves the buildInputs below.
    pkg-config

    # librocksdb-sys (the default `rocks` feature) runs bindgen over the RocksDB
    # C headers in its build script, and bindgen needs libclang at runtime. The
    # hook sets LIBCLANG_PATH plus the BINDGEN_EXTRA_CLANG_ARGS that point clang
    # at glibc's headers, which it can't find on its own here.
    rustPlatform.bindgenHook

    # rdkafka-sys (`kafka`) vendors librdkafka and builds it with its
    # `cmake-build` feature.
    cmake

    # Everything else that compiles C/C++ from source -- librocksdb-sys and its
    # bundled bzip2/lz4/zstd, libsqlite3-sys (`bundled`), tikv-jemalloc-sys,
    # aws-lc-sys -- only needs the cc/make/ar that stdenv already provides.

    # uncomment below if we need native dependencies for some reason
    # rustPlatform.rustLibSrc
  ];

  # Libraries the -sys crates link against. These have to be buildInputs rather
  # than packages: pkg-config's setup hook only scans host-offset inputs when
  # populating PKG_CONFIG_PATH, and the cc wrapper likewise only adds host-offset
  # inputs to NIX_LDFLAGS.
  buildInputs = with pkgs; [
    # openssl-sys, pulled in by `ece` (web push), a dev-dependency of the tests
    # crate. Needed even for a default-feature `cargo test`.
    openssl

    # libz-sys, reached via librocksdb-sys (`rocks`) and rdkafka-sys (`kafka`).
    # Both would otherwise fall back to compiling their vendored copy.
    zlib

    # librdkafka's CMake build turns on its OAUTHBEARER OIDC support as soon as
    # it detects libcurl, and then hard-fails on the missing curl/curl.h unless
    # the headers are here too.
    curl

    # foundationdb-sys (`foundationdb`) links against libfdb_c. Note that
    # nixpkgs is on 7.3 while crates/store asks for the crate's `fdb-7_4` API
    # level: that links fine because the C ABI is unchanged, but a *running*
    # server would need a 7.4 client library, since fdb_select_api_version(740)
    # is rejected by a 7.3 libfdb_c at runtime.
    foundationdb
  ];

  # nixpkgs' cc wrapper injects -D_FORTIFY_SOURCE=2 by default, but cargo's dev
  # and test profiles compile C at -O0. glibc then emits `#warning
  # _FORTIFY_SOURCE requires compiling with optimization`, and tikv-jemalloc-sys
  # runs jemalloc's configure with -Werror, so *every* feature probe fails and it
  # bails out with "cannot determine return type of strerror_r". Fortification is
  # a no-op at -O0 anyway, so drop it rather than force optimization on the C deps.
  hardeningDisable = [ "fortify" ];

  # more stuff for rust-analyzer
  RUST_SRC_PATH = "${pkgs.rustPlatform.rustLibSrc}";

  # cargo-llvm-cov looks for a rustup llvm-tools-preview component by default, which
  # doesn't exist here since nixpkgs' rustc isn't rustup-managed. Point it at the
  # llvm-cov/llvm-profdata that ship with llvmPackages.llvm above instead.
  LLVM_COV = "${pkgs.llvmPackages.llvm}/bin/llvm-cov";
  LLVM_PROFDATA = "${pkgs.llvmPackages.llvm}/bin/llvm-profdata";

  # I don't know why, I don't want to know why, I shouldn't have to wonder why.
  # But tmpdir doesn't exist unless I do this terribleness. All 3 lines.
  shellHook = ''
    bash -c 'mkdir -p $TMPDIR' &
    bash -c 'sleep 1 && mkdir -p $TMPDIR' &
    mkdir -p $TMPDIR || true
  '';
}
