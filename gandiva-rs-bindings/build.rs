/*
 * Copyright 2024 JasonLi-cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::ffi::OsStr;
use std::process::Command;
use std::{env, fs};

use once_cell::sync::Lazy;

static RUST_PROJ_DIR: Lazy<String> =
    Lazy::new(|| std::env::var("CARGO_MANIFEST_DIR").expect("Manifest dir is always set by cargo"));

static PROTO_SOURCE_DIR: Lazy<String> = Lazy::new(|| format!("{}/proto", RUST_PROJ_DIR.as_str()));

static CPP_SOURCE_DIR: Lazy<String> = Lazy::new(|| format!("{}/cpp", RUST_PROJ_DIR.as_str()));

static OUT_DIR: Lazy<String> =
    Lazy::new(|| std::env::var("OUT_DIR").expect("Cannot find OUT_DIR environment variable"));

static ARROW_PROJ_DIR: Lazy<String> = Lazy::new(|| format!("{}/arrow", RUST_PROJ_DIR.as_str()));

static ARROW_CPP_BUILD_DIR: Lazy<String> = Lazy::new(|| match env::var("ARROW_CPP_BUILD_DIR") {
    Ok(s) => s,
    Err(_) => {
        let arrow_cpp_build_parent = format!("{}/arrow-cpp", OUT_DIR.as_str());
        compile_arrow_cpp(&arrow_cpp_build_parent);
        format!("{}/build", arrow_cpp_build_parent)
    }
});

#[inline]
fn rust_proj_dir() -> &'static str {
    RUST_PROJ_DIR.as_str()
}

#[inline]
fn proto_source_dir() -> &'static str {
    PROTO_SOURCE_DIR.as_str()
}

#[inline]
fn cpp_source_dir() -> &'static str {
    CPP_SOURCE_DIR.as_str()
}

#[inline]
fn out_dir() -> &'static str {
    OUT_DIR.as_str()
}

#[inline]
fn arrow_proj_dir() -> &'static str {
    ARROW_PROJ_DIR.as_str()
}

#[inline]
fn arrow_cpp_build_dir() -> &'static str {
    ARROW_CPP_BUILD_DIR.as_str()
}

fn main() {
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    compile_gandiva_rs();
    generate_bindings();
    generate_rs_proto();
}

fn compile_arrow_cpp(arrow_cpp_build_parent: &str) {
    println!("cargo:rerun-if-changed={}/cpp", arrow_proj_dir());

    cmake::Config::new(arrow_proj_dir())
        .configure_arg("-S")
        .configure_arg(format!("{}/cpp", arrow_proj_dir()))
        .define("ARROW_BUILD_SHARED", "OFF")
        .define("ARROW_DEPENDENCY_SOURCE", "BUNDLED")
        .define("ARROW_DEPENDENCY_USE_SHARED", "OFF")
        .define("ARROW_PROTOBUF_USE_SHARED", "OFF")
        .define("ARROW_FILESYSTEM", "ON")
        .define("ARROW_GANDIVA", "ON")
        .define("ARROW_GANDIVA_STATIC_LIBSTDCPP", "ON")
        .define("ARROW_WITH_PROTOBUF", "ON")
        .define("ARROW_WITH_ZLIB", "ON")
        .define("ARROW_WITH_ZSTD", "ON")
        .define("ARROW_USE_CCACHE", "ON")
        // .define("ARROW_SIMD_LEVEL", "AVX2")
        .define("CMAKE_BUILD_TYPE", "Release")
        .define("CMAKE_UNITY_BUILD", "OFF")
        .define("CMAKE_INSTALL_PREFIX", "install")
        .out_dir(arrow_cpp_build_parent)
        .no_build_target(true)
        .profile("Release")
        .build();
}

fn compile_gandiva_rs() {
    println!("cargo:rerun-if-changed={}", cpp_source_dir());
    println!("cargo:rerun-if-changed={}", proto_source_dir());

    let (gandiva_pb_dir, pb_cc) = generate_cpp_proto();

    let mut config = cc::Build::new();
    config.file(pb_cc);

    // Search the following directories for Cpp files to add to the compilation.
    for dir in &[cpp_source_dir()] {
        let mut entries = fs::read_dir(dir)
            .unwrap()
            .map(|r| r.unwrap().path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.extension() == Some(OsStr::new("cc")) {
                config.file(path);
            }
        }
    }

    // Include headers
    let protobuf_headers = format!("{}/protobuf_ep-install/include", arrow_cpp_build_dir());
    let arrow_headers = format!("{}/cpp/src", arrow_proj_dir());
    config.include(protobuf_headers);
    config.include(arrow_headers);
    config.include(&gandiva_pb_dir);
    config.include(cpp_source_dir());

    // Link libs
    target_link_libraries();

    // Compile!
    config
        .cpp(true)
        .flag("-std=c++17")
        .warnings(false)
        .shared_flag(false)
        .static_flag(true)
        .compile("arrow_gandiva_rs");
}

fn target_link_libraries() {
    struct Lib {
        ty: &'static str,
        name: &'static str,
    }

    impl Lib {
        fn new_dylib(name: &'static str) -> Self {
            Self { ty: "dylib", name }
        }

        fn new_static(name: &'static str) -> Self {
            Self { ty: "static", name }
        }
    }

    struct Dependent {
        path: String,
        libs: Vec<Lib>,
    }

    let llvm_lib = find_llvm().expect("Could not find LLVM on this system");

    let openssl_lib = find_openssl().expect("Could not find OpenSSL on this system");

    let deps = vec![
        Dependent {
            path: llvm_lib,
            libs: vec![Lib::new_dylib("LLVM")],
        },
        Dependent {
            path: openssl_lib,
            libs: vec![Lib::new_static("crypto"), Lib::new_static("ssl")],
        },
        Dependent {
            path: format!(
                "{}/jemalloc_ep-prefix/src/jemalloc_ep/lib",
                arrow_cpp_build_dir()
            ),
            libs: vec![Lib::new_static("jemalloc")],
        },
        Dependent {
            path: format!("{}/protobuf_ep-install/lib", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("protobuf")],
        },
        Dependent {
            path: format!("{}/re2_ep-install/lib", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("re2")],
        },
        Dependent {
            path: format!("{}/utf8proc_ep-install/lib", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("utf8proc")],
        },
        Dependent {
            path: format!("{}/zlib_ep/src/zlib_ep-install/lib", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("z")],
        },
        Dependent {
            path: format!("{}/zstd_ep-install/lib", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("zstd")],
        },
        Dependent {
            path: format!("{}/release", arrow_cpp_build_dir()),
            libs: vec![Lib::new_static("arrow"), Lib::new_static("gandiva")],
        },
    ];

    for dep in deps {
        println!("cargo:rustc-link-search=native={}", dep.path);
        for lib in dep.libs {
            println!("cargo:rustc-link-lib={}={}", lib.ty, lib.name);
        }
    }
}

fn generate_cpp_proto() -> (String, String) {
    let out_dir = out_dir();
    let generated_pb_dir = format!("{}/gandiva", out_dir);
    fs::create_dir_all(&generated_pb_dir).expect("Create gandiva pb dir");

    let protoc = format!("{}/protobuf_ep-install/bin/protoc", arrow_cpp_build_dir());
    Command::new(protoc)
        .args([
            "--cpp_out",
            &generated_pb_dir,
            "--proto_path",
            &format!("{}/gandiva", proto_source_dir()),
            &format!("{}/gandiva/types.proto", proto_source_dir()),
        ])
        .status()
        .expect("Failed to generate protobuf code");

    let generated_pb_cc = format!("{}/types.pb.cc", generated_pb_dir);
    (out_dir.to_string(), generated_pb_cc)
}

fn generate_bindings() {
    let bindings = bindgen::Builder::default().header(format!("{}/wrapper.h", cpp_source_dir()));
    let bindings = bindings
        .blocklist_type("max_align_t")
        .size_t_is_usize(true)
        .rustified_enum(".*")
        .clang_args(&["-x", "c++"])
        .clang_arg(format!("-I{}/cpp/src", arrow_proj_dir()))
        .clang_arg("-std=c++17");

    let bindings = bindings.generate().expect("Unable to generate bindings");
    let out_path = format!("{}/src/bindings.rs", rust_proj_dir());
    bindings
        .write_to_file(out_path)
        .expect("Could not write bindings");
}

fn generate_rs_proto() {
    println!("cargo:rerun-if-changed={}", proto_source_dir());

    prost_build::Config::new()
        .compile_protos(
            &[format!("{}/gandiva/types.proto", proto_source_dir())],
            &[format!("{}/gandiva", proto_source_dir())],
        )
        .expect("Compile ProtoBuf");

    let from = format!("{}/gandiva.types.rs", out_dir());
    let to = format!("{}/src/proto/gandiva/types.rs", rust_proj_dir());
    std::fs::copy(from, to).expect("Copy file");
}

////// FIND LIBS CODE //////

fn find_llvm() -> Option<String> {
    let arrow_llvm_versions = [
        "18.1", "17.0", "16.0", "15.0", "14.0", "13.0", "12.0", "11.1", "11.0", "10", "9", "8", "7",
    ];

    for version in arrow_llvm_versions {
        let sub_versions: Vec<&str> = version.splitn(2, '.').collect();

        #[cfg(target_os = "macos")]
        let opt = find_llvm_macos(sub_versions[0]);

        #[cfg(not(target_os = "macos"))]
        let opt = find_llvm_linux(sub_versions[0]);

        if opt.is_some() {
            return opt;
        }
    }
    None
}

#[cfg(target_os = "macos")]
fn find_llvm_macos(version: &str) -> Option<String> {
    find_libs_macos(&format!("llvm@{}", version))
}

#[cfg(not(target_os = "macos"))]
fn find_llvm_linux(version: &str) -> Option<String> {
    let mut llvm_hits = vec![];
    if let Ok(path) = env::var("LLVM_ROOT") {
        llvm_hits.push(path);
    }
    if let Ok(path) = env::var("LLVM_DIR") {
        llvm_hits.push(path);
    }
    llvm_hits.push("/usr/lib".to_string());
    llvm_hits.push("/usr/lib64".to_string());

    for path in llvm_hits {
        if let Ok(dirs) = fs::read_dir(&path) {
            for dir in dirs {
                let maybe_llvm_path = dir.unwrap().path();
                let llvm_dir_name = maybe_llvm_path.file_name().unwrap().to_string_lossy();
                if llvm_dir_name.starts_with("llvm") && llvm_dir_name.contains(version) {
                    let lib = maybe_llvm_path.join("lib");
                    if lib.is_dir() {
                        return Some(lib.display().to_string());
                    }
                }
            }
        }
    }
    None
}

// End find llvm

fn find_openssl() -> Option<String> {
    #[cfg(target_os = "macos")]
    {
        let openssl_versions = ["", "3", "3.0", "1.1"];
        for version in openssl_versions {
            let opt = find_openssl_macos(version);
            if opt.is_some() {
                return opt;
            }
        }
    }

    let openssl_lib = pkg_config::Config::new()
        .probe("openssl")
        .expect("Could not find OpenSSL on this system");
    Some(openssl_lib.link_paths[0].display().to_string())
}

#[cfg(target_os = "macos")]
fn find_openssl_macos(version: &str) -> Option<String> {
    find_libs_macos(&format!("openssl@{}", version))
}

// End find openssl

#[cfg(target_os = "macos")]
fn find_libs_macos(lib_name: &str) -> Option<String> {
    let prefix_output = Command::new("brew")
        .args(["--prefix", lib_name])
        .output()
        .ok()?;
    if !prefix_output.status.success() {
        return None;
    }

    let prefix = String::from_utf8(prefix_output.stdout)
        .ok()?
        .trim()
        .to_string();

    Some(format!("{}/lib", prefix))
}
