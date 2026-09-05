use std::process::Command;

#[test]
fn prints_greeting() {
    let output = Command::new(env!("CARGO_BIN_EXE_rivet"))
        .output()
        .expect("failed to run rivet");

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).expect("stdout was not valid UTF-8");

    assert_eq!(stdout, "Hello, world!\n");
}
