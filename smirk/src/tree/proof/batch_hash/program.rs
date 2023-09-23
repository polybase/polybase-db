use std::sync::OnceLock;

use miden_assembly::Assembler;
use miden_processor::Program;

pub(super) fn program() -> &'static Program {
    static PROGRAM: OnceLock<Program> = OnceLock::new();
    static TEXT: &str = include_str!("./batch_hash.masm");

    PROGRAM.get_or_init(|| Assembler::default().compile(TEXT).unwrap())
}
