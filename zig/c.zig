pub const c = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    // Without NDEBUG the MSVC headers expose secure-API inline shims that
    // translate-c cannot digest, so a Debug or ReleaseSafe build on Windows
    // fails to import Python.h at all. Defining it here keeps the translation
    // identical across optimize modes - and matches a release interpreter.
    @cDefine("NDEBUG", {});
    @cInclude("Python.h");
});
