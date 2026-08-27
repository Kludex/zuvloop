pub const c = @cImport({
    @cDefine("PY_SSIZE_T_CLEAN", {});
    @cInclude("python_shim.h");
    @cInclude("context_shim.h");
});
