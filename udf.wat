(module
  (func $add (param $lhs i64) (param $rhs i64) (result i64)
    get_local $lhs
    get_local $rhs
    i64.add)
  (export "add" (func $add))
)