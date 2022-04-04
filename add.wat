(module
 (export "add" (func $foo))
 (func $foo (; 0 ;) (param $0 i64) (param $1 i64) (result i64)
  (i64.add
   (get_local $1)
   (get_local $0)
  )
 )
)
