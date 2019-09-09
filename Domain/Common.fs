namespace Domain.Common

//module Result =

//    type Result<'E,'V> = 
//        | Error of 'E
//        | Value of 'V

//    let retn value = Value value

//    let map (f: 'V -> 'V2) value =
//        match value with
//        | Error e -> Error e
//        | Value v -> f v |> retn
        
//    let bind (f: 'V -> Result<'E, 'V2>) value =
//        match value with
//        | Error e -> Error e
//        | Value v -> f v

//    let (>>=) x f = bind f x

//    let (>=>) f1 f2 = fun x -> x |> f1 |> bind f2
