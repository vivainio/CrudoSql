module Crudo.SchemaGen

open SqlFrags.SqlGen
open Db

let mutable dbSpec = DbSpec()
let mutable declared = dbSpec.Tables

let startSchema (s : string) =
    printfn "Declaring schema %s" s
    dbSpec <- DbSpec()
    declared <- dbSpec.Tables

let declWith (table : Table) (cols : ColSpec seq) =
    let all = Array.ofSeq cols
    let spec = TableSpec(Name = table.Name, Cols = all)
    declared.Add spec
    spec

let finalize() =
    // eliminate defers until done
    let rec undefer (tspec : TableSpec) (c : ColSpec) =
        let newcol =
            match c with
            | Defer(cc, f) ->
                // value so far
                let undeferred = undefer tspec cc
                let called = f tspec undeferred
                // if function returns some, switch column type, otherwise keep original (pass through)
                match called with
                | None -> undeferred
                | Some col -> col
            | _ -> c
        newcol
    for tspec in declared do
        //let defers = tspec.Cols |> Array.filter (function | Defer(a,b) -> true | _ -> false)
        let newCols = tspec.Cols |> Array.map (undefer tspec)
        tspec.Cols <- newCols

let decl (tab : Table) (cols : ColSpec seq) = declWith tab cols |> ignore

let foreign colName (t : Table) =
    let resolver (ts : TableSpec) (cs : ColSpec) =
        let spec = dbSpec.TryFindTable t.Name
        Fk(colName,
           { Target = t.Col spec.Value.Prim
             Summarizer = t.Col spec.Value.Summ })
        |> Some
    Defer(Col colName, resolver)

let debugCol col =
    let dumper : DeferFunc =
        fun ts cs ->
            printfn "debug %s %A" ts.Name cs
            None
    Defer(col, dumper)

let sortCol col =
    let addSortBy : DeferFunc =
        fun ts cs ->
            SortCol(cs.Name) |> ts.Hints.Add
            None
    Defer(col, addSortBy)

let prettyCol (prettyFunc : string -> string) col =
    let addFilter : DeferFunc =
        fun ts cs ->
            ValueFilter(cs.Name, prettyFunc) |> ts.Hints.Add
            None
    Defer(col, addFilter)

let hint (hint : TableHint) (ts : TableSpec) = ts.Hints.Add(hint)
let GetSpec() = dbSpec
