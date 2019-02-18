module Crudo.DbPatterns

open SqlFrags.SqlGen
open Db
open System

type FieldValues = (string * obj) []

module FieldValues =
    let Get fname (values : FieldValues) =
        values
        |> Array.find (fun ent -> fst ent = fname)
        |> snd

    let Drop (fname : string) (values : FieldValues) =
        values |> Array.filter (fun ent -> fst ent <> fname)

    let Replace (fname : string) (newValue : obj) (values : FieldValues) =
        values
        |> Drop fname
        |> Array.append [| fname, newValue |]

type CarbonCopier(conn : Db.Conn, idCol) =

    member x.ReadMany srcTable where =
        let q =
            [ SelectS [ "*" ]
              From srcTable
              where ]

        let res =
            q
            |> conn.Sql
            |> conn.Query ""

        RawTableRec.KeyValPairs res

    member x.Read srcTable srcIdValue =
        x.ReadMany srcTable
            (WhereS(sprintf "%s = %s" idCol (sqlQuoted srcIdValue)))
        |> Array.head

    member x.Write (toTable : Table) (template : (string * obj) [])
           (overrides : (string * string) []) =
        let skipset =
            [ overrides |> Array.map fst
              [| idCol |] ]
            |> Seq.collect id
            |> Set.ofSeq

        //|> Set.ofArray
        let idd = Guid.NewGuid().ToString("N")

        let pairs =
            Array.concat [ template
                           |> Array.filter
                                  (fun p -> not (Set.contains (fst p) skipset))
                           [| for (k, v) in overrides -> k, v :> obj |]
                           [| idCol, box idd |] ]

        let sql =
            [ toTable.Insert [ for (col, _) in pairs -> (col, "@" + col) ] ]
            |> conn.Sql

        conn.WithParams sql pairs |> ignore
        idd

    member x.WriteMany (toTable : Table) (templates : (string * obj) [] [])
           (overrides : (string * string) []) =
        let skipset =
            [ overrides |> Array.map fst
              [| idCol |] ]
            |> Seq.collect id
            |> Set.ofSeq

        //|> Set.ofArray
        let inserts =
            seq {
                for template in templates do
                    let idd = Guid.NewGuid().ToString("N")

                    let pairs =
                        Array.concat [ template
                                       |> Array.filter
                                              (fun p ->
                                              not (Set.contains (fst p) skipset))

                                       [| for (k, v) in overrides -> k, v :> obj |]
                                       [| idCol, box idd |] ]
                    yield idd, pairs
            }
            |> Seq.toArray

        for idd, pairs in inserts do
            let sql =
                [ toTable.Insert [ for (col, _) in pairs -> (col, "@" + col) ] ]
                |> conn.Sql
            conn.WithParams sql pairs |> ignore
        inserts |> Array.map fst
