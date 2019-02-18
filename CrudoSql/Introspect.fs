module Crudo.Introspect

open Db
open SqlFrags.SqlGen

type DbTypeInfo =
| Str of string 
| Num of string
| Unk of string
with 
    static member Sniff (typeName: string) =
        match typeName with
        | "text" | "decimal" | "money" | "bit" -> Str typeName
        | _ when typeName.Contains "char" -> Str typeName
        | "bit" | "float" | "real"  -> Num typeName
        | _ when typeName.Contains "int" -> Num typeName
        | _ when typeName.Contains "date" -> Str typeName
        | _ -> Unk typeName

type Introspect(conn: Conn) =
    member x.ColTypes(t: Table) = 
        let q = [
            SelectS ["COLUMN_NAME"; "DATA_TYPE"]
            From <| Table "INFORMATION_SCHEMA.COLUMNS"
            WhereS <| "TABLE_NAME = " + sqlQuoted t.Name 
        ]

        let asPairs (arr: 'a[]) = arr.[0], arr.[1]
        
        let gq = q |> serializeSql SqlSyntax.Any
        let resp = conn.Query "" gq
        resp.Rows |> Array.map asPairs 
                  |> Array.map (fun (col, typ) -> (col.ToString(), DbTypeInfo.Sniff <| typ.ToString()))
                  |> Map.ofSeq
    member x.ColInfo (t: Table) =
        let objects = Table "sys.objects"
        let columns = Table "sys.columns"
        let colAlias = Table "col"
        let objAlias = Table "o"

        let query = match conn.Syntax with 
                    | Any -> [
                                SelectS  ["col.Name";"*" ]
                                FromAs(columns,colAlias)
                                JoinOn (objects?object_id,colAlias?object_id,objAlias,"") 
                                Where [objAlias?Name === t.Name; objAlias?``type`` === "U" ] 
                             ]
                    | _ -> []
                    
        let _, r = RunSql query
        let colinfos =
            r
            |> RawTableRec.ChooseCols (fun (row, header, value) -> 
                                            if header.StartsWith("is") 
                                               then
                                                    let boolval = unbox<bool> value
                                                    if boolval then 
                                                        Some(header, row.[0] )
                                                    else None
                                                else 
                                                    None)
            |> Array.ofSeq
            |> printf "%A"
            //|> Seq.groupBy Array.head
        colinfos
        //printf "%A" colinfos

    member x.Tables() =
        let q = ([
                    SelectS ["TABLE_NAME"]
                    From <| Table "INFORMATION_SCHEMA.TABLES"
                ] |> serializeSql SqlSyntax.Any)
        let resp = conn.Query "" q
        resp.Rows |> Array.map (Array.head >> fun e -> e.ToString())
    member x.ReadAll() =
        let columns = Table "INFORMATION_SCHEMA.COLUMNS"
        let rows = RunSql [
                            Select [columns?TABLE_NAME; columns?COLUMN_NAME; columns?DATA_TYPE ]
                            From columns
                   ]
                   |> fst 
                   |> Seq.map Seq.cast<string>
                   |> Seq.groupBy Seq.head
                   |> Seq.map (fun (k, ents) -> (k, Seq.map Seq.tail ents))
                   |> Map.ofSeq

        rows
        
let Test() =
    let ix = Introspect(Db.Conn())
    ix.ColInfo (Table "ADM_USER_DATA") |> printfn "%A"
