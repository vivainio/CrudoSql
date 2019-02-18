module Crudo.Db

open SqlFrags.SqlGen
open System
open System.Data

module DbConnector =
    open System.Diagnostics

    let MsSql = "" // typeof<System.Data.SqlClient.SqlConnection>.AssemblyQualifiedName
    let OleDb = "" // typeof<System.Data.OleDb.OleDbConnection>.AssemblyQualifiedName

    // let ODP = typeof<Oracle.ManagedDataAccess.Client.OracleConnection>.AssemblyQualifiedName
    [<DebuggerDisplay("{provider} -> {connectionString}")>]
    type Connector(provider : string, connectionString : string) =

        let createConnection() =
            let typ = Type.GetType(provider)
            System.Activator.CreateInstance(typ, connectionString) :?> IDbConnection

        let lazyConnection = lazy (createConnection())

        // reuse same dbconnection instance.
        member x.Reuse() =
            let conn = lazyConnection.Value
            if conn.State <> ConnectionState.Open then conn.Open()
            conn

        // create new connection. You need to call conn.Open() yourself
        member x.Connect() =
            let conn = createConnection()
            conn.Open()
            conn

        member x.Show() =
            sprintf "%s :: %s" (provider.Split(',').[0]) connectionString
        member x.Syntax() =
            if provider.Contains "SqlClient" then SqlSyntax.Any
            else SqlSyntax.Ora

    type ConnectionStringFrag =
        | Provider of string
        | ProviderAccess
        | DataSource of string
        | Catalog of string
        | UserId of string
        | Password of string
        | IntegratedSecurity of bool
        member x.Str =
            match x with
            | Provider s -> "Provider=" + s
            | DataSource s -> "Data Source=" + s
            | Catalog s -> "Initial Catalog=" + s
            | IntegratedSecurity true -> "Integrated Security=True"
            | IntegratedSecurity false -> "Integrated Security=no"
            | ProviderAccess -> "Provider=Microsoft.ACE.OLEDB.12.0"
            | Password s -> "Password=" + s
            | UserId s -> "User Id=" + s

    let ConnectionString(parts : ConnectionStringFrag seq) =
        parts
        |> Seq.map (fun f -> f.Str)
        |> String.concat ";"

    let OracleConnectionString host port service username password =
        sprintf
            "Data Source=(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%d))(CONNECT_DATA=(SERVICE_NAME=%s)));User Id=%s;Password=%s;"
            host port service username password

    let mutable DefaultConnector =
        Connector(MsSql,
                  ConnectionString [ DataSource "localhost"
                                     Catalog "IA"
                                     IntegratedSecurity true ])

open DbConnector

let ConnectToDefault() =
    let connector = DefaultConnector
    connector.Connect()

// data as read from raw db query
type RawTableRec =
    { Name : string
      Header : string []
      Rows : obj [] [] }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module RawTableRec =
    let FilterCols (cols : string []) (table : RawTableRec) =
        let indices =
            table.Header
            |> Array.mapi (fun i h ->
                   if Array.contains h cols then Some(i)
                   else None)
            |> Array.choose id
        seq {
            for row in table.Rows do
                yield (indices |> Array.map (row |> Array.get))
        }

    let ChooseCols f (table : RawTableRec) =
        seq {
            for row in table.Rows do
                let chosen =
                    row
                    |> Array.mapi (fun i col -> (row, table.Header.[i], col))
                    |> Array.choose f
                yield chosen
        }

    let KeyValPairs(trec : RawTableRec) =
        seq {
            for row in trec.Rows do
                yield (trec.Header
                       |> Array.mapi (fun i hname -> (hname, row.[i])))
        }
        |> Array.ofSeq

    let AsMaps trec =
        let pairs = KeyValPairs trec
        seq {
            for row in pairs do
                yield Map.ofSeq row
        }
        |> Array.ofSeq

type TableCell =
    | Any of obj
    | Link of string * string // href, label
    member x.Val =
        match x with
        | Any o -> o.ToString()
        | Link(_, label) -> label

type ForeignKey =
    { Target : ColRef
      Summarizer : ColRef }

type TableHint =
    | SortCol of string
    | ValueFilter of string * (string -> string)
    | FillAll

type DeferFunc = TableSpec -> ColSpec -> ColSpec option

and ColSpec =
    | Prim of string
    | Col of string
    | Fk of string * ForeignKey
    | Summ of string
    | Defer of ColSpec * DeferFunc
    member x.Name =
        match x with
        | Prim s -> s
        | Col s -> s
        | Summ s -> s
        | Fk(col, fk) -> col
        | Defer(cs, _) -> cs.Name

and TableSpec() =
    member val Name = "" with get, set
    member val Cols : ColSpec [] = [||] with get, set
    member val Hints = ResizeArray<TableHint>() with get, set
    with

        member x.Prim =
            x.Cols
            |> Array.pick (function
                   | Prim n -> Some n
                   | _ -> None)

        member x.Summ =
            x.Cols
            |> Array.tryPick (function
                   | Summ n -> Some n
                   | _ -> None)
            |> Option.defaultValue x.Cols.[0].Name

        member x.ForeignKeys =
            x.Cols
            |> Array.filter (function
                   | Fk _ -> true
                   | _ -> false)

        member x.GetCol name = x.Cols |> Array.find (fun col -> col.Name = name)
        member x.TryGetCol name =
            x.Cols |> Array.tryFind (fun col -> col.Name = name)
        member x.Ref = Table x.Name

type DbSpec() =
    member val Tables = ResizeArray<TableSpec>() with get, set
    member x.TryFindTable name =
        x.Tables |> Seq.tryFind (fun t -> t.Name = name)
    member x.ReferringForeignKeys(toTable : Table) =
        x.Tables
        |> Seq.choose (fun t ->
               let fksToHere =
                   t.ForeignKeys
                   |> Array.filter (function
                          | Fk(_, fk) when fk.Target.Table = toTable.Name ->
                              true
                          | _ -> false)
               if Array.isEmpty fksToHere then None
               else Some(t, fksToHere))
        |> Seq.toArray

let readToArr (r : IDataReader) =
    [| 0..(r.FieldCount - 1) |] |> Array.map (fun i -> r.GetValue i)

type Conn() =
    let conn = ConnectToDefault()
    member x.Syntax = DefaultConnector.Syntax()
    member x.Connection = conn
    member x.Sql q = serializeSql x.Syntax q

    // not really useful with mssql
    member x.WithObjs sql (values : obj seq) =
        let q = conn.CreateCommand()
        q.CommandText <- sql
        for v in values do
            let p = q.CreateParameter()
            p.Value <- v
            q.Parameters.Add(p) |> ignore
        x.QueryImpl q

    member x.WithParams sql (values : (string * obj) seq) =
        let q = conn.CreateCommand()
        q.CommandText <- sql
        for (k, v) in values do
            let p = q.CreateParameter()
            p.ParameterName <- k
            p.Value <- v
            q.Parameters.Add(p) |> ignore
        x.QueryImpl q

    member x.Query tableName sql =
        let q = conn.CreateCommand()
        q.CommandText <- sql
        x.QueryImpl q

    // tableName is optional informational argument, set to "" if don't care
    member x.QueryImpl q =
        use reader = q.ExecuteReader()
        let headers =
            [| 0..(reader.FieldCount - 1) |]
            |> Array.map (fun i -> reader.GetName i)
        let valArray = Array.zeroCreate reader.FieldCount
        { Name = "tablename"
          Header = headers
          Rows =
              seq {
                  while reader.Read() do
                      let valArray = Array.zeroCreate reader.FieldCount
                      reader.GetValues(valArray) |> ignore
                      yield valArray
              }
              |> Seq.toArray }

let RunSql(query : Frag seq) =
    let conn = Conn()
    query
    |> serializeSql SqlSyntax.Any
    |> conn.Query ""
    |> fun resp -> (resp.Rows, resp)

let cast2<'a, 'b> (objs : obj []) = (objs.[0] :?> 'a, objs.[1] :?> 'b)
let cast3<'a, 'b, 'c> (objs : obj []) =
    (objs.[0] :?> 'a, objs.[1] :?> 'b, objs.[2] :?> 'c)

let Test() =
    let db = Conn()

    let q =
        [ SelectS [ "*" ]

          FromS
              [ "INFORMATION_SCHEMA.TABLE_CONSTRAINTS Tab";
                "INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE Col" ]
          WhereS "Col.Constraint_Name = Tab.Constraint_Name" ]
        |> serializeSql SqlSyntax.Any

    let res = db.Query "" q

    let testq q =
        q
        |> serializeSql SqlSyntax.Any
        |> fun s ->
            printfn "%s" s
            s
            |> db.Query ""
            |> printfn "%A"

    let User = Table "ADM_USER_DATA"
    testq [ SelectS [ "count(1)" ]
            Raw "from"
            NestAs("Users",
                   [ SelectS [ "*" ]
                     From User ]) ]
    testq [ SelectS [ "*" ]
            From(Table "PE_PROCESS_TASK")
            WhereS "recipient_id in "
            Nest [ Select [ User.Col("ID") ]
                   From User ] ]
    res
