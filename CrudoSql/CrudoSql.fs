module Crudo.CrudoSql

open Suave
open Suave.Filters
open Suave.Operators
open Suave.Successful
open Suave.RequestErrors
open Crudo
open Crudo
open SqlFrags.SqlGen
open Db
open View
open Newtonsoft.Json
open Introspect
open System
open System.Diagnostics

module String =
    let truncate count (s : string) =
        let len = String.length s
        if len > count then s.[0..(min count (String.length s) - 1)] + "..."
        else s
    let truncateAtMax count (s:string) = 
        if s.Length < count then s else s.Substring(0, count)

module SqlRunner =
    let RequestLog = ResizeArray<string * string>()

    let RunSql comment sql =
        RequestLog.Insert(0, (comment, sql))
        let db = Db.Conn()
        let t = Stopwatch.StartNew()
        let r = db.Query "" sql
        RequestLog.[0] <- (sprintf "%s [%dms]" comment t.ElapsedMilliseconds, sql)
        r
    let Run comment frags =
        printf "%s\n%A" comment frags
        let syntax = DbConnector.DefaultConnector.Syntax()
        let statement = frags |> serializeSql syntax
        RunSql comment statement

// oracle only allows 30 char id's
let truncatedName (name: string) = String.truncateAtMax 29 name

let joinedTableName s = "J_" + s |> truncatedName

// raw table has summary colrefs in order after the normal rows
//
let renderTable (cols : ColSpec []) (outputTableCols : ColRef [])
    (readResult : TableReadResults) =
    let table = readResult.Data
    let tableName = (Table readResult.TableName)

    let renderCell (spec : ColSpec) (cell : obj) (summaries : Map<ColRef, obj>) =
        match spec with
        | Col(s)
        | Summ(s) -> Any cell
        | Fk(colname, fk) ->
            let (ColRef(tab, col)) = fk.Target
            let summaryCol =
                fk.Summarizer |> function
                | ColRef(_, col) -> ColRef(Table(joinedTableName colname), col)
            let summarized = Map.tryFind summaryCol summaries
            //let summarized = Map.tryFind join summaries
            let summ2 = ()

            let value =
                match summarized with
                | Some s ->
                    sprintf "%s (%s)" (s.ToString())
                        (String.truncate 5 (cell.ToString()))
                | None -> cell.ToString()
            Link
                ((sprintf "/table/%s?%s=%s" tab.Name col (cell.ToString())),
                 value)
        | Prim(s) ->
            Link
                (LinkGen.TableSearch tableName s (cell.ToString()),
                 cell.ToString())
        | Defer(_, _) -> failwithf "Error: deferred left untranslated %A, did you call finalize()?" spec
    seq {
        for row in table.Rows do
            let summaryDict =
                Array.zip (outputTableCols.[cols.Length..])
                    (row.[cols.Length..]) |> Map.ofSeq
            let pairs = Array.zip cols row.[..cols.Length - 1]
            yield pairs
                  |> Array.map (fun (cs, ent) -> renderCell cs ent summaryDict)
    }
    |> Array.ofSeq

module ParseHints =
    let GetSorter(ts : TableSpec) =
        ts.Hints
        |> Seq.tryPick (function
               | SortCol(c) -> Some c
               | _ -> None)
        |> Option.orElse (Some ts.Summ)
        |> Option.get

let CreateViewContext(req : HttpRequest) =
    { Filters = Filters.ParseFromRequest req
      Db = SchemaGen.GetSpec()
      Req = req }


let readTable tname (isRaw : bool) (isJson: bool) (req : HttpRequest) =
    let db = Db.Conn()
    let schema = SchemaGen.GetSpec()

    let tab =
        if isRaw then None
        else schema.TryFindTable tname

    let src = Table tname

    let tableHints =
        if tab.IsSome then tab.Value.Hints |> Array.ofSeq
        else [||]

    let fillRemaining =
        tableHints
        |> Array.exists (function
               | FillAll -> true
               | _ -> false)

    let inputCols =
        if tab.IsNone then [||]
        else if fillRemaining then
            let ix = Introspect(db)

            let toFill =
                ix.ColTypes src
                |> Map.toSeq
                |> Seq.map fst
                |> Seq.filter (fun col ->
                       if Option.isSome (tab.Value.TryGetCol col) then false
                       else true)
                |> Seq.map ColSpec.Col
                |> Array.ofSeq
            Array.concat [ tab.Value.Cols; toFill ]
        else tab.Value.Cols

    let foreignKeys (tab : TableSpec) =
        //tab.ForeignKeys
        inputCols
        |> Array.choose (function
               | Fk(n, fk) -> Some(n, fk)
               | _ -> None)

    let fks =
        match tab with
        | None -> [||]
        | Some spec -> foreignKeys spec

    let joins =
        fks
        |> Seq.map
               (fun (n, fk) ->
               JoinOn
                   (fk.Target, src.Col(n), Table(joinedTableName n),
                    "LEFT OUTER"))

    let summarizers =
        fks
        |> Seq.map
               (fun (n, fk) ->
               match fk.Summarizer with
               | ColRef((Table tname), colname) ->
                   ColRef(Table(joinedTableName n), colname))
        |> Array.ofSeq

    let outputTableCols =
        match tab with
        | Some spec ->
            Array.concat [ inputCols |> Array.map (fun n -> src.Col n.Name)
                           summarizers ]
            |> Some
        | _ -> None

    let filters = Filters.ParseFromRequest req

    // figure out aliases to columns
    let aliasColumns (cols : ColRef []) =
        let mainTable = (Array.head cols).Table
        cols
        |> Array.map (fun col ->
               let (ColRef((Table tab), colname)) = col

               let tabprefix =
                   if tab = mainTable then ""
                   else tab.Replace("J_", "") + "_"
               (col, truncatedName (tabprefix + colname)))

    let (selector, sorter) =
        match tab with
        | Some spec when spec.Cols.Length > 0 ->
            let sortcol =
                ParseHints.GetSorter spec
                |> spec.GetCol
                |> (fun sorterspec ->
                match sorterspec with
                | Fk(_, fk) ->
                    sprintf "%s.%s" (joinedTableName (sorterspec.Name))
                        fk.Summarizer.Right
                | _ -> (spec.Ref.Col sorterspec.Name).Str)

            let sorterColumnIndex =
                outputTableCols.Value
                |> Array.findIndex (fun cref -> cref.Str = sortcol)
            (outputTableCols.Value
             |> aliasColumns
             |> Seq.ofArray
             |> SelectAs, OrderBy [ sprintf "%d ASC" (sorterColumnIndex + 1) ])
        | _ -> (Raw "select *", OrderBy [ "1" ])

    let whereExp = Filters.ToSqlWhereCondition src.Name filters
    let pagerSkip = Filters.TryFind "$skip" filters
    let pagerLimit = Filters.TryFind "$limit" filters
    let pagerCount = Filters.TryFind "$count" filters
    let pager =
        match filters with
        | _ when Array.contains (Meta("$limit", "off")) filters -> Skip
        | _ when pagerSkip.IsSome && pagerLimit.IsSome ->
            Page (int pagerSkip.Value.Value) (int pagerLimit.Value.Value)
        | _ -> Page 0 MAX_PAGE_SIZE

    let innerClause =
        [ //selector goes here
          From src
          Many joins
          whereExp ]

    let pagedClause =
        [ Raw "select * from"
          NestAs("rootq", [ selector ] @ innerClause)
          sorter
          pager ]

    let data = SqlRunner.Run (req.url.PathAndQuery) pagedClause
    let mutable totalCount = data.Rows.Length 
    // totalcount populated separately if we are already paging, or got a big request
    if data.Rows.Length >= MAX_PAGE_SIZE || pagerSkip.IsSome then
        let countClause = [ Raw "select Count(*)" ] @ innerClause
        let countres =
            SqlRunner.Run (req.url.PathAndQuery + " count") countClause
        totalCount <- Convert.ToInt32 (countres.Rows.[0].[0])
    let readResult: TableReadResults =
        { TableName = tname
          TotalCount = totalCount
          Data = data }
    if isJson then JsonConvert.SerializeObject readResult.Data
    else
    match tab with
    | Some spec when spec.Cols.Length > 0 ->
        let data = renderTable inputCols outputTableCols.Value readResult
        View.TableRich inputCols data spec schema filters readResult
    | _ -> View.TableRaw data tname filters readResult



let readRawTable tableName (filters : FilterValue []) =
    let whereExp = Filters.ToSqlWhereCondition tableName filters
    let t = Table tableName
    let query =
        [ t.SelectAll
          whereExp ]

    let data = SqlRunner.Run ("readRawTable " + tableName) query
    { Data = data
      TableName = tableName
      TotalCount = data.Rows.Length }

let editTable tableName (ctx : ViewContext) =
    let data = readRawTable tableName ctx.Filters
    View.TableEdit ctx data

let saveRow tableName ctx =
    let received = ctx.Req.form
    let db = Db.Conn()
    let ix = Introspect(db)
    let colTypes = ix.ColTypes(Table tableName)
    let filters = ctx.Filters

    let quoteIfNeeded colName value =
        match colTypes.TryFind colName with
        | Some(Num _) when value = "" -> "null"
        | Some(Num _) -> value
        | Some(Str _)
        | Some(Unk _) -> sqlQuoted value
        | _ -> sqlQuoted value

    let action =
        received
        |> List.find (fun el -> fst el = "action")
        |> snd
        |> Option.get

    let values =
        received
        |> List.choose (function
               | ("action", _) -> None
               | (k, Some v) -> Some(k, quoteIfNeeded k v)
               | _ -> None)

    let identityinsert tab mode =
        sprintf "set IDENTITY_INSERT %s %s" tab mode |> Raw

    let stmt =
        match action with
        | "update" ->
            [ (Table tableName).Update values
              Filters.ToSqlWhereCondition tableName filters ]
        | "insert" ->
            [ ( //identityinsert tableName "ON"
                Table tableName)
                .Insert values ]
        //identityinsert tableName "OFF"
        | _ -> failwithf "unknown action %s" action
    SqlRunner.Run (sprintf "%s %s" action ctx.Req.path) stmt |> ignore

let queryOnTable tableName = request (fun r -> OK(readTable tableName false false r))
let queryOnTableRaw tableName =
    request (fun r -> OK(readTable tableName true false r))

let queryOnEditRow tableName =
    request (fun r ->
        let ctx = CreateViewContext r
        OK(editTable tableName ctx))

let queryOnSaveRow tableName =
    request
        (fun r ->
        let ctx = CreateViewContext r
        saveRow tableName ctx
        Redirection.redirect
        <| LinkGen.SearchWithFilters "editrow" (Table tableName) ctx.Filters)

let viewAllTables() =
    let db = Conn()
    let ix = Introspect(db)
    let tables = ix.Tables() |> Array.sort
    View.TableList tables

let allMetaData() =
    let ix = Introspect(Conn())
    let md = ix.ReadAll()
    View.MetaData md |> OK


let runCannedQuery (name: string) =
    let lookup =  Seq.tryFind (fun (a,b) -> a = name) SchemaGen.cannedQueries
    let toJson d = JsonConvert.SerializeObject(d, Formatting.Indented)
    match lookup with
    | None -> None
    | Some (name, callable) ->
        SqlRunner.RunSql name (callable()) |> toJson |> Some
    

let showIndex() =
    let conndesc =
        sprintf "Connection: %s" <| Db.DbConnector.DefaultConnector.Show()
    View.Index(SchemaGen.GetSpec(), conndesc) |> OK

let sqlLog() = View.SqlLog <| SqlRunner.RequestLog |> OK
let setStatic = Writers.addHeader "Cache-Control" "public"
let crudoAssembly = System.Reflection.Assembly.GetExecutingAssembly()

let getAsset fname =
    let aname = sprintf "CrudoSql.assets.%s" fname
    Embedded.resource crudoAssembly aname

let listCanned() =
    let all =
        SchemaGen.cannedQueries
        |> Seq.map (fun (a,b) -> a, b())
    View.CannedQueries all |> OK

module Api =
    let apiTable tableName =
        request (fun r -> OK(readTable tableName false true r))
    let rawTable tableName =
        request (fun r -> OK(readTable tableName true true r))
    
    let cannedQuery qname =
        warbler (fun _ ->
            runCannedQuery qname |> (function | None -> NOT_FOUND "command not found" | Some s -> OK s))

let app =
    choose [ // GET >=> pathRegex "/assets/.*" >=> Files.browseHome
             GET >=> path "/" >=> warbler (fun _ -> showIndex())
             GET >=> pathScan "/table/%s" queryOnTable
             GET >=> pathScan "/api/rawtable/%s" Api.rawTable
             GET >=> pathScan "/api/table/%s" Api.apiTable
             GET >=> pathScan "/api/canned/%s" Api.cannedQuery

             GET >=> pathScan "/rawtable/%s" queryOnTableRaw
             GET >=> pathScan "/editrow/%s" queryOnEditRow
             GET >=> pathScan "/assets/%s" getAsset

             GET >=> path "/alltables/"
             >=> request (fun _ -> OK(viewAllTables()))
             GET >=> path "/canned" >=> warbler (fun _ -> listCanned())

             GET >=> path "/meta/" >=> request (fun _ -> allMetaData())
             GET >=> path "/log/" >=> warbler (fun _ -> sqlLog())
             POST >=> pathScan "/saverow/%s" queryOnSaveRow ]

let StartCrudo intf port =
    let config =
        { defaultConfig with bindings =
                                 [ HttpBinding.createSimple HTTP intf port ] }
    startWebServer config app
