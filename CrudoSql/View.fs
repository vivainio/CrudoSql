module Crudo.View

open Suave.Html
open Db
open SqlFrags.SqlGen
open Suave
open System.Net
open Microsoft.FSharpLu.Json
open System

let safeText txt = System.Net.WebUtility.HtmlEncode(txt) |> text
let divId id = div [ "id", id ]
let divc cl = div [ "class", cl ]
let divStyle st = div [ "style", st ]
let table = tag "table" [ "class", "table table-bordered" ]
let tr = tag "tr" []
let td = tag "td" []
let th t = (tag "th") [] (text t)
let textLink (href : string) (title : string) =
    (tag "a") [ "href", href ] (text title)

let textLinkc (href : string) (title : string) (klass : string) =
    (tag "a") [ "href", href
                "class", klass ] (text title)

let buttonLink btnclass (href : string) (title : string) =
    (tag "a") [ "href", href
                "class", "btn " + btnclass
                "role", "button" ] (text title)

let span = tag "span" []
let ul = tag "ul" []
let li = tag "li" []
let textH txt = tag "h3" [] (text txt)
let fontAwesome (iconName : string) =
    tag "i" [ "class", "fa fa-" + iconName
              "aria-hidden", "true" ] []
let awesomeUrl iconName href =
    (tag "a") [ "href", href ] [ fontAwesome iconName ]
let tagc tname klass = tag tname [ "class", klass ]
let htmlEnc s = WebUtility.HtmlEncode s

let importJsModule modName startFunc =
    script [] <| rawText (sprintf """
        document.addEventListener("DOMContentLoaded",
            () => {
                require(['%s'], m => m.%s())
            })""" modName startFunc)

let MAX_PAGE_SIZE = 2000

type TableReadResults =
    { Data : RawTableRec
      TotalCount : int
      TableName : string }

type FilterValue =
    | Eq of string * string
    | Meta of string * string

    member x.Key =
        match x with
        | Eq(k, _)
        | Meta(k, _) -> k

    member x.Value =
        match x with
        | Eq(_, v)
        | Meta(_, v) -> v

module Filters =
    let ToUrlQuery(fvals : FilterValue seq) =
        fvals
        |> Seq.map (function
               | Eq(k, v)
               | Meta(k, v) -> sprintf "%s=%s" k v)
        |> String.concat "&"

    let ToSqlWhereCondition (tableName : string) (fvals : FilterValue []) =
        fvals
        |> Seq.choose (function
               | Eq(k, v) -> sprintf "%s.%s='%s'" tableName k v |> Some
               | _ -> None)
        |> String.concat " AND "
        |> fun s ->
            if s = "" then Skip
            else WhereS s

    let ParseFromRequest(req : HttpRequest) =
        let filters =
            req.query
            |> List.choose (function
                   | (key, Some value) when key.StartsWith("$") ->
                       Meta(key, value) |> Some
                   | (key, Some value) -> Eq(key, value) |> Some
                   | _ -> None)
        Array.ofList filters

    let TryFind key (filters : FilterValue []) =
        filters |> Array.tryFind (fun f -> f.Key = key)

    let GetPage (offset : int) (batchSize : int) (totalCount: int)=
        [| Meta("$skip", string offset)
           Meta("$limit", string batchSize) 
           Meta("$count", string totalCount) 
           |]

    let WithoutPage(filters : FilterValue []) =
        filters
        |> Array.filter (function
               | Meta("$skip", _)
               | Meta("$limit", _) -> false
               | _ -> true)

module LinkGen =
    let Gridomancer endpoint =  sprintf "/assets/gridomancer.html?grid=%s" endpoint
    let TableSearch (Table table) col value =
        sprintf "/table/%s?%s=%s" table col value
    let TableRaw(Table table) = sprintf "/rawtable/%s" table
    let TableTop(Table table) = sprintf "/table/%s" table
    let SearchWithFilters searchType (Table table) filters =
        sprintf "/%s/%s?%s" searchType table (Filters.ToUrlQuery(filters))
    let ApiTableJson(Table table) = sprintf "/api/table/%s" table
    let ApiRawTableJson(Table table) = sprintf "/api/rawtable/%s" table
    let ApiCannedQuery name = sprintf "/api/canned/%s" name

    let TableGrid table = ApiTableJson table |> Gridomancer
    let TableGridRaw table = ApiRawTableJson table |> Gridomancer
    let CannedResponse url = ApiCannedQuery url |> Gridomancer

type ViewContext =
    { Filters : FilterValue []
      Db : DbSpec
      Req : HttpRequest }

let mapPairs f = Seq.map (fun t -> f (fst t) (snd t))

let CreatePager totalCount pageSize currentSkip hrefGenerator =
    let fullPageCount = totalCount / pageSize

    let strides =
        [| for i in 1..fullPageCount -> ((i - 1) * pageSize, (i * pageSize - 1)) |]

    let lastPage = [| (fullPageCount * pageSize, totalCount) |]
    [ strides; lastPage ]
    |> Array.concat
    |> mapPairs
           (fun st en ->
           (textLink (hrefGenerator st en totalCount) (sprintf "%d - %d" st en)),
           st = currentSkip)
    |> Seq.map (fun (nod, isActive) -> tag "li" (if isActive then
                                                     [ "class", "active" ]
                                                 else []) [ nod ])
    |> List.ofSeq
    |> tag "ul" [ "class", "pagination" ]

let renderPage content =
    let loadCss url =
        tag "link" [ "href", url
                     "rel", "stylesheet" ] []
    let loadJs url = script [ "src", "/assets/js/" + url ] []
    let navLink href txt =
        tag "li" [] [ tag "a" [ "class", "nav-link"
                                "href", href ] (text txt) ]
    html [ "lang", "en" ] [ head [] [ title [] "CrudoSql"

                                      loadCss "/assets/bootstrap.min.css"
                                      loadCss "/assets/crudosql.css"
                                      tag "meta" [ "http-equiv",
                                                   "Content-Language"
                                                   "content", "en" ] [] ]
                            body [] [ tagc "nav" "navbar navbar-default"
                                          [ tagc "ul" "nav navbar-nav"
                                                [ navLink "/" "Home"
                                                  navLink "/alltables/" "All"
                                                  navLink "/meta/" "Metadata"
                                                  navLink "/log/" "SQL log"
                                                  navLink "/canned" "Canned"
                                                  ] ]
                                      divc "container-fluid" [ content ] ] ]
    |> htmlToString

let tableView headingNode allTools tableNode filters preamble
    (readResults : TableReadResults) =
    let filterNode =
        if Seq.isEmpty filters then []
        else (text (Filters.ToUrlQuery filters))

    let full =
        divId "tableview" ([ tag "h3" []
                                 [ textLink
                                       (LinkGen.TableTop
                                            (Table readResults.TableName))
                                       readResults.TableName ]
                             divc "btn-group pull-right" allTools
                             tag "em" [] [ divc "filter" [ span filterNode ] ] ]
                           @ preamble @ [ divStyle "clear: both" []
                                          tableNode ])
    full

let bytesAsGuid (o: obj) =
    if o.GetType() = typeof<System.DBNull> then "null" else
    let b = o:?> byte[]
    Guid(b).ToString("N")

let renderTyped (t: Type) (o: obj) =
    if t = typeof<byte[]> then bytesAsGuid o else
    o.ToString()


let TableRaw (t : RawTableRec) tableName filters (readResult : TableReadResults) =
    let heading =
        t.Header
        |> Seq.map th
        |> List.ofSeq

    let toCell (idx: int) (o : obj) = renderTyped t.Types.[idx] o |> text

    let singleElementTable (keys : string []) (values : obj []) =
        let pairs = Array.zip keys values

        let trs =
            pairs
            |> Array.mapi (fun idx (k, v) ->
                   tr [ td (text k)
                        td (toCell idx v) ])
            |> List.ofArray
        table trs

    let (tableElement, rowtools) =
        match t.Rows.Length with
        | 1 ->
            let singleColTools =
                [ buttonLink "btn-secondary"
                      (LinkGen.SearchWithFilters "editrow" (Table tableName)
                           filters) "Edit" ]
            (singleElementTable t.Header t.Rows.[0], singleColTools)
        | _ ->
            let rows =
                t.Rows
                |> Seq.map (fun rowData ->
                       let cells =
                           rowData
                           |> Seq.mapi (fun idx o -> toCell idx o |> td)
                           |> List.ofSeq
                       tr cells)
                |> List.ofSeq

            let tableRows = [ tr heading ] @ rows
            (table tableRows, [])

    //let allTools = [ buttonLink "btn-primary" (LinkGen.TableTop (Table tableName)) "Unraw" ]
    let allTools =
        rowtools
        @ [ buttonLink "btn-secondary"
                (LinkGen.SearchWithFilters "table" (Table tableName) filters)
                "Unraw"

            buttonLink "btn-secondary"
                (LinkGen.TableGridRaw (Table tableName))
                "Grid"
           ]
    tableView (textH tableName) allTools tableElement filters [] readResult
    |> renderPage

let cellToHtml (c : TableCell) =
    match c with
    | TableCell.Any(o) -> span (o.ToString() |> text)
    | Link(link, label) -> textLink link label

let TableRich (cols : ColSpec []) (tab : TableCell [] []) (spec : TableSpec)
    (dbSpec : DbSpec) filters (readResult : TableReadResults) =
    let colNames =
        cols
        |> Seq.map (fun col -> col.Name)
        |> List.ofSeq

    let heading =
        colNames
        |> Seq.map th
        |> List.ofSeq

    let rowTds (rowData : TableCell []) =
        rowData
        |> Array.map (fun c -> [ cellToHtml c ] |> td)
        |> List.ofArray

    let rows = tab |> Array.map rowTds

    //|> Array.truncate 1000
    // got tds, now create trs maybe
    let columnValue (colSpecs : ColSpec []) (row : TableCell []) colName =
        colSpecs
        |> Array.findIndex (fun c -> c.Name = colName)
        |> Array.get row
        |> fun c -> c.Val

    let (tableNode, toolLinks, tableLinks) =
        match rows.Length with
        | 1 -> // table with 1 row provides extra features
            let tdList = rows.[0]

            let pairs =
                tdList
                |> List.zip colNames
                |> Array.ofList

            let keyValAsTr (key : string, value) =
                tr [ td (text key)
                     value ]

            let trLines =
                pairs
                |> Array.map keyValAsTr
                |> List.ofArray

            let foreignKeysToLink =
                dbSpec.ReferringForeignKeys(Table spec.Name)
                |> Array.collect
                       (fun (t, fks) -> fks |> Array.map (fun fk -> (t, fk)))
                |> Array.sortBy (fun (ts, cs) -> cs.Name)

            let links =
                seq {
                    // exist to redece repetition
                    let mutable previousColValue = ""
                    for (foreignTable, fk) in foreignKeysToLink do
                        let colInOtherTable = fk.Name
                        let colInThisTable = spec.Prim
                        let valueIntThisTable =
                            columnValue cols tab.[0] colInThisTable

                        let marker =
                            if previousColValue = colInOtherTable then ""
                            else " (" + colInOtherTable + ")"
                        previousColValue <- colInOtherTable
                        let linkhref =
                            LinkGen.SearchWithFilters "table"
                                (Table foreignTable.Name)
                                [ Eq(colInOtherTable, valueIntThisTable) ]
                        yield divc "tablelink" [ (fontAwesome "link")

                                                 (textLink linkhref
                                                      foreignTable.Name)
                                                 text marker |> span ]
                }
                |> Seq.toList

            let singleColTools =
                [ buttonLink "btn-secondary"
                      (LinkGen.SearchWithFilters "editrow" (Table spec.Name)
                           filters) "Edit" ]
            (table trLines, singleColTools, links)
        | _ ->
            let nodesUnderTr (nodes : Node list) = tr nodes

            let trLines =
                rows
                |> Array.map nodesUnderTr
                |> List.ofArray

            let tableRows = [ tr heading ] @ trLines
            let noPaging = Filters.WithoutPage filters
            let makeHref start end_ total =
                LinkGen.SearchWithFilters "table" (Table spec.Name)
                    (Array.concat [ noPaging
                                    Filters.GetPage start (end_ - start) total ])


            let currentSkip =
                filters
                |> Filters.TryFind "$skip"
                |> Option.defaultValue (Meta("$skip", "0"))
                |> fun f -> int f.Value

            let pager =
                CreatePager readResult.TotalCount MAX_PAGE_SIZE currentSkip
                    makeHref
            (table tableRows, [], [ pager ])

    let allTools =
        toolLinks
        @ [ buttonLink "btn-secondary"
                (LinkGen.SearchWithFilters "rawtable" (Table spec.Name) filters)
                "Raw"
            buttonLink "btn-secondary"
                (LinkGen.TableGrid (Table spec.Name))
                "Grid"
            ]
    tableView (textH (sprintf "%s (%d)" spec.Name readResult.TotalCount))
        allTools tableNode filters tableLinks readResult |> renderPage

let emitPageData (id : string) (cont : string) =
    let embedder =
        script [ "id", id
                 "type", "application/json" ] (text cont)
    embedder

type TableEditDto =
    { Fields : obj
      Spec : TableSpec option
      Filters : FilterValue [] }

let TableEdit (ctx : ViewContext) (readResult : TableReadResults) =
    let data = readResult.Data
    let row = data.Rows.[0]
    let pairs = Array.zip data.Header data.Rows.[0]
    let filters = ctx.Filters
    let tableName = readResult.TableName

    let getFormInputLine key value =
        let label = tag "label" [ "for", key ] (text key)
        let inputText initialval =
            tag "input" [ "type", "text"
                          "name", key
                          "value", value |> htmlEnc
                          "class", "form-control"
                          "id", key ] []
        divc "form-group" [ label
                            inputText value ]

    let trs =
        pairs
        |> Array.map (fun (k, v) -> getFormInputLine k (v.ToString()))
        |> List.ofArray

    let submitBtn submitval txt =
        tag "button" [ "type", "submit"
                       "name", "action"
                       "value", submitval
                       "class", "btn btn-primary" ] (text txt)
    let saveSubmit = submitBtn "update" "Save"
    let insertSubmit = submitBtn "insert" "Insert"
    let tableNode =
        tag "form" [ "action",
                     (LinkGen.SearchWithFilters "saverow" (Table tableName)
                          filters)
                     "method", "post" ] (trs @ [ saveSubmit; insertSubmit ])
    let editTools =
        [ buttonLink "btn-secondary"
              (LinkGen.SearchWithFilters "table" (Table tableName) filters)
              "Cancel" ]
    let tv =
        tableView (textH tableName) editTools tableNode filters [] readResult
    let scripts = importJsModule "app" "startEditRow"
    (*
    let testChosen = tag "select" [
                                    "data-placeholder", "villen placeholder"; "class", "chosen-select"] [
                                     tag "option" ["value","testval"] (text "hallo")
                    ]
    *)
    let pageDataHolder = emitPageData "pageDataEditRow" "PAGE_DATA_HERE"

    let pageData =
        { Fields = (Map.ofSeq pairs) :> obj
          Spec = ctx.Db.TryFindTable tableName
          Filters = ctx.Filters }
    divc "crudo-row-edit" [ tv; scripts; pageDataHolder ]
    |> renderPage
    |> String.replace "PAGE_DATA_HERE" (Compact.serialize pageData)

let Index(dbSpec : DbSpec, connectionDesc) =
    let tables = dbSpec.Tables |> Seq.map (fun t -> t.Name)

    let tablelinks =
        tables
        |> Seq.map
               (fun tname ->
               tagc "li" "list-group-item"
                   [ textLink (LinkGen.TableTop(Table tname)) tname ])
        |> List.ofSeq
    divc "crudo-index" [ divc "jumbotron"
                             [ tag "h1" [ "class", "display-3" ]
                               <| text "CrudoSql"

                               tag "p" [ "class", "lead" ]
                               <| text
                                      "Your schema, your rules. Made with <3 using F# and Suave." ]
                         divc "tablelinks" [ tagc "ul" "list-group" tablelinks ]
                         tagc "p" "text-right" <| text connectionDesc ]
    |> renderPage

let TableList(tables : string []) =
    divc "tablist" (tables
                    |> Array.map
                           (fun tname -> (tname, LinkGen.TableTop(Table tname)))
                    |> Array.map
                           (fun (tname, href) ->
                           (tag "a") [ "href", href ] (text tname))
                    |> Array.map (fun an -> li [ an ])
                    |> List.ofArray) |> renderPage
let unpack2 els = (Seq.head els, Seq.tail els |> Seq.head)

let MetaData(md : Map<string, seq<seq<string>>>) =
    md
    |> Map.toSeq
    |> Seq.map (fun (k, v) -> divc "md-entry" [ (tag "h4") []
                                                    [ textLink
                                                          (LinkGen.TableTop
                                                               (Table k)) k ]
                                                v
                                                |> Seq.map
                                                       (unpack2
                                                        >> (fun (colname, coltype) ->
                                                        tr [ td (text colname)
                                                             td (text coltype) ]))
                                                |> List.ofSeq
                                                |> table ])
    |> List.ofSeq
    |> divc "meta"
    |> renderPage

let SqlLog(ents : seq<string * string>) =
    let pres =
        ents
        |> Seq.map (fun (comment, q) ->
               [ tagc "h3" "log-comment" (text comment)
                 tagc "pre" "sqlentry" (text q) ])
    pres
    |> List.concat
    |> divc "crudo-log"
    |> renderPage


let CannedQueries (queries: (string*string) seq) =
    let links = queries
                |> Seq.map (fun (n,s) -> [
                    (tag "a") [ "href", LinkGen.CannedResponse n ] (text n)
                    tagc "pre" "sqlentry" (text s)
                ])
                |> List.concat
                |> List.ofSeq
    
    let linksn = divc "canned-links" links
    let header = tag "p" [] (text "Tip: use declCannedQuery() in crudoscript to create canned sql queries")
    divc "canned-page" [header; linksn] 
    |> renderPage
