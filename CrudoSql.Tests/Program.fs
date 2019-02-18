// Learn more about F# at http://fsharp.org
// This is where your schema etc goes. Crudo is just a library!

open Crudo
open Db
open SqlFrags.SqlGen
open SchemaGen


let createSchema() =
    startSchema "Demoschema"

    // declare tables
    let table1 = Table "table1"
    decl table1 [
        Prim "ID" |> sortCol
        Summ "SUMMARIZER_COL"
        Col "LANGUAGE_ID"
        Col "ANOTHER_COL"
    ]

    let Columns = Table "INFORMATION_SCHEMA.COLUMNS"
    decl Columns [
        Col "TABLE_NAME"
        Col "COLUMN_NAME"
        Col "DATA_TYPE"
        // Prim "ID"
    ]


module ConnectionConfig =
    open Db.DbConnector

    let mssql = typeof<System.Data.SqlClient.SqlConnection>.AssemblyQualifiedName

    let GetConnectors() =
        [
            "local",
                Connector(mssql,
                    ConnectionString [ DataSource "localhost"; Catalog "IA"; IntegratedSecurity true])

            "bigoracle",
                Connector( "NOTPROVIDED_PLEASEFIX", "(oracle-bs-here)")
        ]
    let Init() =
        DefaultConnector <- GetConnectors().[0] |> snd


let boot() =
    createSchema()
    ConnectionConfig.Init()
    // host, port
    CrudoSql.StartCrudo "0.0.0.0" 9988

[<EntryPoint>]

let main _ =
    boot()
    0 