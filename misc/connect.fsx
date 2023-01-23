#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\MySQL\Connector NET 6.8.3\Assemblies\v4.5\MySql.Data.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5.1\System.configuration.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5.1\System.Transactions.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\FSharp\.NETFramework\v4.0\4.3.0.0\Type Providers\FSharp.Data.TypeProviders.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"



open System
open System.Linq
open System.Configuration
open System.Transactions
open Microsoft.FSharp.Data.TypeProviders


open SA2.Common.DbSchema
open SA2.Common.Dates


open MySql.Data.MySqlClient



type TestSchema = DbmlFile< "//Terra/Users/Majed/devel/InOut/test.dbml" >





let connect_data_fields () =

    let connectionString = "server=127.0.0.1;uid=root;pwd=31July!;database=sa2_multistrategy"

    let sqlConnection = new MySqlConnection ( connectionString )

    sqlConnection.Open ()


    let sqlCommandStr = "select * from tbl_data_fields"

    let sqlCommand = new MySqlCommand ( sqlCommandStr , sqlConnection )

    let sqlReader = sqlCommand.ExecuteReader ()

    while sqlReader.Read () do

        let fieldId = sqlReader.GetInt32 ( 0 )
        let fieldName = sqlReader.GetString( 1 )
        let sa2Name = sqlReader.GetString( 2 )

        printfn "%d , %s , %s" fieldId fieldName sa2Name


    sqlConnection.Close ()

    ()



let connect_instruments () =

    let connectionString = "server=127.0.0.1;uid=root;pwd=31July!;database=sa2_multistrategy"

    let sqlConnection = new MySqlConnection ( connectionString )

    sqlConnection.Open ()

    // look into MySqlDataAdapter

    let sqlCommandStr = "select * from tbl_instruments where InstrumentTicker like 'GE %'"
    let sqlCommand = new MySqlCommand ( sqlCommandStr , sqlConnection )

    let sqlReader = sqlCommand.ExecuteReader ()

    for i in 0 .. sqlReader.FieldCount - 1 do
        printfn "%s" (sqlReader.GetName ( i ))
        
    
    printfn "hasrows %d" sqlReader.RecordsAffected

    while sqlReader.Read () do
    
        let InstrumentId = sqlReader.GetInt64 ( 0 )
        let InstrumentTicker = sqlReader.GetString ( 1 )
        let Description = sqlReader.GetString( 2 )

        printfn "%d , %s , %s" InstrumentId InstrumentTicker Description

    sqlConnection.Close ()

    ()




let emptyEquityTable () =

    
    let context = createMultistrategyContext ()

    query
        {
            for row in context.Tbl_equity_data do

            select row
        }

        |> context.Tbl_equity_data.DeleteAllOnSubmit

    context.SubmitChanges ()

    query
        {
            for row in context.Tbl_equity_data do

            select row

            count
        }

        |> ( fun s -> printfn "count %d" s )



let uploadStuff () =

    let builder = new System.Data.SqlClient.SqlConnectionStringBuilder ()

    printfn "keys %A" (builder.Keys.ToString ())
    let enumerator = builder.Keys.GetEnumerator()
    for i in 0 .. builder.Keys.Count  - 1 do
        enumerator.MoveNext () |> ignore
        printfn "%s" ( enumerator.Current.ToString() )


    let connectionString = "server=127.0.0.1;uid=root;pwd=31July!;database=test"
    let sqlConnection = new MySqlConnection ( connectionString )

    let foo = Array.create 2000 0.0
    let mutable sqlCommandStr = "insert into test.testtable (idtest , testcol , testcol1 ) values "
    for i in 5 .. 7 do
        
        sqlCommandStr <- sqlCommandStr + sprintf " (%d,%s,%f)" i ( "\"" + i.ToString() + "modified" + "\"" ) ( 2.0 * (float i ))
        if i <> 7 then sqlCommandStr <- sqlCommandStr + ","

    sqlCommandStr <- sqlCommandStr + " on duplicate key update testcol=VALUES(testcol),testcol1=VALUES(testcol1)"
    printfn "%s" sqlCommandStr
    let sqlCommand = new MySqlCommand ( sqlCommandStr , sqlConnection )
    sqlConnection.Open ()
    let retCode = sqlCommand.ExecuteNonQuery () 
    sqlConnection.Close ()
    printfn "retcode %d" retCode



let loadEquity ( newRows : MultistrategySchema.Tbl_equity_data [] ) = 

    let context = createMultistrategyContext ()
    context.Tbl_equity_data.InsertAllOnSubmit( newRows )
    context.SubmitChanges ()





let transferData () =


    let connectionString = "server=127.0.0.1;uid=root;pwd=31July!;database=sa2_multistrategy"
    let sqlConnection = new MySqlConnection ( connectionString )

    try
        

        sqlConnection.Open ()

        let maxRows = 1000000
        let mutable newRows = Array.create maxRows ( new MultistrategySchema.Tbl_equity_data () )

        let prefixes = [ [ 'A' .. 'Z' ] ; [ '0' .. '9' ] ] |> List.concat

        for prefix in prefixes do
            
            printfn "%s" (prefix.ToString())

            let sqlCommandStr = "select * from tbl_equity_data where InstrumentId in ( select InstrumentId from tbl_instruments where InstrumentTicker like '" + prefix.ToString() + "%EQUITY' )"
            let sqlCommand = new MySqlCommand ( sqlCommandStr , sqlConnection )
            let sqlReader = sqlCommand.ExecuteReader ()
            printfn "%d" ( sqlReader.FieldCount )

            let mutable keepLooping = sqlReader.Read () 
            let mutable ( newRows : MultistrategySchema.Tbl_equity_data [] ) = Array.zeroCreate maxRows
            let mutable counter = 0
            while keepLooping do

                let newRow = new MultistrategySchema.Tbl_equity_data ()

                newRow.InstrumentId <- sqlReader.GetInt32( 0 ) /// "InstrumentId" )
                newRow.FieldId <- sqlReader.GetInt32( 1 ) /// "FieldId" )
                newRow.Value <- Nullable< float > ( sqlReader.GetDouble( 2 ) ) /// "Value" ) )
                newRow.Date <- sqlReader.GetDateTime( 3 ) /// "Date" )
                newRow.SourceId <- sqlReader.GetInt32( 4 ) /// "SourceId" )
                newRow.Divisor <- sqlReader.GetDouble( 5 ) /// "Divisor" )
                
                newRows.[counter] <- newRow

                keepLooping <- sqlReader.Read ()
                counter <- counter + 1

                if counter = maxRows || ( not keepLooping ) then

                    loadEquity newRows.[ 0 .. ( counter - 1 )]
                    newRows <- Array.create maxRows ( new MultistrategySchema.Tbl_equity_data () )
                    counter <- 0

            sqlReader.Close ()

        sqlConnection.Close ()

    with

    | e -> printfn "exception: %s" ( e.ToString() ) ; sqlConnection.Close ()

    ()

// execute

//connect_data_fields ()
//connect_instruments ()
//uploadStuff()

//emptyEquityTable ()
transferData ()