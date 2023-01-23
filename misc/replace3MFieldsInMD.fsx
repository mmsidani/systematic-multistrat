#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"

open System
open System.Data
open System.Linq
open SA2.Common.Table_market_data
open SA2.Common.Table_instruments
open SA2.Common.Table_data_fields
open SA2.Common.Utils
open SA2.Common.Io
open SA2.Common.Dates
open SA2.Common.DbSchema


// what's this? the PX_LAST for these rates was changed to map to yield instead of price. I wrote this to change existing data to have the yield field. so first function gets all instances of fieldId=12 ("price") and second function modifies to fieldId=21 (yield)


let conversionRate = 

    [ 

    ( "USD" , "USGG3M INDEX" ) ;
    ( "GBP" , "UKGTB3M INDEX" ) ;
    ( "GBp" , "UKGTB3M INDEX" ) ;
    ( "JPY" , "GJTB3MO INDEX" ) ;
    ( "CHF" , "SF0003M INDEX" ) ;
    ( "CAD" , "GCAN3M INDEX" ) ;
    ( "EUR" , "GETB1 INDEX" ) ;
    ( "SEK" , "GSGT3M INDEX" ) ;
    ( "AUD" , "GACGB1 INDEX" ) ;
    ( "HKD" , "GHKTB3M INDEX" ) ;
    ( "SGD" , "MASB3M INDEX" ) ;
    ( "NOK" , "GNGT3M INDEX" ) ;
    ( "NZD" , "NDBB3M INDEX" ) ;
    ( "ZAR" , "JIBA3M INDEX" ) ;
    ( "ZAr" , "JIBA3M INDEX" ) ;
    ( "KRW" , "GVSK3MON INDEX" ) ;
    ( "MXN" , "MPTBC INDEX" ) ;
    ( "BRL" , "BZAD3M INDEX" ) ;
    ( "PLN" , "WIBO3M INDEX" ) ;
    ( "RUB" , "MICXRU3M INDEX" ) ;
    ( "THB" , "TBDC3M INDEX" ) ;
    ( "MYR" , "BNNN3M INDEX" ) ;
    ( "INR" , "GINTB3MO INDEX" ) ;
    ( "TWD" , "NTRPC INDEX" ) ;
    ( "CNY" , "CNBI3MO INDEX" ) ;
    ( "IDR" , "GIDN1YR INDEX" ) ;
    ( "CZK" , "PRIO3M INDEX" ) ;
    ( "DKK" , "GDGT3M INDEX" ) ;
    ( "ISK" , "SEDL3MDE INDEX" ) ;
    ( "KZT" , "KZDR30D INDEX" ) ;
    ( "HUF" , "GHTB3M INDEX" ) ;
    ( "TRY" , "TRLIB3M INDEX" ) ;
    ( "ILs" , "TELBOR03 INDEX" ) ;                        
    ( "ILS" , "TELBOR03 INDEX" ) ;
    ( "AED" , "EIBO3M INDEX" ) ;
    ( "QAR" , "QRDRC CURNCY" ) ;
    ( "BHD" , "BHIBOR3M INDEX" ) ;                        
    ( "CLP" , "CHDRCU CURNCY" ) ;
    ( "COP" , "COMM90D INDEX" ) ;
    ( "KWd" , "KIBOR3M INDEX" ) ;
    ( "KWD" , "KIBOR3M INDEX" ) ;
    ( "OMR" , "ORDRC INDEX" ) ;
    ( "SAR" , "SAIB3M INDEX" ) ;

    ]



let getAndSavePriceData ( fileName : string )  =

    let swLog = new IO.StreamWriter( fileName )

    //let priceFieldId = 12
    let yieldFieldId = 21

    let names = conversionRate |> List.unzip |> snd |> set

    let multistrategyContext = createMultistrategyContext ()

    let nameIds = getKey_instruments names |> valueSet

    let tData =

        [

        for id in nameIds -> 

            query {

                for row in multistrategyContext.Tbl_market_data do

                where ( row.FieldId = yieldFieldId && id = row.InstrumentId )

                select row

            }

            |> Seq.toList
        ]

        |> List.concat

    let output =

        tData

            |> List.map ( fun row -> row.InstrumentId.ToString() + "," + row.FieldId.ToString() + "," + row.SourceId.ToString() + "," + row.Value.ToString() + "," + (row.Date |>obj2Date).ToString() + "," + row.Divisor.ToString() )
            |> List.iter ( fun l -> swLog.WriteLine( l ) )

    swLog.Close()

    try

        multistrategyContext.Tbl_market_data.DeleteAllOnSubmit tData

        multistrategyContext.SubmitChanges ()

    with

    | e -> raise e

    tData




let readSavedDataFromFile fileName =

        readFile fileName

            |> Seq.toList
            |> List.map ( fun l -> l.Split ( [| ',' |] ) )
            |> List.map ( fun a -> ( ( int a.[ 0 ] , int a.[ 4 ] ) , a ) )
            |> Map.ofList
            |> Map.toList
            |> List.unzip
            |> snd
            |> List.map ( fun a -> new MultistrategySchema.Tbl_market_data (InstrumentId = int a.[ 0 ] , FieldId = int a.[ 1 ] , SourceId = int a.[ 2 ] , Value = Nullable<float>( float a.[3]) , Date = date2Obj (int a.[ 4 ] ) , Divisor = float a.[ 5 ] ))



let modifyAndUpload ( data : MultistrategySchema.Tbl_market_data list ) =

    let yieldFieldId = 21

    try

        // new context needed

        let multistrategyContext1 = createMultistrategyContext ()

        let newRecords = data |> List.map ( fun row -> new MultistrategySchema.Tbl_market_data (InstrumentId = row.InstrumentId , FieldId = yieldFieldId , SourceId = row.SourceId , Value = row.Value , Date = row.Date , Divisor = row.Divisor ) )

        multistrategyContext1.Tbl_market_data.InsertAllOnSubmit newRecords

        multistrategyContext1.SubmitChanges ()

    with

    | e -> raise e




// execute

////getAndSavePriceData  "../output/3mpriceData.csv" 
( readSavedDataFromFile >> modifyAndUpload ) "../output/3mpriceDataCombined.csv"
