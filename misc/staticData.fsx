#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"


open System
open System.Data
open System.Linq

open SA2.Common.Table_static_data
open SA2.Common.Table_instruments
open SA2.Common.Table_index_members
open SA2.Common.DbSchema
open SA2.Common.Dates
open SA2.Common.Utils


let singleNames =

    [|
        |]

let conversionStuff =
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
        ( "ILs" , "TELBOR03 INDEX" )
    ]


let uploadSelfFundamentalTickers () =

    let names , values = singleNames 
                            |> set 
                            |> getKey_instruments 
                            |> Map.toList
                            |> List.unzip

    values |> List.map ( fun i -> i.ToString() ) |> updateExisting_static_data "BB" "fundamentalTicker" names 




let uploadFundamentalTickers () =

    let indexMembersWeightConstant = -1.0 // ACHTUNG ACHTUNG old implementation stuff

    let newNames = [ ( "AAL US EQUITY" , "AMERICAN AIRLINE" ) ; ( "GENTERA*MM EQUITY" , "COMPARTAMOS SAB" ) ] |> Map.ofList

    let name2FundTicker = [ ( "AAMRQ US EQUITY" , "AAL US EQUITY" ) ; ( "COMPARC*MM EQUITY" , "GENTERA*MM EQUITY" ) ]

    let oldIds = [ "AAMRQ US EQUITY" ; "COMPARC*MM EQUITY" ] |> set |> getKey_instruments 
    let newIds = [ "AAL US EQUITY" ; "GENTERA*MM EQUITY" ] |> set |> getKey_instruments 

    let multistrategyContext = createMultistrategyContext ()
    
    let indexIds = [ 7134 ; 13092 ] |> set |> getTicker_instruments 

    printfn "aal before"
    let aal = 
        let aalId = Map.find "AAMRQ US EQUITY" oldIds
        query {

            for row in multistrategyContext.Tbl_index_members do

            where ( aalId = row.StockId )

            select row

        }
        |> Seq.toList
        |> List.map ( fun row -> 
                        printfn "%d %d" row.IndexId row.StockId 
                        row )

    printfn "aal after"
    let newAal =
        aal 
        |> List.map ( fun row -> 
                        row.StockId <- Map.find "AAL US EQUITY" newIds
                        printfn "%d %d" row.IndexId row.StockId  
                        row
                        )
        |> List.map ( fun row -> ( row.Date |> obj2Date , Map.find row.IndexId indexIds , "AAL US EQUITY" , indexMembersWeightConstant ) )
        //|> List.iter ( fun r -> printfn "new %A" r )
        |> update_index_members Map.empty

    printfn "comparc before"
    let comparc = 
        let comparcId = Map.find "COMPARC*MM EQUITY" oldIds
        query {

            for row in multistrategyContext.Tbl_index_members do

            where ( comparcId = row.StockId )

            select row

        }
        |> Seq.toList
        |> List.map ( fun row -> 
                        printfn "%d %d" row.IndexId row.StockId 
                        row )


    printfn "comparc after"
    let newComparc =
        comparc 
        |> List.map ( fun row -> 
                        row.StockId <- Map.find "GENTERA*MM EQUITY" newIds
                        printfn "%d %d" row.IndexId row.StockId  
                        row
                        )
        |> List.map ( fun row -> ( row.Date |> obj2Date , Map.find row.IndexId indexIds , "GENTERA*MM EQUITY" , indexMembersWeightConstant ) )
        //|> List.iter ( fun r -> printfn "new %A" r )
        |> update_index_members Map.empty

    ()


let allCurrencies () =

    let multistrategyContext = createMultistrategyContext ()

    let currenciesInDb = 

        query {

            for row in multistrategyContext.Tbl_static_data do

            where ( row.FieldId = 1 )

            select row.Value

        }
        |> set

    let knownCcys = conversionStuff |> List.unzip |> fst |> set

    let unknownConversionRates = 
        currenciesInDb
            |> Set.filter ( fun ccy -> knownCcys.Contains ccy |> not )
                        
    printfn "unknown conversion rates \n%s" ( paste unknownConversionRates )


    
//execute

allCurrencies ()
