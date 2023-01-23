#r@"..\sa2-dlls\common.dll"



open System


open SA2.Common.Io
open SA2.Common.Table_market_data
open SA2.Common.Table_rate_data
open SA2.Common.DbSchema


let tickers = 

    [

       

    ]

//    |> List.map ( fun ( t , _ ) -> t )



let findMinMaxDates fileName =

    printfn "%s" fileName

    readFile fileName

        |> Seq.map ( fun l -> l.Split ',' )
        |> Seq.map ( fun a -> ( a.[ 0 ] , a.[ 1 ] , a.[ 2 ] ) )
        |> Seq.groupBy ( fun ( _ , t , _ ) -> t )
        |> Seq.iter ( fun ( t , s ) -> 
                        s 
                        |> Seq.map ( fun ( d , _ , _ ) -> int d ) 
                        |> Seq.fold ( fun ( smn , smx ) d  ->  ( min smn d , max smx d ) ) ( 100000000 , 0 ) // infinity is float not int
                        |> ( fun sq -> printfn "%s %d %d" t ( fst sq ) ( snd sq )  ) )




let findMinMaxDatesInDB tickers sa2Field =

    let data = get_market_data sa2Field tickers |> Map.find sa2Field

    data

        |> Map.iter ( fun k l -> 
                        l 
                        |> List.sortBy ( fun ( d , _ ) -> d ) 
                        |> List.map ( fun ( d , _ ) -> d )
                        |> ( fun l -> printfn "%s , %d , %d " k l.[ 0 ] l.[ l.Length - 1 ] )

                        )




let findMinMaxDatesInDBFromTickersFile fileName sa2Field dbTable =



    // fileName is a string with the FULL path to the file containing the tickers, one ticker per line
    // dbTable is "tbl_rate_data" or "tbl_market_data"
    // sa2Field is the name of the field in sa2_multistrategy otherwise can be set to "" for tbl_rate_data



    let tickers = readFile fileName |> Seq.toList

    if dbTable = "tbl_rate_data" then
        get_rate_data tickers
    elif dbTable = "tbl_market_data" then
        get_market_data sa2Field tickers |> Map.find sa2Field
        
    else
        
        raise ( Exception ( "unkown db table " + dbTable ) )

    |> Map.iter ( fun k l -> 
                    l 
                        |> List.sortBy ( fun ( d , _ ) -> d ) 
                        |> List.map ( fun ( d , _ ) -> d )
                        |> ( fun l -> printfn "%s , %d , %d " k l.[ 0 ] l.[ l.Length - 1 ] )

                    )


// execute

//findMinMaxDates "//Terra/Users/Majed/devel/InOut/operatingIncome.csv"
//findMinMaxDatesInDB tickers DatabaseFields.ebit
//printfn "\n\n"
//findMinMaxDatesInDB tickers DatabaseFields.enterpriseValue
//printfn "\n\n"
//findMinMaxDatesInDB tickers DatabaseFields.returnOnCapital 

findMinMaxDatesInDBFromTickersFile "//Terra/Users/Majed/devel/InOut/foo.csv" "" "tbl_rate_data"
