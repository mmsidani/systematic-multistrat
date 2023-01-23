#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"


#load@"../scripts/settingsDataFieldDictionary.fsx"


open System


open SA2.Common.DbSchema
open SA2.Common.Table_market_data
open SA2.Common.Table_instruments
open SA2.Common.Table_data_fields
open SA2.Common.Io
open SA2.Common.Utils


open SA2.MarketData.BloombergHistorical

let tickers2Gics =

        [

        ( "MXWO0AP INDEX" , "251010" ) ;
        ( "MXWO0AU INDEX" , "251020" ) ;
        ( "MXWO0HD INDEX" , "252010" ) ;
        ( "MXWO0LE INDEX" , "252020" ) ;
        ( "MXWO0TA INDEX" , "252030" ) ;
        ( "MXWO0DC INDEX" , "253020" ) ;
        ( "MXWO0HL INDEX" , "253010" ) ;
        ( "MXWO0ME INDEX" , "254010" ) ;
        ( "MXWO0DT INDEX" , "255010" ) ;
        ( "MXWO0NC INDEX" , "255020" ) ;
        ( "MXWO0MR INDEX" , "255030" ) ;
        ( "MXWO0SR INDEX" , "255040" ) ;
        ( "MXWO0FS INDEX" , "301010" ) ;
        ( "MXWO0BV INDEX" , "302010" ) ;
        ( "MXWO0FP INDEX" , "302020" ) ;
        ( "MXWO0TB INDEX" , "302030" ) ;
        ( "MXWO0HH INDEX" , "303010" ) ;
        ( "MXWO0PP INDEX" , "303020" ) ;
        ( "MXWO0EE INDEX" , "101010" ) ;
        ( "MXWO0OG INDEX" , "101020" ) ;
        ( "MXWO0CB INDEX" , "401010" ) ;
        ( "MXWO0TM INDEX" , "401020" ) ;
        ( "MXWO0CK INDEX" , "402030" ) ;
        ( "MXWO0CF INDEX" , "402020" ) ;
        ( "MXWO0DS INDEX" , "402010" ) ;
        ( "MXWO0IR INDEX" , "403010" ) ;
        ( "MXWO0RI INDEX" , "404020" ) ;
        ( "MXWO0RM INDEX" , "404030" ) ;
        ( "MXWO0HE INDEX" , "351010" ) ;
        ( "MXWO0HV INDEX" , "351020" ) ;
        ( "MXWO0HT INDEX" , "351030" ) ;
        ( "MXWO0BT INDEX" , "352010" ) ;
        ( "MXWO0LS INDEX" , "352030" ) ;
        ( "MXWO0PH INDEX" , "352020" ) ;
        ( "MXWO0AD INDEX" , "201010" ) ;
        ( "MXWO0BP INDEX" , "201020" ) ;
        ( "MXWO0CE INDEX" , "201030" ) ;
        ( "MXWO0EL INDEX" , "201040" ) ;
        ( "MXWO0IC INDEX" , "201050" ) ;
        ( "MXWO0MA INDEX" , "201060" ) ;
        ( "MXWO0TD INDEX" , "201070" ) ;
        ( "MXWO0CO INDEX" , "202010" ) ;
        ( "MXWO0PS INDEX" , "202020" ) ;
        ( "MXWO0AF INDEX" , "203010" ) ;
        ( "MXWO0AL INDEX" , "203020" ) ;
        ( "MXWO0MN INDEX" , "203030" ) ;
        ( "MXWO0RR INDEX" , "203040" ) ;
        ( "MXWO0TI INDEX" , "203050" ) ;
        ( "MXWO0ST INDEX" , "453010" ) ;
        ( "MXWO0IF INDEX" , "451020" ) ;
        ( "MXWO0WS INDEX" , "451010" ) ;
        ( "MXWO0SO INDEX" , "451030" ) ;
        ( "MXWO0CQ INDEX" , "452010" ) ;
        ( "MXWO0PC INDEX" , "452020" ) ;
        ( "MXWO0EI INDEX" , "452030" ) ;
        ( "MXWO0OE INDEX" , "452040" ) ;
        ( "MXWO0CH INDEX" , "151010" ) ;
        ( "MXWO0CJ INDEX" , "151020" ) ;
        ( "MXWO0CP INDEX" , "151030" ) ;
        ( "MXWO0MM INDEX" , "151040" ) ;
        ( "MXWO0PF INDEX" , "151050" ) ;
        ( "MXWO0DV INDEX" , "501010" ) ;
        ( "MXWO0WT INDEX" , "501020" ) ;
        ( "MXWO0EU INDEX" , "551010" ) ;
        ( "MXWO0GU INDEX" , "551020" ) ;
        ( "MXWO0IP INDEX" , "551050" ) ;
        ( "MXWO0MU INDEX" , "551030" ) ;
        ( "MXWO0WU INDEX" , "551040" )

        ]

        |> Map.ofList

let gics2Tickers =
    tickers2Gics
        |> Map.toList
        |> List.unzip
        ||> ( fun l r -> ( r , l ) ||> List.zip )
        |> Map.ofList




let badData () =

    readFile "//Terra/Users/Majed/devel/InOut/badData.csv"

        |> Seq.map ( fun l -> l.Split [| ',' |] )
        



let nomGdpData () = 

    readFile "//Terra/Users/Majed/devel/InOut/nomgdp.csv"

        |> Seq.map ( fun l -> l.Split [| ',' |] )





let fixData ( data : seq< string[] > ) =

    let tickersGroupedByFieldDivisor =

        data
            |> Seq.groupBy ( fun a -> a.[ 1 ] )
            |> Seq.map ( fun ( fid , s ) -> s |> Seq.map ( fun a -> ( a.[ 0 ] |> int  , a.[ 1 ] |> int , a.[ 5 ] |> float )) |> Seq.groupBy ( fun ( _ , _ , d ) -> d ) |> Seq.map ( fun ( _ , s ) -> s |> Seq.toList ) |> Seq.toList )
            |> Seq.toList


    let fid2BBField = 
        data
            |> Seq.map ( fun a -> a.[ 1 ] |> int )
            |> set
            |> getName_data_fields false

    let fid2Sa2Field = 
        data
            |> Seq.map ( fun a -> a.[ 1 ] |> int )
            |> set
            |> getName_data_fields true

    let tid2Ticker =
        data
            |> Seq.map ( fun a -> a.[ 0 ] |> int )
            |> set
            |> getTicker_instruments

    tickersGroupedByFieldDivisor

        |> List.map ( fun l  ->
                        
                        l 
                            |> List.map ( fun la ->

                                            let bbField = la  |> List.map ( fun ( _ , f , _ ) -> f ) |> set |> ( fun s -> 
                                                                                                                    if s.Count <> 1 then raise( Exception "Bloody hell")
                                                                                                                    Map.find (Seq.head s) fid2BBField )
                                            let sa2Field = la  |> List.map ( fun ( _ , f , _ ) -> f ) |> set |> ( fun s -> 
                                                                                                                    if s.Count <> 1 then raise( Exception "Bloody hell")
                                                                                                                    Map.find (Seq.head s) fid2Sa2Field )
                                            let divisor = la  |> List.map ( fun ( _ , _ , d ) -> d ) |> set |> ( fun s -> 
                                                                                                                    if s.Count <> 1 then raise( Exception "Bloody hell")
                                                                                                                    Seq.head s )
                                            let date = "20110331"
                                            let tickers = la  |> List.map ( fun ( t , _ , _ ) -> 
                                                                                let ticker = Map.find t tid2Ticker 
                                                                                if Map.containsKey ticker gics2Tickers then
                                                                                    Map.find ticker gics2Tickers
                                                                                else 
                                                                                    ticker
                                                                                ) 
                                            if sa2Field <> "GdpQoq" then
                                                    printfn "new : %s %s %f " bbField sa2Field divisor
                                                    printfn "%s" ( paste tickers )
                                                    bloombergHistoricalRequest tickers bbField "daily" true date date
        
                                                        |> Array.map ( fun ( d , t , v ) -> ( d , 
                                                                                                if Map.containsKey t tickers2Gics then
                                                                                                    Map.find t tickers2Gics
                                                                                                else
                                                                                                    t 
                                                                                                    
                                                                                               , float v ) )
                                                        |> updateExisting_market_data "BB" sa2Field divisor  
                        )
                )

    


let fixNomGdp ( data : string[] seq ) =
    let idValue =
        data
            |> Seq.map ( fun a -> ( a.[ 3 ].Replace("-","") |> int , a.[ 0 ] |> int  , a.[ 2 ] |> float ) )
            |> Seq.toArray

    let tid2Ticker =
        data
            |> Seq.map ( fun a -> a.[ 0 ] |> int )
            |> set
            |> getTicker_instruments

    idValue
        |> Array.map ( fun ( d , i , v ) -> printfn "%d %s %f" d ( Map.find i tid2Ticker ) v ; ( d , Map.find i tid2Ticker , v ))
        |> updateExisting_market_data "OECD" "GdpQoq" 1.0 
        

// execute

/// badData () |> fixData
///nomGdpData () |> fixNomGdp
