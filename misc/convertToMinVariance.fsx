#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"
#r@"..\sa2-dlls\MathNet.Numerics.dll"



#load "../scripts/settingsFxConversion.fsx"


open System
open SA2.Common.Io
open SA2.Common.Table_market_data
open SA2.Common.Table_rate_data
open SA2.Common.DbSchema
open SA2.Common.Returns
open SA2.Common.Dates
open SA2.Common.KMeans
open SA2.Common.Math
open SA2.Common.Utils

open SA2.Equity.RiskModels

let index2ConversionRate = 

    [ 

        ( "RTY INDEX" ,"USGG3M INDEX" ) ;
        ( "NDX INDEX" ,"USGG3M INDEX" ) ;
        ( "SPX INDEX" ,"USGG3M INDEX" ) ;
        ( "INDU INDEX" ,"USGG3M INDEX" ) ;
        ( "UKX INDEX" , "UKGTB3M INDEX" ) ;
        ( "NKY INDEX" , "GJTB3MO INDEX" ) ;
        ( "TPX INDEX" , "GJTB3MO INDEX" ) ;
        ( "SMI INDEX" , "SF0003M INDEX" ) ;
        ( "SPTSX60 INDEX" , "GCAN3M INDEX" ) ;
        ( "SX5E INDEX" , "GETB1 INDEX" ) ;
        ( "FTSEMIB INDEX" , "GETB1 INDEX" ) ;
        ( "IBEX INDEX" , "GETB1 INDEX" ) ;
        ( "AEX INDEX"  , "GETB1 INDEX" ) ;
        ( "SXXP INDEX"  , "GETB1 INDEX" ) ;
        ( "DAX INDEX"  , "GETB1 INDEX" ) ;
        ( "CAC INDEX" , "GETB1 INDEX" ) ;
        ( "OMX INDEX" , "GSGT3M INDEX" ) ;
        ( "AS51 INDEX" , "GACGB1 INDEX" ) ;
        ( "HSCEI INDEX" , "GHKTB3M INDEX" ) ;
        ( "HSI INDEX" , "GHKTB3M INDEX" ) ;
        ( "SIMSCI INDEX" , "MASB3M INDEX" ) ;
        ( "OBXP INDEX" , "GNGT3M INDEX" ) ;
        ( "TOP40 INDEX" , "JIBA3M INDEX" ) ;
        ( "KOSPI2 INDEX" , "GVSK3MON INDEX" ) ;
        ( "MEXBOL INDEX" , "MPTBC INDEX" ) ;
        ( "IBOV INDEX" , "BZAD3M INDEX" ) ;
        ( "WIG20 INDEX" , "WIBO3M INDEX" ) ;
        ( "RTSI$ INDEX" , "MICXRU3M INDEX" ) ;
        ( "SET50 INDEX" , "TBDC3M INDEX" ) ;
        ( "FBMKLCI INDEX" , "BNNN3M INDEX" ) ;
        ( "NIFTY INDEX" , "GINTB3MO INDEX" ) ;
        ( "TAMSCI INDEX" , "NTRPC INDEX" ) ;
        ( "SHSN300 INDEX" , "CNBI3MO INDEX" ) ;
        ( "XIN9I INDEX" , "CNBI3MO INDEX" ) ;
        ( "XU030 INDEX" , "TRLIB3M INDEX" ) ;

    ]

    |> Map.ofList


let index2CurrencyDtr =

    let rate2Ccy = SettingsFxConversion.fxConversionRates.conversionRate |> List.unzip |> ( fun ( l , r ) -> ( r , l ) ) ||> List.zip |> Map.ofList |> Map.map ( fun _ c -> c.ToLower() + ".usd.fx.b.dtr" )

    index2ConversionRate

        |> Map.map ( fun i r -> Map.find r rate2Ccy )




let writePortfolioFiles ( fileName : string ) portfolios =

    let names , weightsLongsShortsHist = portfolios

    let sw = new IO.StreamWriter( fileName )
    let swLong = new IO.StreamWriter( fileName + ".long.csv" )
    let swShort = new IO.StreamWriter( fileName + ".short.csv" )

    let header = Array.fold ( fun t n -> t + "," + n) "Date" names
    sw.WriteLine( header )
    swLong.WriteLine( header )
    swShort.WriteLine( header )

    for i in 0 .. Array.length weightsLongsShortsHist - 1 do
        let date , weights , longs , shorts = weightsLongsShortsHist.[ i ]
        let line = 
            Array.fold ( fun t n -> 
                            let weight = 
                                if Map.containsKey n weights then
                                    Map.find n weights
                                else
                                    0.0
                            t + "," + weight.ToString() ) ( date.ToString() ) names

        sw.WriteLine( line )

        let lineLong =
            let numLongs = Set.count longs |> float
            Array.fold ( fun t n -> 
                            let weight =
                                if Set.contains n longs then
                                    1.0 / numLongs
                                else
                                    0.0
                            t + "," + weight.ToString() ) ( date.ToString() ) names
        
        swLong.WriteLine( lineLong )
                                
        let lineShort =
            let numShorts = Set.count shorts |> float
            Array.fold ( fun t n -> 
                            let weight =
                                if Set.contains n shorts then
                                    1.0 / numShorts
                                else
                                    0.0
                            t + "," + weight.ToString() ) ( date.ToString() ) names

        swShort.WriteLine( lineShort )

    sw.Close () 
    swLong.Close ()
    swShort.Close ()



    
let convertPortfolio numDays upperLongsBound lowerShortsBound fileName =

    let lines = readFile fileName |> Seq.map ( fun l -> l.Split ',' ) |> Seq.toArray

    let header = lines.[ 0 ]
    let names = header.[ 1 .. ]

    let marketData = 
        names
            |> Array.toList
            |> get_market_data DatabaseFields.price  
            |> Map.find DatabaseFields.price
            |> Map.map ( fun _ l -> List.toArray l )

    let ratesData =
        index2CurrencyDtr
            |> valueSet
            |> Set.toList
            |> List.filter ( fun c -> c <> "usd.usd.fx.b.dtr" )
            |> get_rate_data // from the old db
            |> Map.map ( fun _ l -> l |> List.toArray )
            |> ( fun m ->
                        let dateZero = Map.find "SPX INDEX" marketData |> Array.map ( fun ( d , _ ) -> ( d , 0.0 ) ) 
                        Map.add "usd.usd.fx.b.dtr" dateZero  m ) // why? just so that we don't have to treat SPX and INDU differently below. there's no usd.usd.fx.b.dtr in the DB


    let rets = marketData |> Map.map ( fun _ l -> calculateReturns l |> List.toArray )

    ( names ,

        [|
        
        for i in 1 .. lines.Length - 1 ->

            let line = lines.[ i ]

            let date = int line.[ 0 ] // dates start in row 1

            let weights = line.[ 1 .. ] |> Array.map ( fun w -> float w )

            let dRateRets = 
                ratesData  
                    |> Map.map ( fun i a -> a |> Array.filter ( fun ( d , _ ) -> d <= date ) )
                    |> Map.filter ( fun _ a -> a.Length >= numDays )
                    |> Map.map ( fun _ a -> a |> Array.sortBy ( fun ( d , _ ) -> -d ) )
                    |> Map.map ( fun _ a -> a |> Array.map ( fun ( _ , v ) -> v ) |> ( fun a -> a.[ 0 .. (numDays-1) ] ) )

            let dRets = 
                rets 
                    |> Map.map ( fun i a -> a |> Array.filter ( fun ( d , _ ) -> d <= date ) )
                    |> Map.filter ( fun n a -> a.Length >= numDays && Map.containsKey ( Map.find n index2CurrencyDtr ) dRateRets )
                    |> Map.map ( fun _ a -> a |> Array.sortBy ( fun ( d , _ ) -> -d ) )
                    |> Map.map ( fun _ a -> a |> Array.map ( fun ( _ , v ) -> v ) |> ( fun a -> a.[ 0 .. (numDays-1) ] ) )
                    |> Map.map ( fun n a -> 
                                    let aRate = Map.find ( Map.find n index2CurrencyDtr ) dRateRets
                                    a |> Array.mapi ( fun i v -> a.[ i ] + aRate.[ i ]  ) // excess returns
                    )

            let longs = 
                names 
                    |> Array.filter ( fun n -> Map.containsKey n dRets )
                    |> Array.mapi ( fun i n -> ( i , n ) ) 
                    |> Array.filter ( fun ( i , _ ) -> weights.[ i ] > 0.0 ) 
                    |> Array.map ( fun ( _ , n ) -> n )

            let shorts = 
                names  
                    |> Array.filter ( fun n -> Map.containsKey n dRets )
                    |> Array.mapi ( fun i n -> ( i , n ) ) 
                    |> Array.filter ( fun ( i , _ ) -> weights.[ i ] < 0.0 ) 
                    |> Array.map ( fun ( _ , n ) -> n )

            let weights , vol = quadProg true longs shorts [| upperLongsBound |] [| lowerShortsBound |] dRets
//            let weights = inverseVol ( longs , shorts ) dRets 
            ( date , weights , longs |> set , shorts |> set )

        |]
    )


// execute

let minVarPortfolios = convertPortfolio 60 1.0 -1.0 "//Terra/Users/Majed/devel/staging/output/developedPortfolio.csv"
writePortfolioFiles "//Terra/Users/Majed/devel/InOut/minVar.csv" minVarPortfolios