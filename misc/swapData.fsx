#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"


open System
open System.Linq


open SA2.Common.DbSchema
open SA2.Common.Table_market_data
open SA2.Common.Table_static_data
open SA2.Common.Table_instruments
open SA2.Common.Io
open SA2.Common.Utils
open SA2.MarketData.BloombergHistorical
open SA2.MarketData.BloombergReference

    

let tickers =

    readFile "//TERRA/Users/Majed/devel/InOut/Rate-Names-Bloomberg.csv"
        |> Seq.toList
        |> set
        |> Set.toList


let modifyShortNames insts =

    let descriptions = insts |> set |> getDescription_instruments 

    descriptions
        |> Map.map ( fun _ d -> "SWAP " + d )
        |> update_instruments



let uploadNewCcys () =

    let ccys = [ "BGNUSD CURNCY" ; "PHPUSD CURNCY" ; "PKRUSD CURNCY" ; "RONUSD CURNCY" ; "VNDUSD CURNCY" ]

    bloombergHistoricalRequest ccys "PX_LAST" "daily" false "19800101" "20140318"
        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> updateExisting_market_data "BB" DatabaseFields.price 1.0



let createStaticDataTableForSwaps () =
    
    let sw = new IO.StreamWriter( "//TERRA/Users/Majed/devel/InOut/swapsTable.csv")

    sw.WriteLine ( "ticker,description,currency,base_currency,day_count,tenor,days_to_settle")

    let tickers =

        readFile "//TERRA/Users/Majed/devel/InOut/Rate-Names-Bloomberg.csv"
            |> Seq.toList
            |> set

    let shortNames =
        getDescription_instruments tickers

    let currencies =
        get_static_data DatabaseFields.currency tickers |> Map.find DatabaseFields.currency |> Map.ofList

    let baseCurrencies =
        get_static_data DatabaseFields.baseCurrency tickers |> Map.find DatabaseFields.baseCurrency |> Map.ofList

    let dayCounts =
        get_static_data DatabaseFields.dayCount tickers |> Map.find DatabaseFields.dayCount |> Map.ofList

    let tenors =
        get_static_data DatabaseFields.tenor tickers |> Map.find DatabaseFields.tenor |> Map.ofList

    let daysSettle =
        get_static_data DatabaseFields.daysToSettle tickers |> Map.find DatabaseFields.daysToSettle |> Map.ofList

    tickers
        |> Set.iter ( fun t ->
                        sw.Write( t )

                        if shortNames.ContainsKey t then
                            sw.Write( "," + Map.find t shortNames )
                        else
                            sw.Write( "," + "NA" )

                        if currencies.ContainsKey t then
                            sw.Write( "," + Map.find t currencies )
                        else
                            sw.Write( "," + "NA" )

                        if baseCurrencies.ContainsKey t then
                            sw.Write( "," + Map.find t baseCurrencies )
                        else
                            sw.Write( "," + "NA" )

                        if dayCounts.ContainsKey t then
                            sw.Write( "," + Map.find t dayCounts )
                        else
                            sw.Write( "," + "NA" )

                        if tenors.ContainsKey t then
                            sw.Write( "," + Map.find t tenors )
                        else
                            sw.Write( "," + "NA" )

                        if daysSettle.ContainsKey t then
                            sw.Write( "," + Map.find t daysSettle )
                        else
                            sw.Write( "," + "NA" )

                        sw.WriteLine()
        )

    sw.Close () 


let deleteFirstLoad ( tickers : string list ) =

    let multistrategyContext = createMultistrategyContext()
    let ticker2Id = tickers |> set |> getKey_instruments 
    let ids = ticker2Id |> valueSet |> Set.toList

    tickers
     |> List.iter ( fun t -> if ticker2Id.ContainsKey t |> not then printfn "%s not in instruments" t )


    let swapRows =
        query
            {
                for row in multistrategyContext.Tbl_market_data do
            
                where ( ids.Contains row.InstrumentId )

                select row
            }
    printfn "%d rows" ( Seq.length swapRows )

    swapRows
        |> Seq.map ( fun row -> row.InstrumentId )
        |> set
        |> ( fun s -> printfn "%d unique ids" s.Count ; 
                      tickers
                        |> List.iter ( fun t -> if s.Contains (Map.find t ticker2Id) |> not then printfn "%s not in static" t ) )

    multistrategyContext.Tbl_market_data.DeleteAllOnSubmit( swapRows )

    try

        multistrategyContext.SubmitChanges ()

    with

    | e -> raise ( Exception "failed...")




let newCurrencies () =
    
    let multistrategyContext = createMultistrategyContext ()

    let ccys =

        query

            {
               for row in multistrategyContext.Tbl_static_data do

               where ( row.FieldId = 1 )

               select row.Value
            }

            |> set

    let ccyTickers =  ccys |> Set.map ( fun c -> c + "USD CURNCY" )

    let ccyIds = getKey_instruments ccyTickers

    Set.iter ( fun c -> if ccyIds.ContainsKey c |> not then printfn "%s" c )




    
let specialSwaps insts =
    
    printfn "short names"

    bloombergReferenceRequest insts "SHORT_NAME" 20140318 
        |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
        |> Map.ofArray
        |> update_instruments
        |> ignore


    printfn "currency"

    bloombergReferenceRequest insts "CRNCY" 20140318
        |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
        |> Array.toList
        |> List.unzip
        ||> updateExisting_static_data "BB" DatabaseFields.currency

    printfn "baseCurrency"

    bloombergReferenceRequest insts "BASE_CRNCY" 20140318
        |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
        |> Array.toList
        |> List.unzip
        ||> updateExisting_static_data "BB" DatabaseFields.baseCurrency


    printfn "DAY_CNT_DES"

    bloombergReferenceRequest insts "DAY_CNT_DES" 20140318
        |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
        |> Array.toList
        |> List.unzip
        ||> updateExisting_static_data "BB" DatabaseFields.dayCount

    printfn "TENOR"

    bloombergReferenceRequest insts "SECURITY_TENOR_TWO" 20140318
        |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
        |> Array.toList
        |> List.unzip
        ||> updateExisting_static_data "BB" DatabaseFields.tenor

    printfn "DAYS_TO_MTY_TDY"

    let daysToMtyTdy = 
        bloombergReferenceRequest insts "DAYS_TO_MTY_TDY" 20140318
            |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
            |> Map.ofArray

    printfn "DAYS_TO_MTY"

    let daysToMty=
        bloombergReferenceRequest insts "DAYS_TO_MTY" 20140318
            |> Array.map ( fun ( _ , i , n ) -> ( i , n ) )
            |> Map.ofArray
    
    let commonNames = [ daysToMtyTdy |> keySet ; daysToMty |> keySet ] |> Set.intersectMany

    Map.fold 
        ( fun s k v ->
                if Set.contains k commonNames then
                    Map.add k ( ( (Map.find k daysToMtyTdy |> int ) -  ( int v ) ).ToString() ) s
                else
                    s
        ) Map.empty daysToMty
    |> Map.toList
    |> List.unzip
    ||> updateExisting_static_data "BB" DatabaseFields.daysToSettle
    



let marketData () =

    let insts = 
        readFile "//TERRA/Users/Majed/devel/InOut/cleanSA2-RE-Bloomberg-swap-mapping.csv"
            |> Seq.toArray
            |> ( fun a -> a.[ 1 .. ] ) // skip header
            |> Array.map ( fun l -> l.Split [| ',' |] )
            |> Array.map ( fun a -> a.[ 1 ])
            |> Array.toList

    printfn "bid"

    bloombergHistoricalRequest insts "PX_BID" "daily" false "19800101" "20140318"
        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> updateExisting_market_data "BB" DatabaseFields.bid 100.0

    printfn "ask"

    bloombergHistoricalRequest insts "PX_ASK" "daily" false "19800101" "20140318"
        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> updateExisting_market_data "BB" DatabaseFields.ask 100.0

    printfn "mid"

    bloombergHistoricalRequest insts "PX_MID" "daily" false "19800101" "20140318"
        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> updateExisting_market_data "BB" DatabaseFields.mid 100.0

    let indexes = 
        readFile "//TERRA/Users/Majed/devel/InOut/cleanSA2-RE-Bloomberg-fixing-mapping.csv"
            |> Seq.toArray
            |> ( fun a -> a.[ 1 .. ] ) // skip header
            |> Array.map ( fun l -> l.Split [| ',' |] )
            |> Array.map ( fun a -> a.[ 1 ])
            |> Array.toList

    printfn "price"

    bloombergHistoricalRequest indexes "PX_Last" "daily" false "19800101" "20140318"
        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> updateExisting_market_data "BB" DatabaseFields.price 100.0




let loadNewTickers () =

    let newTicker2Desc =

        readFile "//TERRA/Users/Majed/devel/InOut/swaptable.csv"
            |> Seq.map ( fun l -> l.Split [| ',' |] )
            |> Seq.toArray
            |> ( fun aa -> aa.[ 1 .. ] ) // skip header
            |> Array.map ( fun a -> ( a.[0], a.[1]) )
            |> Map.ofArray

    update_instruments newTicker2Desc |> ignore

    newTicker2Desc |> keySet |> Set.toList |> specialSwaps



let loadSwapContracts () =

    let tickers2Symbol , tickers2Descr =

        readFile "//TERRA/Users/Majed/devel/InOut/cleanSA2-RE-Bloomberg-swap-mapping.csv"

            |> Seq.map ( fun l -> l.Split [| ',' |] )
            |> Seq.toArray
            |> ( fun a -> a.[ 1 .. ] ) // skip header
            |> Array.map ( fun a -> ( ( a.[ 1 ] , a.[ 0 ] ) , ( a.[ 1 ] , a.[ 2 ] ) ) )
            |> Array.unzip
            ||> ( fun l r -> Map.ofArray l , Map.ofArray r )

    updateWithSa2Symbol_instruments tickers2Descr tickers2Symbol |> ignore



let loadStaticData source col sa2Field =

    let lines =

//        readFile "//TERRA/Users/Majed/devel/InOut/cleanSA2-RE-Bloomberg-fixing-mapping.csv"
        readFile "//TERRA/Users/Majed/devel/InOut/correctedSwaps.csv"
            |> Seq.map ( fun l -> l.Split [| ',' |] )
            |> Seq.toArray

    let header = lines.[ 0 ]
    let data = lines.[ 1 .. ]
    
    let tickers = data |> Array.map ( fun a -> a.[ 1 ] ) |> Array.toList



    let colData = data |> Array.map ( fun a -> ( a.[ 1 ] , a.[ col ]  ) ) /// .ToUpper()
                       |> Array.toList
                       |> List.map ( fun ( t , v ) -> ( t , if v = "Semi-annual" then "Semiannual" else v ) )
//                       |> List.map ( fun ( t , v ) -> ( t , v.Replace( "D" , "" )))
                       |> List.filter ( fun ( _ , v ) -> v <> "NA" )
                       |> Map.ofList

//    let floatIndexIds = colData |> valueSet |> getKey_instruments 
//    let tickers2FloatIndex =
//        colData
//            |> Map.map ( fun _ i -> (Map.find i floatIndexIds ).ToString() )

//    tickers2FloatIndex
    colData
        |> Map.toList
        |> List.unzip
        ||> updateExisting_static_data source sa2Field /// tickers colData



// execute

//newCurrencies ()
//specialSwaps tickers
//printfn "%d %d" tickers.Length ( tickers |> set |> Set.count )
//modifyShortNames tickers
//loadNewTickers ()
//loadStaticData "BB" 4 DatabaseFields.baseCurrency
//marketData ()