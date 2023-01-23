#r@"..\sa2-dlls\common.dll"


open SA2.Common.Table_instruments
open SA2.Common.Table_static_data
open SA2.Common.Utils
open SA2.Common.Io
        
              
let lookForNoDescription names =

    try

        names |> set |> getDescription_instruments |> ignore

        Seq.empty // if any name was not known already, an exception would have been thrown

    with

        | KeysNotFound ( newNames ) -> newNames |> Set.toSeq




let partitionForNoFundTicker names =

    try

        let fundTickers = getFundamentalTicker_static_data names

        (

        names

            |> Seq.filter ( fun n -> fundTickers.ContainsKey n |> not ) // return will have both notKnown and known without fundamental tickers

        , fundTickers
        
        )

    with

        | e -> raise( e )




let doIt indexMembersFile indexFundTickersFile indexShortNameFile =

    let indexMembers = 
        readFile indexMembersFile
            |> Seq.toArray
            |> Array.map ( fun l -> l.Split( [| ',' |] ) )
            |> Array.map ( fun a -> ( a.[ 0 ] , a.[ 1 ] , a.[ 2 ] + " EQUITY" , float a.[ 3 ] ) )

    let stocks = indexMembers |> Array.map ( fun ( _ , _ , n , _ ) -> n  ) |> Array.toList
    let indexes = indexMembers |> Array.map ( fun ( _ , i , _  , _ ) -> i  ) |> Array.toList
    
    let unknownStocks = lookForNoDescription stocks |> set
    let knownStocks = stocks |> List.filter ( fun s -> Set.contains s unknownStocks |> not ) |> set

    let unknownIndexes = lookForNoDescription indexes |> set

    let namesWithNoFundTicker , names2FundTicker = partitionForNoFundTicker knownStocks

    let fundTickersValues =
        readFile indexFundTickersFile
            |> Seq.toArray
            |> Array.map ( fun l -> l.Split( [| ',' |] ) )
            |> Array.map ( fun a -> a.[ 2 ] ) 

    let unknownFundTickers = lookForNoDescription fundTickersValues |> set

    let ticker2ShortName =
        readFile indexShortNameFile
            |> Seq.toArray
            |> Array.map ( fun l -> l.Split( [| ',' |] ) )
            |> Array.map ( fun a -> ( a.[ 1 ] , a.[ 2 ] )  )
            |> Map.ofArray

    for name in [ unknownStocks ; unknownIndexes ; unknownFundTickers ] |> Set.unionMany do
        
        if Map.containsKey name ticker2ShortName |> not then
            printfn "no shortName %s" name




// execute

doIt "../output/indexMembers.txt" "/../output/fundTicker.txt" "../output/shortName.txt"
