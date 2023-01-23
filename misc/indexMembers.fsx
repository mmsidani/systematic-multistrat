#r@"..\sa2-dlls\common.dll"



open SA2.Common.Io
open SA2.Common.Utils
open SA2.Common.Dates
open SA2.Common.Table_index_members
open SA2.Common.Table_instruments
open SA2.Common.Table_static_data



let indexesToLoad =

    [
        
        "NIFTY INDEX" ;
        "SET50 INDEX" ;
        "FBMKLCI INDEX" ;
        "WIG20 INDEX" ;
        "SX5E INDEX" ;
        "TOP40 INDEX" ;
        "INDU INDEX" ;
        "NKY INDEX" ;

    ]

    |> set

let indexesMembersFileName = "../data/indexMembersSAVE.csv"

let equityClassIndicator = "EQUITY"


let uploadIndexMembers () = 

    // upload index members of indexesToLoad from file
    // assumes that all members have fundamental tickers that have already been loaded to DB otherwise code crashes because of exception thrown in tbl_instruments code ( see comment below )

    let indexMembers =

        readFile indexesMembersFileName

            |> Seq.map ( fun l -> l.Split( [| ',' |] ) )
            |> Seq.filter ( fun a -> indexesToLoad.Contains a.[ 1 ] )
            |> Seq.map ( fun a -> (int a.[ 0 ] , a.[ 1 ] ,  a.[ 2 ] + " " + equityClassIndicator , float a.[ 3 ] ) )
            |> set
            |> Set.toList


    let uniqueSingleNames = indexMembers |> List.map ( fun ( _ , _ , n , _ ) -> n ) |> set

    let fundTickerData = getFundamentalTicker_static_data uniqueSingleNames 

    let uniqueFundTickers = fundTickerData |> valueSet

    let currencies = get_static_data "currency" uniqueFundTickers |> Map.find "currency"
    let gics = get_static_data "gicsIndustry" uniqueFundTickers |> Map.find "gicsIndustry"

    printfn "%d have fundTickers out of %d single names" fundTickerData.Count uniqueSingleNames.Count 
    printfn "%d have currencies and %d have gics out of %d fund tickers" currencies.Length gics.Length uniqueFundTickers.Count

    let existingIndexMembers = getUnion_index_members indexesToLoad
    printfn "the selected indexes have %d members in the DB" existingIndexMembers.Length

    indexMembers 

        |> List.map ( fun ( d , i , n , w ) -> ( d , i , Map.find n fundTickerData , w ) )
        |> (fun l -> printfn "before set %d" l.Length ; l )
        |> set
        |> Set.toList
        |> (fun l -> printfn "after set %d" l.Length ; l )
        |> update_index_members Map.empty // the map is for ticker description; harmless i think, because if we pass tickers that are unknown, we'll have an exception when update_index_members tries to update tbl_instruments




let checkIndexMembers () =

    // which indexes have components in tbl_index_members that are not fundamental tickers -- this would have resulted from a bug

    let stocks = getAll_index_members ()

    let stock2FundTicker = getFundamentalTicker_static_data stocks

    let badStocks = stock2FundTicker |> Map.filter ( fun k v -> k <> v ) |> keySet

    let indexes =[ 

        "DAX INDEX" ;
        "CAC INDEX" ;
        "UKX INDEX" ;
        "OMX INDEX" ;
        "SXXP INDEX" ;
        "TPX INDEX" ;
        "AS51 INDEX" ;
        "SIMSCI INDEX" ;
        "SPTSX60 INDEX" ;
        "HSI INDEX" ;
        "AEX INDEX" ;
        "IBEX INDEX" ;
        "FTSEMIB INDEX" ;
        "OBXP INDEX" ;
        "SMI INDEX" ;
        "TAMSCI INDEX";
        "NIFTY INDEX" ;
        "SET50 INDEX" ;
        "FBMKLCI INDEX" ;
        "WIG20 INDEX" ;
        "SX5E INDEX" ;
        "TOP40 INDEX" ;
        "INDU INDEX" ;
        "NKY INDEX" ;
        "SPX INDEX" ; 
        "NDX INDEX" ;
        "HSCEI INDEX" ;
        "RTSI$ INDEX" ; 
        "XIN9I INDEX"; 
        "MEXBOL INDEX"; 
        "IBOV INDEX" ; 
        "XU030 INDEX";

        ]

    for index in indexes do
        let iStocks = [ index ] |> set |> getUnion_index_members |> set
        if [ iStocks ; badStocks ] |> Set.intersectMany |> Set.count <> 0  then
             printfn "bad index %s \n %s " index ( [ iStocks ; badStocks ] |> Set.intersectMany |> paste )



let getIndexMembers date indexes =

    for index in indexes do
        printfn "%s :\n%s" index  ( date  |> date2Obj |> get_index_members ( set [ index] ) |> Map.find index |> set |> getDescription_instruments |> paste )


// execute

//uploadIndexMembers ()
//checkIndexMembers ()
getIndexMembers 20020731 [ "SPX INDEX" ]