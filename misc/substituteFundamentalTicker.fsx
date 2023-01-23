#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"


open SA2.Common.DbSchema
open SA2.Common.Table_index_members
open SA2.Common.Table_static_data
open SA2.Common.Table_market_data
open SA2.Common.Table_instruments
open SA2.Common.Dates
open SA2.Common.Utils


let addMissingFundamentalTickers () =

    // 20131213: some fundamental tickers are not known in static_data to be their own fundamental tickers

    let allFundamentalTickers =
        
        let multistrategyContext = createMultistrategyContext () 

        query
            {
                for row in multistrategyContext.Tbl_static_data do

                where ( row.FieldId = 22 )

                select row.Value
            }

            |> Seq.map ( fun s -> try Some(int s ) with | _ -> None )
            |> Seq.filter ( fun s -> s.IsSome)
            |> Seq.map ( fun s -> s.Value )
            |> set
            |> getTicker_instruments
            |> valueSet
    printfn "%A" allFundamentalTickers
    let fundTickersWithFundTickers = allFundamentalTickers  |> getFundamentalTicker_static_data |> keySet
    let missingFundTickers = allFundamentalTickers |> Set.filter ( fun t -> fundTickersWithFundTickers.Contains t |> not ) |> Set.toList

    let toLoadTickers , toLoadVals = missingFundTickers |> set |> getKey_instruments  |> Map.toList |> List.unzip 
    update_static_data "BB" "EQY_FUND_TICKER" "fundamentalTicker" Map.empty toLoadTickers  ( toLoadVals |> List.map ( fun i -> i.ToString() )) // fund tickers are their own fund tickers

    //printfn "missing fund tickers %d" missingFundTickers.Length




let doTableIndexMembers indexesOfInterest =
        
    let multistrategyContext = createMultistrategyContext () 

    let indexesOfInterestIds = getKey_instruments indexesOfInterest |> valueSet

    let indexMembers = 
        SA2.Common.Io.readFile "F:\\SQLBackups\\table_index_members_20131213.csv"
            |> Seq.map ( fun l -> l.Split( [| ',' |] ) )
            |> Seq.map ( fun a -> ( int a.[ 0 ] , int a.[ 1 ] , float a.[ 2 ] ,  a.[ 3 ].Split( [| ' ' |] ).[ 0 ].Replace( "-" , "" ) |> int ) )
            |> Seq.filter ( fun ( i , _ , _ , _ ) -> indexesOfInterestIds.Contains i )
            |> Seq.toList

    let stockIdsInFile = indexMembers |> Seq.map ( fun ( _ , i , _ , _ ) -> i ) |> Set.ofSeq
    let stockNamesInFile = getTicker_instruments stockIdsInFile

    let stock2FundTicker = getFundamentalTicker_static_data ( stockNamesInFile |> valueSet ) 
    let stock2Id = stock2FundTicker |> keySet |> getKey_instruments
    let fund2Id =  stock2FundTicker |> valueSet |> getKey_instruments
    let stockId2FundId = stock2FundTicker |> Map.toList |> List.map ( fun ( s , f ) -> ( Map.find s stock2Id , Map.find f fund2Id ) ) |> Map.ofList


    printfn "indexMembers : %d" indexMembers.Length

    let missing = ref Set.empty

    let remappedIndexMembers =
        indexMembers 
            |> List.map ( fun ( i , u , _ , d ) -> ( i , u , d ) )
            |> List.map ( fun ( i , u , d ) -> ( i , Map.find u stockId2FundId , d ) )
            |> set

    printfn "missing \n %s" ( getTicker_instruments !missing |> paste )
        
    let newRecords = 
        [
        for ( i , u , d ) in remappedIndexMembers ->
            new MultistrategySchema.Tbl_index_members( IndexId = i, StockId = u , Weight = indexMembersWeightConstant , Date = date2Obj d ) 
        ]

    multistrategyContext.Tbl_index_members.InsertAllOnSubmit( newRecords )

    try
        
        multistrategyContext.SubmitChanges()

    with

    | e -> raise( e )




let doMarketData indexes fields (outputFile : string) = 
    
    let swWriter = new System.IO.StreamWriter( outputFile )

    let allUnds = getUnion_index_members indexes

    let name2FundTicker = getFundamentalTicker_static_data allUnds 
    printfn "not equal to fund ticker %d" (Map.fold ( fun s k v -> if k <> v then s + 1 else s ) 0 name2FundTicker)
    let fundTicker2Names = name2FundTicker |> Map.toList |> List.unzip ||> ( fun l r -> ( r , l ) ) ||> List.zip |> buildMap2List
    let fundTickers = name2FundTicker |> valueSet 
    printfn "total names %d" fundTickers.Count

    let allNames = [ fundTickers ; set allUnds ] |> Set.unionMany |> Set.toList

    for field in fields do
        let data = get_market_data field allNames |> Map.find field

        for ticker in fundTickers do
            if (Map.find ticker data).Length = 0 then
                let mutable txt = ticker + "," + field + ",0,"
                for n in Map.find ticker fundTicker2Names do
                    txt <- txt + "," + n + "," + ((Map.find n data).Length).ToString()

                swWriter.WriteLine( txt )

    swWriter.Close()

    
let indexes = 
    
    [ 

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
//    "NIFTY INDEX" ;///
//    "SET50 INDEX" ;///
//        "KOSPI2 INDEX" ;
//    "FBMKLCI INDEX" ;///
//    "WIG20 INDEX" ;///
//    "SX5E INDEX" ;///
//    "TOP40 INDEX" ;///
//    "INDU INDEX" ;///
//        "SPX INDEX" ;
//        "HSCEI INDEX" ;
//    "NKY INDEX" ;///
//        "NDX INDEX" ;
//        "RTY INDEX" ;
//        "MID INDEX" ;

//        "RTSI$ INDEX" ;
//        "SHSN300 INDEX";
//        "XIN9I INDEX";
//        "MEXBOL INDEX"; 
//        "IBOV INDEX" ;


//        "XU030 INDEX";
//        "FTASE INDEX" ////////(???? no greek t-bills)
//        "BUX INDEX" ////////////( ???? too small?)

    ]

    |> set

let fields = 
    
        [

        "ltDebt" ;

        "priceToBook" ;

        "totalEquity" ;

        "returnOnEquity" ;

         "grossMargin" ;

        "salesPerShare" ;

        "earnings" ;

        "dividendYield"  ;

        "sharesOutstanding" ;

        "operatingIncome" ;

        "ebitda" ;

        "enterpriseValue" ;

        "price" ;

        "bookValue" ;

        "marketCap" 

        ] ;


// execute

//doTableIndexMembers indexes
doMarketData indexes fields "../output/fundTickerMarketData.csv"
