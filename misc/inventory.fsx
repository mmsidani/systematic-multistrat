#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\equities.dll"

open System.IO

open SA2.Equity.IndexTypes

open SA2.Common.DbSchema
open SA2.Common.Table_index_members
open SA2.Common.Table_static_data
open SA2.Common.Table_market_data
open SA2.Common.Dictionaries

let indexUniverse =

    {

    indexUniverse =
        [ 

         // members and static data done

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

    }

let dataFieldDictionary = {

    bbField2Sa2NameDivisor = 
    
        [

        ( "BS_LT_BORROW" , DatabaseFields.longTermBorrowing , 1.0 ) ;

        ( "PX_TO_BOOK_RATIO" , DatabaseFields.priceToBook , 1.0 ) ;

        ( "TOTAL_EQUITY" , DatabaseFields.totalEquity , 1.0 ) ;

        ( "RETURN_COM_EQY" , DatabaseFields.returnOnEquity , 100.0 ) ;

        ( "TRAIL_12M_OPER_MARGIN" , DatabaseFields.margin , 100.0 ) ;

        ( "TRAIL_12M_SALES_PER_SH" , DatabaseFields.salesPerShare , 1.0 ) ;

        ( "TRAIL_12M_EPS" , DatabaseFields.earnings , 1.0 ) ;

        ( "EQY_DVD_YLD_12M" , DatabaseFields.dividendYield , 100.0 ) ;

        ( "EQY_SH_OUT" , DatabaseFields.sharesOutstanding , 1.0 ) ;

        ( "TRAIL_12M_OPER_INC" , DatabaseFields.operatingIncome , 1.0 ) ;

        ( "EBITDA" , DatabaseFields.ebitda , 1.0 ) ;

        ( "CURR_ENTP_VAL" , DatabaseFields.enterpriseValue , 1.0 ) ;

        ( "PX_LAST" , DatabaseFields.price , 1.0 ) ;

        ( "BOOK_VAL_PER_SH" , DatabaseFields.bookValue , 1.0 ) ;

        ( "CUR_MKT_CAP" ,  DatabaseFields.marketCap , 1.0 ) ;

        ( "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS" , DatabaseFields.dailyTotalReturn , 100.0 ) 

        ]

}

let fields =

    [
    "BS_LT_BORROW" ;

    "PX_TO_BOOK_RATIO" ;

    "TOTAL_EQUITY" ;

    "RETURN_COM_EQY" ;

    "TRAIL_12M_OPER_MARGIN" ;

    "TRAIL_12M_SALES_PER_SH" ;

    "EQY_SH_OUT" ;

    "TRAIL_12M_OPER_INC" ;

    "EBITDA" ;

    "CURR_ENTP_VAL" ;

    "PX_LAST" ;

    "BOOK_VAL_PER_SH" ;

    "CUR_MKT_CAP"

    "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS"

    ] ;

let staticFields =

    [
    "currency" ;
    "gicsIndustry"
    ]

let checkInventory () =

    let swC = new StreamWriter( "../output/inventoryData.csv" )
    let swE = new StreamWriter( "../output/inventoryNoData.csv" )
    let swF = new StreamWriter( "../output/inventoryFundamental.csv" )
    let swS = new StreamWriter( "../output/inventoryStatic.csv" )

    try

        for index in indexUniverse.indexUniverse do

            let indexMembers = getOne_index_members index

            let fundamentalTickers = getFundamentalTicker_static_data indexMembers

            for i in indexMembers do 

                if fundamentalTickers.ContainsKey i |> not then

                    swF.WriteLine( index + " " + i + " hasnofundamentalticker " )

                else

                    let ft = Map.find i fundamentalTickers

                    if i <> ft then

                        swF.WriteLine( index + " " + i + " isdifferenttofundamentalticker " )

            let fundTickers = indexMembers |> Set.filter ( fun i -> Map.containsKey i fundamentalTickers ) |> Set.map ( fun i -> Map.find i fundamentalTickers ) |> Set.toList

            for field in fields do

                let sa2Field = dataFieldDictionary.mapIt field
                let marketData = fundTickers |> get_market_data sa2Field  |> Map.find sa2Field

                for i in fundTickers do

                    if ( Map.containsKey i marketData |> not ) || (Map.find i marketData |> List.length) = 0 then

                        swE.WriteLine ( index + " " +  i + " " + field + " nodatafor " )

            for field in staticFields do

                try

                    let staticData = get_static_data field fundTickers |> Map.find field |> Map.ofList

                    for i in fundTickers do

                        if Map.containsKey i staticData |> not then

                            swS.WriteLine ( index + " " +  i + " " + field + " nodatafor " )
                with
                | _ -> printfn "%s raised an exception for %s " index field

        swC.Close()
        swE.Close()
        swF.Close()
        swS.Close()

    with
    | e -> 
        printfn "%s" ( e.ToString() )
        swC.Close()
        swE.Close()
        swF.Close()
        swS.Close()


// execute

checkInventory ()