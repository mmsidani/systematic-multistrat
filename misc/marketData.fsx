#r@"..\sa2-dlls\common.dll"


open System

open SA2.Common.Io
open SA2.Common.Utils
open SA2.Common.Table_index_members
open SA2.Common.Table_instruments
open SA2.Common.Table_market_data
open SA2.Common.Dictionaries


let dataFieldDictionary = {

    bbField2Sa2NameDivisor = 
    
        [

        ( "BS_LT_BORROW" , "ltDebt" , 1.0 ) ;

        ( "PX_TO_BOOK_RATIO" , "priceToBook" , 1.0 ) ;

        ( "TOTAL_EQUITY" , "totalEquity" , 1.0 ) ;

        ( "RETURN_COM_EQY" , "returnOnEquity" , 100.0 ) ;

        ( "TRAIL_12M_OPER_MARGIN" , "grossMargin" , 100.0 ) ;

        ( "TRAIL_12M_SALES_PER_SH" , "salesPerShare" , 1.0 ) ;

        ( "TRAIL_12M_EPS" , "earnings" , 1.0 ) ;

        ( "EQY_DVD_YLD_12M" , "dividendYield" , 100.0 ) ;

        ( "EQY_SH_OUT" , "sharesOutstanding" , 1.0 ) ;

        ( "TRAIL_12M_OPER_INC" , "operatingIncome" , 1.0 ) ;

        ( "EBITDA" , "ebitda" , 1.0 ) ;

        ( "CURR_ENTP_VAL" , "enterpriseValue" , 1.0 ) ;

        ( "PX_LAST" , "price" , 1.0 ) ;

        ( "BOOK_VAL_PER_SH" , "bookValue" , 1.0 ) ;

        ( "CUR_MKT_CAP" ,  "marketCap" , 1.0 ) ;

        ( "DAY_TO_DAY_TOT_RETURN_GROSS_DVDS" , "dtr" , 100.0 ) 

        ]

}



let fileNamesToLoad =

    [

    for first in [ 'a' .. 'f' ] ->
        [
        for second in [ 'a' .. 'z' ] ->

            "x" + first.ToString() + second.ToString()
        ]
    ]

    |> List.concat

    |> List.filter ( fun name -> name < "xfy" ) // because those 2 didn't exist. data didn't extend to that

    |> List.filter ( fun name -> name >= "xfd" ) // code broke while processing this file



let sourceName = "BB"

let pathName = "../data/"



let uploadMarketData () =

    for fl in fileNamesToLoad do
        
        let file = pathName + fl

        printfn "reading file %s %s" file (DateTime.Now.TimeOfDay.ToString())
        let data = SA2.Common.Io.readFile( file )
        let zippedArray = 
            data 
                |> Seq.map 
                    ( 
                    fun l -> 
                        let splits = l.Split( [| ','|])
                        ( splits.[ 0 ] , ( splits.[ 1 ] |> int ) , splits.[ 2 ] , float splits.[3] )

                    )
                |> Seq.toArray

        let fields = zippedArray |> Array.map ( fun ( f , _ , _ , _ ) -> f ) |> set

        for f in fields do
            printfn "field %s %s" f (DateTime.Now.TimeOfDay.ToString())
            let dateTickerValues = zippedArray |> Array.filter ( fun ( field , _ , _ , _ ) -> field = f ) |> Array.map ( fun ( _ , d , t , v ) -> (d , t , v ) )
            updateExisting_market_data sourceName ( dataFieldDictionary.mapIt( f ) ) ( dataFieldDictionary.divisor( f ) ) dateTickerValues
            printfn "done with field %s %s" f (DateTime.Now.TimeOfDay.ToString())
        printfn "done with file %s %s" file (DateTime.Now.TimeOfDay.ToString())



// execute

uploadMarketData ( )