#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"


open System
open System.Data
open System.Linq
open SA2.Common.Table_market_data
open SA2.Common.Table_instruments
open SA2.Common.Table_data_fields
open SA2.Common.Utils
open SA2.Common.DbSchema


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

    ]


let removeDuplicates () =

    let allTickers = getAllTickers_instruments () 

    for ticker in allTickers do

        printfn "now doing %s" ticker

        let multistrategyContext = createMultistrategyContext ()

        let instId = [ ticker ] |> set |> getKey_instruments |> Map.find ticker
 
        for field in fields do
            
            printfn "now doing %s" field

            let fieldId = [ field ] |> set |> getKey_data_fields false |> Map.find field

            let tData = 

                query {

                    for row in multistrategyContext.Tbl_market_data do

                    where ( fieldId = row.FieldId && instId = row.InstrumentId )

                    select row

                }

                |> Seq.sortBy ( fun row -> row.Date )

            printfn "before: %d " (Seq.length tData)

            if Seq.isEmpty tData |> not then

                let dataToLoad = tData |> Seq.pairwise |> Seq.toList |> List.filter ( fun ( row0 , row1 ) -> row0.Value <> row1.Value ) |> List.map ( fun ( _ , row ) -> row ) |> List.append [ Seq.head tData ]

                printfn "\nafter: %d " (Seq.length dataToLoad) 

                if Seq.length tData <> dataToLoad.Length then

                    try

                        multistrategyContext.Tbl_market_data.DeleteAllOnSubmit tData

                        multistrategyContext.SubmitChanges ()

                    with

                    | e -> raise e

                    try

                        // new context needed

                        let multistrategyContext1 = createMultistrategyContext ()

                        let newRecords = dataToLoad |> List.map ( fun row -> new MultistrategySchema.Tbl_market_data (InstrumentId = row.InstrumentId , FieldId = row.FieldId , SourceId = row.SourceId , Value = row.Value , Date = row.Date , Divisor = row.Divisor ) )

                        multistrategyContext1.Tbl_market_data.InsertAllOnSubmit newRecords

                        multistrategyContext1.SubmitChanges ()

                    with

                    | e -> raise e

                else

                    printfn "%s nothing to do for %s" ticker field

// execute

removeDuplicates ()