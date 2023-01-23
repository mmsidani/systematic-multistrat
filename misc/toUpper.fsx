#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"

open System


open SA2.Common.DbSchema
open SA2.Common.Table_instruments


let doIt () =

    let context = createMultistrategyContext ()

    let allRows =

        query

            {
                    for row in context.Tbl_instruments do

                    select row
            }

            |> Seq.toList

    allRows

        |> List.iter ( fun row -> 
                            if row.InstrumentTicker.Contains "Index" && row.InstrumentTicker.ToUpper() <> row.InstrumentTicker then

                                printfn "%s" row.InstrumentTicker
                                row.InstrumentTicker <- row.InstrumentTicker.ToUpper()
                        )
        
    context.SubmitChanges () 





// execute

doIt ()