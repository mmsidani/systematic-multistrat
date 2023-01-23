#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"



open System
open System.Linq
open SA2.Common.DbSchema
open SA2.Common.Table_instruments


let copyTickersToSymbols () =

    let context = createMultistrategyContext () 

    let allRows =

        query 
            {
                for row in context.Tbl_instruments do

                select row
            }

            |> Seq.map ( fun row -> row.Sa2Symbol <- row.InstrumentTicker ; row )
            |> Seq.toList

    context.SubmitChanges()
    


// execute

copyTickersToSymbols () 