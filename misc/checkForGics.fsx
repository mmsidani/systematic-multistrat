#r@"..\sa2-dlls\common.dll"
#r@"C:\Program Files (x86)\Reference Assemblies\Microsoft\Framework\.NETFramework\v4.5\System.Data.Linq.dll"



open System
open System.Data
open System.Linq

open SA2.Common.DbSchema
open SA2.Common.Table_index_members
open SA2.Common.Table_static_data
open SA2.Common.Table_instruments
open SA2.Common.Utils
open SA2.Common.Dates


let checkForGics () =

    // which names have no Gics and on which dates?


    let multistrategyContext = createMultistrategyContext()

    let names =

        query

            {
                for row in multistrategyContext.Tbl_index_members do

                select row.StockId

            }

            |> set

    let name2Gics = 

        query

            {
                for row in multistrategyContext.Tbl_static_data do

                where ( row.FieldId = 4 )

                select ( row.InstrumentId , row.Value )

            }

            |> Map.ofSeq

    let namesWithNoGics =

        names

            |> Set.filter ( fun n -> Map.containsKey n name2Gics |> not ) 

    let tickersWithNoGics =

        namesWithNoGics
            |> getTicker_instruments

    tickersWithNoGics |> paste |> printfn "no Gics:\n%s" 


    let noGicsDates =

        query

            {
                for row in multistrategyContext.Tbl_index_members do

                select ( row.StockId , row.Date )

            }

            |> Seq.filter ( fun ( i , _ ) -> namesWithNoGics.Contains i )

            |> Seq.groupBy ( fun ( _ , d ) -> d ) 

            |> Seq.map ( fun ( d , l ) -> ( d |> obj2Date , l |> Seq.map ( fun ( n , _ ) -> n ) ) ) 


    noGicsDates

        |> Seq.map ( fun ( d , l ) -> ( d , paste l ) ) 

        |> Seq.iter ( fun ( d , l ) -> 

                            printfn "no gics on %d:\n%s" d l

                    )

    let otherStaticData =

        query

            {
                for row in multistrategyContext.Tbl_static_data do

                select ( row.InstrumentId , row.Value )

            }

            |> Seq.filter ( fun ( i , _ ) -> namesWithNoGics.Contains i )
    
    printfn "data that we have"
       
    otherStaticData

        |> Seq.map ( fun ( i , v ) -> ( Map.find i tickersWithNoGics , v.ToString() ) )

        |> Seq.iter ( fun ( i , v ) -> printfn "( %s , %s )" i v )



// execute

checkForGics ()