module SA2.Simulation.SingleNameSimulation



open System
open SA2.Common.DbSchema
open SA2.Common.Table_rate_data
open SA2.Common.Table_market_data
open SA2.Common.Utils



let private ccyDtrSuffixConstant = ".usd.fx.b.dtr"
let private maximumNumberOfPointsConstant = 1000




let getCcyDtr ccys =

    ccys

        |> Seq.toList
        |> List.map ( fun ( ccy : string ) -> ccy.ToLower () +  ccyDtrSuffixConstant )
        |> get_rate_data 




let getNamesDtr today names =

    getForDate_market_data maximumNumberOfPointsConstant today DatabaseFields.dailyTotalReturn names |> Map.find DatabaseFields.dailyTotalReturn



    
let convertToBaseCcyPerspective name2Currency currencyDtrs namesDtrs names =
        
    names

        |> List.map ( fun n -> 

                        let nCcy = Map.find n name2Currency
                        let alignedDtrs = ( currencyDtrs |> Map.filter ( fun c _ -> c = nCcy ) ,
                                                namesDtrs |> Map.filter ( fun k _ -> k = n ) ) ||> Map.fold ( fun s k v -> Map.add k v s ) |> alignOnSortedDates

                        ( n ,

                            ( alignedDtrs.Item n , alignedDtrs.Item nCcy )

                                ||> List.map2 ( fun ( ( d0 , rn ) : int * float ) ( d1 , rc ) -> 

                                                    if d0 <> d1 then

                                                        raise ( Exception "misalignment in dtrs" )
                                                
                                                    ( d1 , rn + rc ) ) )

                        )


        |> Map.ofList

        

//
//let simulate portfolios =
//
//    let names = 
//        
//



//
//let buildName2CurrencyDtr name2Currency names =
//
//    let name2CcyDtr = 
//
//        names
//
//            |> List.map ( fun )
//         
//        Map.map ( fun _ c -> c.ToLower() + ".usd.fx.b.dtr" )
//
//    index2ConversionRate
//
//        |> Map.map ( fun i r -> Map.find r rate2Ccy )
//
//
//
//
//
//
//
//        let ratesData =
//            index2CurrencyDtr
//                |> valueSet
//                |> Set.toList
//                |> List.filter ( fun c -> c <> "usd.usd.fx.b.tr" )
//                |> get_rate_data // from the old db
//                |> Map.map ( fun _ l -> l |> List.toArray )
//                |> ( fun m ->
//                            let dateZero = Map.find "SPX INDEX" marketData |> Array.map ( fun ( d , _ ) -> ( d , 0.0 ) ) 
//                            Map.add "usd.usd.fx.b.tr" dateZero  m ) // why? just so that we don't have to treat SPX and INDU differently below. there's no usd.usd.fx.b.dtr in the DB
//
//
//
// ()