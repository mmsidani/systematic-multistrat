module SA2.Rates.Rates



open System


open SA2.Common.Utils
open SA2.Common.Dates



let annualizeRate rate2PayFreq rates =

    rates

        |> Map.map ( fun n l -> 
                        
                        let payFreq = Map.find n rate2PayFreq

                        l |> List.map ( fun ( d , y ) -> 
                        
                                            ( d , 
                                            
                                                ( 1.0 + y / payFreq  ) ** payFreq - 1.0  )

                                            )

                                        )




let forwardRates payFreqLong payFreqShort maturityLong maturityShort ratesLongData ratesShortData =

    let annualizeExp = 1.0 / ( maturityLong - maturityShort )

    [ ( "long" , ratesLongData ) ; ( "short" , ratesShortData ) ] 
    
        |> Map.ofList 

        |> alignOnSortedDates // dates sorted in decreasing order

        |> ( fun m -> ( Map.find "long" m , Map.find "short" m ) )

        ||> List.map2 ( fun ( d , rl ) ( _ , rs ) -> // same date d

                ( d , ( ( 1.0 + rl / payFreqLong ) ** ( payFreqLong * maturityLong ) / ( 1.0 + rl / payFreqShort ) ** ( payFreqShort * maturityShort ) ) ** annualizeExp  - 1.0 ) ) 




let priceBond payFrequency maturity coupon yld = 


    let yldFreq = yld / payFrequency

    List.init ( ( int maturity ) * ( int payFrequency ) ) ( fun i -> 1.0 / ( 1.0 + yldFreq ) ** ( float ( i + 1 ) ) )

    |> List.sum 

    |> ( fun s ->  coupon / payFrequency * s +  1.0 / ( 1.0 + yldFreq ) ** ( maturity * payFrequency ) )

    


let parRateReturns rate2PayFreq rate2Tenor rates =

    // maturity tomorrow is one day later

    rates

    |> Map.map ( fun r l -> 

                    let payFreq = Map.find r rate2PayFreq
                    let tenor = Map.find r rate2Tenor

                    l

                    |> List.sortBy ( fun ( d , _ ) -> d )
                    |> Seq.pairwise
                    |> Seq.toList
                    |> List.map ( fun ( ( d0 , r0 ) , ( d1 , r1 ) ) -> 
                    
                                    let capitalRet = priceBond payFreq tenor r0 r1 - 1.0                                    
                                    let interest = ( ( 1.0 + r0 / payFreq ) ** ( payFreq / 365.25 ) - 1.0 ) * ( System.DateTime.op_Subtraction( d1 |> date2Obj , d0 |> date2Obj ).TotalDays |> float )

                                    ( d1 , capitalRet , capitalRet + interest ) )

            )


