module SA2.Equity.EquityFactor


open System

open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.Math



let singleFactor date frequency numPeriods marketData factorUniverse =

    let priceData = 
        
        Map.find DatabaseFields.price marketData

            |> Map.filter ( fun i _ -> Set.contains i factorUniverse )

    let numDays =

        // +1 because we have to calculate returns below

        if frequency = DateFrequency.daily then

            numPeriods + 1

        elif frequency = DateFrequency.weekly then

            ( numPeriods + 1 ) * 7

        elif frequency = DateFrequency.monthly then

            ( numPeriods + 1 ) * 31

        else

            raise( Exception ( "frequency " + frequency.ToString() + " not supported in singleFactor yet." ) )
            
    let startDate = ( date |> date2Obj ).AddDays( - numDays |> float ) |> obj2Date

    let dates = datesAtFrequency frequency startDate date

    let levelsForDates =

        if frequency = DateFrequency.daily then

            priceData

                |> Map.map ( fun _ l -> l |> List.filter ( fun ( d , _ ) -> startDate <= d && d <= date ) ) // input data is assumed to be daily

        else 

            [
            for date in dates ->

                lastDataMap date priceData |> Map.toList |> List.map ( fun ( i , p ) -> ( i , ( date , p ) ) )
            ]

            |> List.concat
            |> buildMap2List

        |> Map.map ( fun i l -> l |> List.sortBy ( fun ( d , _ ) -> d ) )

        |> Map.map ( fun i l -> l |> List.map ( fun ( _ , p ) -> p ) ) 

    levelsForDates

        |> Map.iter ( fun i l -> 

            if l.Length < numPeriods + 1 then

                raise ( Exception ( i + " does not have enough data." ) ) ) // TODO Note this doesn't catch all errors: an index that was last updated a year ago will have its value from a year ago repeated till today to fill the series and wouldn't trigger this 

    let prices =

        [|
        for i in factorUniverse ->

            Map.find i levelsForDates |> Array.ofList

        |]

        |> array2D

    let returns = Array2D.zeroCreate numPeriods factorUniverse.Count

    for i in 0 .. factorUniverse.Count-1 do

        for j in 1 .. numPeriods-1 do

            returns.[ i , j ] <- prices.[ i , j ] / prices.[ i , j - 1] - 1.0
    
    firstPcaComp returns