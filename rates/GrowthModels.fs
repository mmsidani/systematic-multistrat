module SA2.Rates.GrowthModels


open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.Dates
open SA2.Common.Math
open SA2.Common.ForEx

open System



let private quantileProbsConstant = [| 0.2 ; 0.4 ; 0.6 ; 0.8 ; 1.0 |]
//let private quantileProbsConstant = [| 0.25 ; 0.5 ; 0.75 ; 1.0 |]

let private maximumGrowthGapPoints = 120 // use that many readings in calculating the output gap. so 30 years


let private filterForAvailability useReal ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi orderedMarketData rateNames =
    
    let yieldData = Map.find DatabaseFields.yld orderedMarketData

    let growthData = Map.find DatabaseFields.nominalGrowthRate orderedMarketData

    let gdpData = Map.find DatabaseFields.realGrowthRate orderedMarketData

    let cpiData = Map.find DatabaseFields.cpiRate orderedMarketData

    rateNames

        |> List.filter 
        
            ( 
            
            fun r -> 

                let proxyRate = Map.find r rate2Proxy

                let growthRate = Map.find r rate2Growth

                let gdpRate = Map.find r rate2RealGdpRate

                let cpiRate = Map.find proxyRate proxyRate2Cpi

                Map.containsKey r yieldData && List.length ( Map.find r yieldData ) >= ratesDays 

                    && Map.containsKey proxyRate yieldData && List.length ( Map.find proxyRate yieldData ) >= proxyDays

                        && Map.containsKey growthRate growthData && List.length ( Map.find growthRate growthData ) >= growthQuarters 
                            
                            && Map.containsKey gdpRate gdpData && List.length ( Map.find gdpRate gdpData ) >= gapQuarters

                                && ( if useReal then Map.containsKey cpiRate cpiData else true )

            )




let private averages n yieldData =

    // for every rate x, x values assumed sorted in decreasing order
    
    yieldData

        |> Map.map 
        
                    ( 
                    
                    fun _ l -> 
                        
                        let a = l |> List.toArray 
                        
                        a.[ 0 .. min ( n - 1 ) ( a.Length - 1 ) ] 
                        
                            |> Array.map ( fun ( _ , v ) -> v ) 
                            
                            |> Array.average 
                            
                    )
    



let private fillAndSortQuarterlyGaps ( datesVals : ( int * float ) list ) =

    let datesObjs = 

        datesVals 
    
            |> List.sortBy ( fun ( d , _ ) -> d ) 
            |> List.map ( fun ( d , v ) -> ( d |> date2Obj , v ) ) 

    let augmented =

        datesObjs

            |> Seq.pairwise
            |> Seq.toList
            |> List.collect ( 
        
                        fun ( ( d0 , v0 ) , ( d1 , v1 ) ) -> 
                        
                            let timeSpan = ( d1.Subtract d0 ).TotalDays

                            let quarterGap = diffQuarters d0 d1

                            if quarterGap = 2 then 
                        
                                // interpolate once    

                                [ ( timeSpan / 2.0 |> d0.AddDays |> shiftWeekendsOneDate , v0 + 0.5 * ( v1 - v0 ) ) ] 
                         
                            elif quarterGap = 3 then

                                // interpolate twice

                                [ ( timeSpan / 3.0 |> d0.AddDays |> shiftWeekendsOneDate  , v0 + ( v1 - v0 ) / 3.0 )  ;
                                    ( 2.0 * timeSpan / 3.0 |> d0.AddDays |> shiftWeekendsOneDate , v0 + ( v1 - v0 ) * 2.0 / 3.0 ) ] 
                            else
                                                   
                                List.empty 
                        )

            |> List.append datesObjs
            |> List.sortBy ( fun ( d , _ ) -> d )

    // finally truncate series if it has gaps of more than 3 quarters

    let cutOffIndex =

        augmented

            |> Seq.pairwise
            |> Seq.tryFindIndex ( fun ( ( d0 , _ ) , ( d1 , _ ) ) -> diffQuarters d0 d1  > 3 )

    match cutOffIndex
        with
            | Some idx -> Seq.skip ( idx + 1 ) augmented |> Seq.toList
            | None -> augmented

            

let private buildGdpLevelsFromYoy yoyGdpRates =

    yoyGdpRates

        |> Map.map ( 
        
            fun k l ->
                
                let sortedL = l |> fillAndSortQuarterlyGaps
                        
                let cumL : float [] = Array.zeroCreate ( sortedL.Length + 4 )

                cumL.[ 0 ]  <- 1.0
                cumL.[ 4 ] <- 1.0 + snd sortedL.[ 0 ]
                cumL.[ 1 ] <- cumL.[ 4 ] ** ( 0.25 )
                cumL.[ 2 ] <- cumL.[ 4 ] ** ( 0.5 )
                cumL.[ 3 ] <- cumL.[ 4 ] ** ( 0.75 )

                for i in 5 .. cumL.Length - 1 do

                    cumL.[ i ] <- cumL.[ i - 4 ] * ( 1.0 + snd sortedL.[ i - 4 ] )

                cumL.[ 4 .. ]

                    |> Array.mapi ( fun i v -> ( fst sortedL.[ i ] , v  ) )

                    |> Array.toList

                    |> List.rev

    )
    



let private buildGdpLevelsFromQoq qoqGdpRates = 

    qoqGdpRates
 
        |> Map.map ( fun k l -> 

                let sortedFilledData = l |> fillAndSortQuarterlyGaps

                sortedFilledData

                    |> List.map ( fun ( _ , v ) -> v )
                    |> ( fun vs ->

                            let mutable cumL = Array.zeroCreate l.Length
                            cumL.[ 0 ] <-  1.0
                            for i in 1 .. sortedFilledData.Length - 1 do
                                cumL.[ i ] <- cumL.[ i - 1 ] * ( 1.0 + vs.[ i ] )

                            cumL 

                                |> Array.toList
                                |> List.mapi ( fun i v -> (  fst sortedFilledData.[ i ] , v ) )
                                |> List.rev
                        )
                    
            )


                            
    
let private calcOutputGaps gapQuarters lambda rate2RealGdpRate orderedMarketData rateNames =

    let gdpRateNames = rate2RealGdpRate |> Map.filter ( fun k _ -> Set.contains k rateNames ) |> valueSet

    let gdpData =
    
        orderedMarketData

           |> Map.find DatabaseFields.realGrowthRate

           |> Map.filter ( fun k _ -> Set.contains k gdpRateNames )

           |> buildGdpLevelsFromYoy 
    
    let outputGap =

        gdpData

            |> Map.map ( fun _ l -> 

                            l 
                                |> List.unzip 
                                |> ( fun ( ds , vs ) -> 
                                        
                                        ( ds |> List.toArray |> ( fun a -> a.[ 0 .. min ( a.Length-1 ) maximumGrowthGapPoints ] ) |> Array.toList , 
                                        
                                            vs 
                                                |> List.toArray 
                                                |> Array.map ( fun g -> log g ) // filter the logs
                                                |> ( fun a -> a.[ 0 .. min ( a.Length-1 ) maximumGrowthGapPoints ] ) // don't use entire history
                                                |> hpFilter lambda 
                                                |> Array.map ( fun lnf -> exp lnf ) 
                                                |> Array.mapi ( fun i f -> ( f - vs.[ i ] ) / f ) // sign should be such that small is a short
                                                |> Array.toList ) ) 
                                ||> List.zip  ) 

            |> averages gapQuarters

    rateNames

        |> Set.map ( fun r -> ( r , Map.find ( Map.find r rate2RealGdpRate ) outputGap ) )
        |> Map.ofSeq
        
        

            
let private calcRates ratesDays numeraire date rate2Conversion orderedMarketData rateNames =     
    
    let yieldData = 
    
        orderedMarketData

           |> Map.find DatabaseFields.yld
    
    let avgRatesToTrade =  

        yieldData

            |> Map.filter ( fun r _ -> Set.contains r rateNames ) 

            |> averages ratesDays

    
    let conversionRates = 

        rate2Conversion

            |> Map.filter ( fun r _ -> Set.contains r rateNames )

            |> Map.map
            
                ( 
            
                    fun _ c -> 
                            
                            Map.find c yieldData |> List.head |> snd 

                )

    let convertedRates =

        let domesticRate = Map.find numeraire yieldData |> List.head |> snd

        avgRatesToTrade

            |> Map.map ( fun r v -> ( 1.0 + v ) * ( 1.0 + domesticRate ) / ( 1.0 + Map.find r conversionRates ) - 1.0 )

    convertedRates




let private calcGrowthDifferentials useReal annualizeGrowthRate proxyDays growthQuarters numeraire date rate2Proxy rate2Growth rate2RealGdpRate proxyRate2CpiRate orderedMarketData rateNames =

    let yieldData = Map.find DatabaseFields.yld orderedMarketData

    let growthData =

        if useReal then

            Map.find DatabaseFields.realGrowthRate orderedMarketData

        else
            
            Map.find DatabaseFields.nominalGrowthRate orderedMarketData

    let proxyRates = 

        rateNames

            |> Set.map ( fun r -> Map.find r rate2Proxy )

    let avgProxyRates = 

        yieldData 
        
            |> Map.filter ( fun r _ -> Set.contains r proxyRates )
            |> ( fun m -> 
                    if useReal then
                        let cpis = Map.find DatabaseFields.cpiRate orderedMarketData 

                        m
                            |> Map.map ( 
                                 fun r l -> 
                                        let rCpis = Map.find ( Map.find r proxyRate2CpiRate ) cpis
                                        let filledCpi = fillGaps true ( l |> List.map ( fun ( d , _ ) -> d ) ) rCpis
                                        let dataMap = Map.add "cpi" filledCpi Map.empty |> Map.add "yield" l |> alignOnSortedDates

                                        ( Map.find "yield" dataMap , Map.find "cpi" dataMap ) ||> List.map2 ( fun ( d , y ) ( _ , c ) -> ( d , y - c ) ) )
                    else m )
                                 
            |> averages proxyDays

    let growthRates =

        if useReal then
            rateNames  |> Set.map ( fun r -> Map.find r rate2RealGdpRate )
        else
            rateNames  |> Set.map ( fun r -> Map.find r rate2Growth )

    let avgGrowthRates = 
    
        growthData
        
            |> Map.filter ( fun r _ -> Set.contains r growthRates )
            |> ( fun m -> if annualizeGrowthRate then Map.map ( fun _ l -> l |> List.map ( fun ( d , r ) -> ( d , ( 1.0 + r ) ** 4.0 - 1.0 ) )  ) m else m  )        
            |> averages growthQuarters

    let differentials = 

        rateNames
        
            |> Set.toList
            |> List.map ( fun r -> 
                            let avgGrowthRate = if useReal then Map.find ( Map.find r rate2RealGdpRate ) avgGrowthRates else Map.find ( Map.find r rate2Growth ) avgGrowthRates

                            ( r , Map.find ( Map.find r rate2Proxy ) avgProxyRates - avgGrowthRate ) )

            |> Map.ofList

    differentials




let averageAllScores useReal annualizeGrowthRate lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi orderedMarketData rateNames =

    // IMPORTANT NOTE: ranking functions should give lowest rank to most attractive to short    

   let name2BogusBucket = rateNames |> Set.map ( fun r -> ( r , 1 ) ) |> Map.ofSeq // bogus bucket 1; only needed if we're using averageQuantilesPerBucket
            
   [ 
        calcRates ratesDays numeraire date rate2Conversion orderedMarketData rateNames ;
    
        calcGrowthDifferentials useReal annualizeGrowthRate proxyDays growthQuarters numeraire date rate2Proxy rate2Growth rate2RealGdpRate proxyRate2Cpi orderedMarketData rateNames  ; // NO fx conversion

        calcOutputGaps gapQuarters lambda rate2RealGdpRate orderedMarketData rateNames 
            
   ]

   |> averageZScore




let modelGrowthDifferential useReal annualizeGrowthRate lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi orderedMarketData rateNames =

    let filteredRateNames = filterForAvailability useReal ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi orderedMarketData rateNames
    
    filteredRateNames
        
        |> set
            
        |> averageAllScores useReal annualizeGrowthRate lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi orderedMarketData
        
        |> rankByQuantiles quantileProbsConstant
        
        |> value2Keys
