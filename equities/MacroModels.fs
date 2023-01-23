module SA2.Equity.MacroModels


open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.Dates
open SA2.Common.Math
open SA2.Common.ForEx

open System


open EquityMetricsOps
open EquityMetrics
open FeaturesNames



let private quantileProbsConstant = [| 0.2 ; 0.4 ; 0.6 ; 0.8 ; 1.0 |]
let private maximumGrowthGapPoints = 120 // use that many readings in calculating the output gap. so 30 years


let private filterForAvailability ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate orderedMarketData rateNames =
    
    let ebitData = Map.find DatabaseFields.ebit orderedMarketData
    let entPData = Map.find DatabaseFields.enterpriseValue orderedMarketData
    let yieldData = Map.find DatabaseFields.yld orderedMarketData

    let growthData = Map.find DatabaseFields.price orderedMarketData

    let gdpData = Map.find DatabaseFields.realGrowthRate orderedMarketData

    rateNames

        |> List.filter 
        
            ( 
            
            fun r -> 
                
                let proxyRate = Map.find r rate2Proxy
                
                let growthRate = Map.find r rate2Growth
                
                let gdpRate = Map.find r rate2RealGdpRate
                
                Map.containsKey r ebitData && List.length ( Map.find r ebitData ) >= ratesDays 

                    && Map.containsKey r entPData && List.length ( Map.find r entPData ) >= ratesDays 

                        && Map.containsKey proxyRate yieldData && List.length ( Map.find proxyRate yieldData ) >= proxyDays

                            && Map.containsKey growthRate growthData && List.length ( Map.find growthRate growthData ) >= growthQuarters 
                            
                                && Map.containsKey gdpRate gdpData && List.length ( Map.find gdpRate gdpData ) >= gapQuarters

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

            

let buildGdpLevelsFromYoy yoyGdpRates =

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
                                                |> Array.mapi ( fun i f -> ( f - vs.[ i ] ) / vs.[ i ] ) // sign should be such that small is a short
                                                |> Array.toList ) ) 
                                ||> List.zip  ) 

            |> averages gapQuarters

    rateNames

        |> Set.map ( fun r -> ( r , Map.find ( Map.find r rate2RealGdpRate ) outputGap ) )
        |> Map.ofSeq
        
        





let private calcGrowthDifferentials proxyDays growthQuarters numeraire date rateToProxy rateToGrowth orderedMarketData rateNames =

    let yieldData = Map.find DatabaseFields.yld orderedMarketData

    let growthData = Map.find DatabaseFields.price orderedMarketData

    let proxyRates = 

        rateNames

            |> Set.map ( fun r -> Map.find r rateToProxy )

    let avgProxyRates = 
    
        yieldData
        
            |> Map.filter ( fun r _ -> Set.contains r proxyRates )
            
            |> averages proxyDays

    let growthRates =

        rateNames

            |> Set.map ( fun r -> Map.find r rateToGrowth )

    let avgGrowthRates = 
    
        growthData
        
            |> Map.filter ( fun r _ -> Set.contains r growthRates )
            
            |> averages growthQuarters

    let differentials = 

        rateNames
        
            |> Set.toList
            // the next one is the opposite of the measure for bonds
            |> List.map ( fun r -> ( r , ( Map.find ( Map.find r rateToProxy ) avgProxyRates ) / 4.0  - Map.find ( Map.find r rateToGrowth ) avgGrowthRates ) )

            |> Map.ofList

    differentials




let calcValuations useConversion name2Conversion numeraire date orderedMarketData instruments =
    
    let getDbData = getDbDataForDate orderedMarketData date

    let enterpriseValues : Map< string , float > = getDbData DatabaseFields.enterpriseValue

    let ebit = getDbData DatabaseFields.ebit

    let conversionData = getDbData DatabaseFields.yld

    let baseFactor = 1.0 + Map.find numeraire conversionData

    let conversionFactors = name2Conversion |> Map.map ( fun _ r -> baseFactor / ( 1.0 + Map.find r conversionData ) )

    instruments

        |> Seq.filter ( fun n -> enterpriseValues.ContainsKey n && ebit.ContainsKey n )

        |> Seq.map ( fun n -> ( n , Map.find n ebit / Map.find n enterpriseValues ) )
            
        |> ( fun ret ->
                if useConversion then
                    ret |> Seq.map ( fun ( n , v ) ->  ( n , v * ( Map.find n conversionFactors ) ) )
                else ret ) 

        |> Map.ofSeq



let calcEGP date orderedMarketData inst2Gap instruments =

    let prices = getDbDataForDate orderedMarketData date DatabaseFields.price

    instruments

        |> Seq.filter ( fun n -> prices.ContainsKey n && Map.containsKey n inst2Gap )

        |> Seq.map ( fun n -> ( n , Map.find n inst2Gap / Map.find n prices ) )
    
        |> Map.ofSeq




let calcReturnOnCapital date orderedMarketData instruments =
    
    let getDbData = getDbDataForDate orderedMarketData date

    let returnOnCapital = getDbData DatabaseFields.returnOnCapital

    instruments

        |> Seq.filter ( fun n -> returnOnCapital.ContainsKey n )

        |> Seq.map ( fun n -> ( n , Map.find n returnOnCapital ) )

        |> Map.ofSeq




let averageAllScores lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate orderedMarketData rateNames =

    // IMPORTANT NOTE: ranking functions should give lowest rank to most attractive to short    

   let name2BogusBucket = rateNames |> Set.map ( fun r -> ( r , 1 ) ) |> Map.ofSeq // bogus bucket 1; only needed if we're using averageQuantilesPerBucket
            
   let inst2Gap = calcOutputGaps gapQuarters lambda rate2RealGdpRate orderedMarketData rateNames

   [ 
   
//    calcValuations false rate2Conversion numeraire date orderedMarketData rateNames ;
    
    calcGrowthDifferentials proxyDays growthQuarters numeraire date rate2Proxy rate2Growth orderedMarketData rateNames ; // NO fx conversion

//    calcEGP date orderedMarketData inst2Gap rateNames

//    calcOutputGaps gapQuarters lambda rate2RealGdpRate orderedMarketData rateNames ;
                
//    calcReturnOnCapital date orderedMarketData rateNames 
    ]

   |> averageZScore




let macroEquityModel lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate orderedMarketData rateNames =
    
    let filteredRateNames = filterForAvailability ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate orderedMarketData rateNames
    
    filteredRateNames
        
        |> set
            
        |> averageAllScores lambda ratesDays proxyDays growthQuarters gapQuarters numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate orderedMarketData
        
        |> rankByQuantiles quantileProbsConstant
        
        |> value2Keys