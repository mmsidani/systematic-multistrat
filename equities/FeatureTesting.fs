module SA2.Equity.FeatureTesting


open System


open SA2.Common.Dates
open SA2.Common.Table_equity_universe
open SA2.Common.Table_market_data
open SA2.Common.Table_static_data
open SA2.Common.Table_index_members
open SA2.Common.ForEx
open SA2.Common.Math
open SA2.Common.Utils
open SA2.Common.DbSchema


open EquityMetrics
open EquityMetricsOps
open IndexWeights

open EquitiesPortfolio



let private growthPeriod = 3


type FeatureEvaluationInput =

    {

        startDate : int ;
        endDate : int ;
        numberOfPortfolios : int ;
        frequency : DateFrequency ;
        growthPeriod : int ;
        forexIndicator : string ;
        baseCurrency : string 

    }




let private produceTestPortfolios numPortfolios ( data : Map< string , float > )  =

    let numInstruments = data.Count
    let instsPerPortfolio = numInstruments / numPortfolios
    let remainder = numInstruments % numPortfolios

    let numInstsInPortfolio =     
        
        [| 
        
        for i in 0 .. numPortfolios-1 ->
        
            instsPerPortfolio + if i < remainder then 1 else 0

        |]

        |> cumsum

        |> Array.append [| 0 |]

    
    let sortedData =
    
        data

        |> Map.toArray
        |> Array.sortBy ( fun ( _ , v ) -> v )
        |> Array.map ( fun ( i , _ ) -> i )
    
    [|

    for i in 1 .. numInstsInPortfolio.Length - 1 ->

        sortedData.[ numInstsInPortfolio.[ i - 1 ] .. numInstsInPortfolio.[ i ] ]             

    |]




let salesGrowthToMarketGrowth ( input : FeatureEvaluationInput ) =

    let dates = datesAtFrequency input.frequency input.startDate input.endDate

    let convert2MapMap sq =

        sq

            |> Seq.map ( fun ( d , n , v ) -> ( n , ( d , v ) ) )
            |> buildMap2List 

    [

    for date in dates ->

        let instruments = date |> date2Obj |> get_equity_universe 

        let priorDate = ( date |> date2Obj ).AddMonths( - growthPeriod ) |> obj2Date
        let staticData = get_static_data DatabaseFields.currency instruments

        let marketData = 
            get_market_data DatabaseFields.salesPerShare instruments
                |> Map.foldBack ( fun f l s -> Map.add f l s ) ( get_market_data DatabaseFields.sharesOutstanding instruments )
         
        let nameToCurrency = Map.find DatabaseFields.currency staticData |> Map.ofList
        let currencyPairs = nameToCurrency |> valueSet |> Set.toList |> formCurrencyPairs input.forexIndicator input.baseCurrency 

        let forexData = get_market_data DatabaseFields.price currencyPairs |> Map.find DatabaseFields.price           

        let nameToFxRate = name2ForexRate input.forexIndicator input.baseCurrency forexData nameToCurrency date
        let nameToPriorFxRate = name2ForexRate input.forexIndicator input.baseCurrency forexData nameToCurrency priorDate

        let salesDate =  calculateTotalSales true nameToFxRate marketData date instruments    
        let salesPriorDate = calculateTotalSales true nameToPriorFxRate marketData priorDate instruments

        let salesGrowth = periodChange salesDate salesPriorDate instruments

        salesGrowth

            |> produceTestPortfolios input.numberOfPortfolios

    ]





let marginIndexes ( input : FeatureEvaluationInput ) indexes =
    ()
//    let dates = datesAtFrequency input.frequency input.startDate input.endDate
//
//    let indexMembers = getUnion_index_members indexes
//
//    let marginData = get_market_data DatabaseFields.margin indexMembers
//
//    [
//        for date in dates ->
//
//            let dateObj = date |> date2Obj
//
//            let instruments = dateObj |> get_index_members indexes 
//            
//            let knownIndexWeights = indexes |> getWeights_index_members false dateObj 
//            let indexesWithNoWeights = indexes |> Set.filter ( fun i -> Map.containsKey i knownIndexWeights |> not )
//            let indexMemberWeights = 
//                memberWeights date name2FxRate marketData indexConstraints.priceWeightedIndex indexConstraints.cappedIndex indexMembersData indexesWithNoWeights
//                        |> Map.fold ( fun s k l -> Map.add k l s ) knownIndexWeights
//
//            let marginScores = getDbDataForDate marginData date DatabaseFields.margin |> zscore
//
//            ()
//
//     ]

