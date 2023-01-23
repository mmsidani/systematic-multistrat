module SA2.Equity.FeaturesNames


open System


open SA2.Common.Utils
open SA2.Common.ForEx
open SA2.Common.DbSchema

open EquityMetrics
open EquityMetricsOps
open FeatureType




let marginFeature date marketData instruments =

    getDbDataForDate marketData date DatabaseFields.margin

        |> Map.filter ( fun k _ -> Set.contains k instruments )




let profitabilityFeature useConversion baseCurrency conversionData name2Currency date marketData instruments = 

    calculateProfitability useConversion baseCurrency marketData conversionData date instruments name2Currency




let marketShareFeature name2Gics name2FxRate date marketData instruments =

    calculateMarketShares marketData date name2Gics name2FxRate instruments



let evEbitdaFeature useConversion baseCurrency conversionData name2Currency date marketData instruments =

    calculateEvEbitdaMultiple useConversion baseCurrency marketData conversionData date instruments name2Currency




let evEbitFeature useConversion baseCurrency conversionData name2Currency date marketData instruments =

    calculateEvEbitMultiple useConversion baseCurrency marketData conversionData date instruments name2Currency


let combineFeatures featureTypes ( combiner : Map<string , float> list -> Map<string , float> ) ( ranker : Map<string , float> -> Map< string , float > ) 

                forexIndicator baseCurrency date priorDate staticData marketData forexData conversionData name2Gics name2Currency instruments =


    let name2FxRate = name2ForexRate forexIndicator baseCurrency forexData name2Currency date
    let name2PriorFxRate = name2ForexRate forexIndicator baseCurrency forexData name2Currency priorDate

    let mutable qualityFeatures = List.empty
    let mutable valuationFeatures = List.empty

    let featureTypesSet = set featureTypes

    // market share

    if featureTypesSet.Contains FeatureType.marketShare then

        qualityFeatures <- marketShareFeature name2Gics name2FxRate date marketData instruments :: qualityFeatures
    
    // margins

    if featureTypesSet.Contains FeatureType.margin then

        qualityFeatures <- marginFeature date marketData instruments :: qualityFeatures

    // profitability

    if featureTypesSet.Contains FeatureType.profitability then

        qualityFeatures <- profitabilityFeature false baseCurrency conversionData name2Currency date marketData instruments :: qualityFeatures

    // ebitda / ev

    if featureTypesSet.Contains FeatureType.evEbitda then

        valuationFeatures <- evEbitdaFeature false baseCurrency conversionData name2Currency date marketData instruments :: valuationFeatures

    // ebit / ev . for indexes for example

    if featureTypesSet.Contains FeatureType.evEbit then

        valuationFeatures <- evEbitFeature false baseCurrency conversionData name2Currency date marketData instruments :: valuationFeatures

    let qualityScores = if qualityFeatures.IsEmpty |> not then combiner qualityFeatures else Map.empty
    let valuationScores = if valuationFeatures.IsEmpty |> not then combiner valuationFeatures else Map.empty

    let combinedScores = 

            if [ qualityScores ; valuationScores ] |> List.forall ( fun m -> m.IsEmpty |> not ) then

                combiner [ qualityScores ; valuationScores ]

            elif qualityScores.IsEmpty |> not then

                qualityScores

            elif valuationScores.IsEmpty |> not then

                valuationScores

            else

                raise( Exception( "no scores whatsoever" ) )

    combinedScores

        |> ranker

        |> value2Keys

        |> Map.map ( fun _ l -> l |> List.map ( fun n -> ( n , Map.find n combinedScores ) ) )