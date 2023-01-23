module SA2.Equity.MatrixModels


open System

open SA2.Common.Utils
open SA2.Common.Data
open SA2.Common.Dates
open SA2.Common.Math
open SA2.Common.ForEx
open SA2.Common.DbSchema

open StaticPartitions
open EquityMetrics
open EquityMetricsOps
open RelativeShares
open FeaturesIndustries
open FeaturesNames



let private growthPeriod = 3

let private gicsName = "gicsIndustry"

let private currencyName = "currency"

let private quantileProbs = [| 0.2 ; 0.4 ; 0.6 ; 0.8 ; 1.0 |] |> Array.sort // to stress the point

let private gicsQuantileProbs = [| 1.0 / 3.0 ; 2.0 / 3.0 ; 1.0 |] |> Array.sort // to stress the point




let model2By2Equities ( ranker : Map< string , ( string * string ) list > -> Map< string, Map< string , ( int * float ) list > > -> Map< string, string list > -> Map< string , float> ) 

        forexIndicator baseCurrency date forexData staticData marketData globalData instruments =

    let insts , fieldValues = 
    
        match List.isEmpty [ gicsName ]
        
            with
            
                | true -> [ instruments ] , [ Map.empty ]
                | false -> partitionByStatic staticData [ gicsName ] instruments

    let allGics = 
    
        fieldValues 
    
            |> List.map ( 
                    
                        fun m -> 
                                                                        
                            Map.find gicsName m 
                        ) 
                                
            |> set 
            |> Set.toList

    let globalSales = getDbDataForDate globalData date DatabaseFields.salesPerShare 

    let priorDate = ( date |> date2Obj ).AddMonths( - growthPeriod ) |> obj2Date
    let priorGlobalSales = getDbDataForDate globalData priorDate DatabaseFields.salesPerShare 

    let globalSalesGrowth = periodChange globalSales priorGlobalSales allGics

    let inflationRate = globalInflation growthPeriod globalData date 
        
    let nameToCurrency = Map.find currencyName staticData |> Map.ofList
    let nameToFxRate = name2ForexRate forexIndicator baseCurrency forexData nameToCurrency date
    
    let salesDate =  calculateTotalSales true nameToFxRate marketData date instruments
    let salesPriorDate = calculateTotalSales true nameToFxRate marketData date instruments
    let salesGrowth = periodChange salesDate salesPriorDate instruments

    fill2By2 inflationRate salesGrowth globalSales insts fieldValues

        |> ranker staticData marketData 




let model3By3Equities featureTypes forexIndicator baseCurrency date forexData conversionData staticData marketData globalData instruments =
    
    let staticFields = [ gicsName ]    

    let priorDate = ( date |> date2Obj ).AddMonths( - growthPeriod ) |> obj2Date

    let partitionedSecs , _ = partitionByStatic staticData staticFields instruments 

    let nameToCurrency = Map.find currencyName staticData |> Map.ofList    
    let nameToGics = Map.find gicsName staticData |> Map.ofList

    // industries

    let gicsRanker = rankByQuantiles gicsQuantileProbs

    let hGics , lGics = sizeFragGrowth averageZScore gicsRanker forexIndicator baseCurrency date priorDate staticData marketData forexData globalData nameToGics nameToCurrency instruments

                            |> ( fun m -> Map.find 3.0 m |> set , Map.find 1.0 m |> set )

    let hNames , lNames = 

        nameToGics |> Map.filter ( fun _ v -> hGics.Contains v ) |> keySet , 
    
            nameToGics |> Map.filter ( fun _ v -> lGics.Contains v ) |> keySet

    let singleNameRanker = rankByQuantiles quantileProbs

    let longNames = combineFeatures featureTypes averageZScore singleNameRanker forexIndicator baseCurrency date priorDate staticData marketData forexData conversionData nameToGics nameToCurrency hNames

                         |> ( fun m -> 
                                let floatLength = quantileProbs.Length |> float

                                match  floatLength |> m.ContainsKey  // max score

                                    with

                                    | true -> Map.find floatLength m 

                                    | false -> List.empty )
                                
    let shortNames = combineFeatures featureTypes averageZScore singleNameRanker forexIndicator baseCurrency date priorDate staticData marketData forexData conversionData nameToGics nameToCurrency lNames

                         |> ( fun m -> 

                                match m.ContainsKey 1.0

                                    with

                                    | true -> Map.find 1.0 m 

                                    | false -> List.empty )

    longNames , shortNames 
