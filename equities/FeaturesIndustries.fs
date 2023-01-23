module SA2.Equity.FeaturesIndustries


open SA2.Common.Math
open SA2.Common.Utils
open SA2.Common.ForEx
open SA2.Common.DbSchema


open EquityMetrics
open EquityMetricsOps
open RelativeShares




let sizeFragGrowth ( combiner : Map<'a , float> list -> Map<'a , float> ) ( ranker : Map<'a , float> -> Map< 'a , 'b > ) 

            forexIndicator baseCurrency date priorDate staticData marketData forexData globalData nameToGics nameToCurrency instruments =

    // industry size, fragmentation, growth

    let nameToFxRate = name2ForexRate forexIndicator baseCurrency forexData nameToCurrency date
    
    let marketShares =  calculateMarketShares marketData date nameToGics nameToFxRate instruments
    
    let industrySize  = calculateGlobalIndustrySize globalData date
    
    let allGics =

        instruments

        |> List.map ( fun i -> Map.find i nameToGics )

        |> set
    
    let globalSales = getDbDataForDate globalData date DatabaseFields.salesPerShare
    
    let priorGlobalSales = getDbDataForDate globalData priorDate DatabaseFields.salesPerShare
    
    let globalSalesGrowth = periodChange globalSales priorGlobalSales allGics    
       
    let herfindahls = herfindahlIndex marketShares nameToGics
    
    let scores = [ industrySize ; herfindahls ; globalSalesGrowth ]
    
    scores 
    
        |> combiner  
        |> ranker
        |> value2Keys

    


let fill2By2 inflationRate salesGrowth globalSales ( insts : string list list ) ( fieldValues : Map< string , string > list ) = 

    let mutable ggList , sgList , gsList , ssList = List.empty , List.empty , List.empty , List.empty
    
    for i in 0 .. insts.Length - 1 do

        let gSalesGrowth = Map.find ( Map.find "gics" fieldValues.[ i ] ) globalSales
                        
        for inst in insts. [ i ] do 

            let iSalesGrowth = Map.find inst salesGrowth 

            if iSalesGrowth > inflationRate && gSalesGrowth > inflationRate then

                ggList <- inst :: ggList

            elif iSalesGrowth > inflationRate && gSalesGrowth < inflationRate then

                sgList <- inst :: sgList

            elif iSalesGrowth < inflationRate && gSalesGrowth > inflationRate then

                gsList <- inst :: gsList

            else

                ssList <- inst :: ssList

    [ ( "GG", ggList ) ; ( "SG" , sgList ) ; ( "GS" , gsList ) ; ( "SS" , ssList ) ]

        |> Map.ofList