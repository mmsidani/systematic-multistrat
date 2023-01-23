module SA2.Equity.IndexesPortfolio


open System
open System.IO


open SA2.Common.DbSchema
open SA2.Common.Utils
open SA2.Common.Table_market_data
open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.FxTypes
open SA2.Common.Io
open SA2.Common.Table_sa2_macro_data
open SA2.Common.Table_rate_data



open MacroModels



type GrowthDifferential = {

    rateToTrade : string

    proxyRate : string

    growthRate : string

    conversionRate : string

    realGdpRate : string
}




type RatesPortfolioInput = {

    numeraire : string

    fxConversionRates : YieldConversionRates

    growthDifferential : GrowthDifferential list

    rateField : string

    growthField : string

    proxyRateField : string ;

    conversionRateField : string ;

    gdpField : string ;

    rateDays : int ;

    proxyDays : int ;

    growthQuarters : int ;

    gapQuarters : int ;

    lambda : float ;

    longPositionsFraction : float ;

    outputFile : string
    
}




type RatesPortfolioHistoricalInput = {

    ratesPortfolioInput : RatesPortfolioInput
    
    startDate : int

    endDate : int

    frequency : DateFrequency

}




let ratesData ( input : RatesPortfolioInput ) = 

    let rateNames , proxyNames , growthNames , conversionNames , gdpRateNames = 
    
        input.growthDifferential 
        
            |> List.fold ( fun ( r , p , g , c , gd ) grd -> ( grd.rateToTrade :: r , grd.proxyRate :: p , grd.growthRate :: g , grd.conversionRate :: c , grd.realGdpRate :: gd ) )
        
                ( List.empty , List.empty , List.empty , List.empty , List.empty )
  
    let proxySet , growthSet , conversionSet , gdpRateSet = set proxyNames , set growthNames , set conversionNames , set gdpRateNames // get unique names; rates to trade are not expected to be duplicated

    let foldThem newKey map0 map1 =

        let m1 = Map.find newKey map1

        if Map.containsKey newKey map0 then

            let m0 = Map.find newKey map0
            let combined = Map.fold ( fun s k v -> Map.add k v s ) m0 m1 
            Map.add newKey combined map0

        else

            Map.add newKey m1 map0

    let mutable data = Map.empty
    data <-  [ input.numeraire ] |> get_rate_data |> ( fun m -> Map.add input.conversionRateField m Map.empty )
    data <-  rateNames |> get_market_data DatabaseFields.ebit |> foldThem DatabaseFields.ebit data // ACHTUNG ACHTUNG ACHTUNG hard-coded ebit
    data <-  rateNames |> get_market_data DatabaseFields.enterpriseValue |> foldThem DatabaseFields.enterpriseValue data // ACHTUNG ACHTUNG ACHTUNG hard-coded enterpriseValue
    data <-  rateNames |> get_market_data DatabaseFields.returnOnCapital |> foldThem DatabaseFields.returnOnCapital data // ACHTUNG ACHTUNG ACHTUNG hard-coded returnOnCapital
    data <-  rateNames |> get_market_data DatabaseFields.price |> foldThem DatabaseFields.price data // ACHTUNG ACHTUNG ACHTUNG hard-coded price
    data <- proxySet |> Set.toList |> get_rate_data |> ( fun m -> Map.add input.proxyRateField m Map.empty ) |> foldThem input.proxyRateField data
    data <- growthSet |> Set.toList |> get_market_data input.growthField |> foldThem input.growthField data
    data <- conversionSet |> Set.toList |> get_rate_data |> ( fun m -> Map.add input.conversionRateField m Map.empty ) |> foldThem input.conversionRateField data
    data <- gdpRateSet |> Set.toList |> get_sa2_macro_data |> ( fun m -> Map.add input.gdpField m Map.empty ) |> foldThem input.gdpField data // get_sa2_macro_data does not return the field as a key; i do it manually here

    let rate2Proxy , rate2Growth , rate2Conversion , rate2Gdp = 
        
            ( rateNames , proxyNames ) ||> List.zip |> Map.ofList ,

                ( rateNames , growthNames ) ||> List.zip |> Map.ofList ,

                    ( rateNames , conversionNames ) ||> List.zip |> Map.ofList ,

                        ( rateNames , gdpRateNames ) ||> List.zip |> Map.ofList 
    
    // on output, all data is keyed on the rates names that we want to trade

    ( rateNames , rate2Proxy , rate2Growth , rate2Conversion , rate2Gdp , data )
    

    

let equalWeightsPortfolio longPositionsFraction scores =
    
    scores

        |> Map.toArray
        |> Array.map ( fun ( k , l ) -> l |> List.toArray |> Array.map ( fun n -> ( n, k ) ) )
        |> Array.concat
        |> Array.sortBy ( fun ( _ , k ) -> k )
        
        |> ( fun a -> 

                let num = a.Length / scores.Count
                if num >= 1 then
                    ( a.[ a.Length - num .. a.Length - 1 ] , a.[ 0 .. num-1 ] )
                elif a.Length > 1 then
                    ( [| a.[ a.Length - 1 ] |] , [| a.[ 0 ] |] )
                else 
                    ( Array.empty , Array.empty )
            )
        |> ( fun ( a , b ) -> 

                let lengthA = float a.Length
                let lengthB = float b.Length

                if lengthA <> 0.0 && lengthB <> 0.0 then

                    // NOTE we drop the score next 

                    ( a |> Array.map ( fun ( n , _ ) -> ( n , longPositionsFraction / lengthA ) ) |> Map.ofArray , 
                        b |> Array.map ( fun ( n , _ ) -> ( n , - ( 1.0 - longPositionsFraction ) / lengthB ) ) |> Map.ofArray )
                else

                    ( Map.empty , Map.empty ) )

        ||> Map.fold ( fun s k v -> Map.add k v s )




let buildIndexesPortfolioForDates ( input : RatesPortfolioInput ) dates =

    let ratesToTrade , rate2Proxy , rate2Growth , rate2Conversion , rate2RealGdpRate , data = ratesData input
    
    let sortedDataByRate =

        data |> Map.map ( fun _ m -> m |> Map.map ( fun _ l -> l |> List.sortBy ( fun ( d , _  ) -> -d ) ) ) // Note: decreasing order

    let trimData date = 
    
        sortedDataByRate

            |> Map.map 
            
                ( 
                
                fun _ m -> 

                    m |> Map.map ( fun _ l -> l |> List.filter ( fun ( d , _ ) -> d <= date ) ) 
                )

    let createZeroWeights () =

        ( ratesToTrade , Array.zeroCreate ratesToTrade.Length |> Array.toList ) ||> List.zip |> Map.ofList
            
    [|
        for date in dates ->

            printfn "now doing %d" date

            let trimmedData = trimData date
            
            let scores = macroEquityModel input.lambda input.rateDays input.proxyDays input.growthQuarters input.gapQuarters input.numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate trimmedData ratesToTrade

            if scores <> Map.empty then
                
                (  date  , scores |> equalWeightsPortfolio input.longPositionsFraction ) 

            else
                
                ( date , createZeroWeights () )
                
    |]

    |> Array.rev

    |> outputPortfolioFile ratesToTrade input.outputFile




let buildIndexesPortfolioHistorical ( input : RatesPortfolioHistoricalInput ) =  

    datesAtFrequency input.frequency input.startDate input.endDate |> buildIndexesPortfolioForDates input.ratesPortfolioInput

