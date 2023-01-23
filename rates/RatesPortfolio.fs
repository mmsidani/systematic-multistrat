module SA2.Rates.RatesPortfolio

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
open SA2.Common.Table_static_data
open SA2.Common.Table_instruments



open Rates
open GrowthModels


type RateTradingPacket = {

    rateToTrade : string

    proxyRate : string

    growthDifferentialRate : string

    conversionRate : string

    outputGapRate : string

    cpiRate : string

}




type RatesPortfolioInput = {

    numeraire : string ;

    annualizeGrowthRate : bool ;

    useReal : bool ;
    
    rateTradingPackets : RateTradingPacket list ;

    rateField : string ;

    rateTable : string ;

    useSa2SymbolForRate : bool ;

    growthDifferentialField : string ;

    growthDifferentialTable : string ;

    proxyRateField : string ;

    proxyRateTable : string ;

    useSa2SymbolForProxy : bool ;

    conversionRateField : string ;

    conversionRateTable : string ;

    gdpField : string ;

    gdpTable : string ;

    cpiField : string ; 

    cpiTable : string ;

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




let getDbFunction field table = 

        if table = "tbl_market_data" then
            get_market_data field >> Map.find field
        elif table = "tbl_rate_data" then
            List.map ( fun r -> r + ".er") >> get_rate_data >> Map.toList >> List.map ( fun ( r , l ) -> ( r.Replace( ".er" , "" ) , l ) ) >> Map.ofList /// >> ( fun m -> Map.add field m Map.empty )
        elif table = "tbl_sa2_macro_data" then
            get_sa2_macro_data /// >> ( fun m -> Map.add field m Map.empty )
        else
            raise ( Exception ( "don't know about table " + table ) )





let private removePipes names =

    names 
        |> List.map ( fun ( r : string ) ->
                        if r.Contains( "|" ) then
                            r.Split [| '|' |] |> Array.toList
                        else
                            [ r ]
                        )


    

let convertSymbols2Tickers names =

    let namesSplits = removePipes names

    let symbol2Ticker = namesSplits |> List.concat |> Set.ofList |> getTickerFromSymbol_instruments

    namesSplits
        |> List.map ( fun l -> 
                            if l.Length = 2 then
                                Map.find l.[ 0 ] symbol2Ticker + "|" + Map.find l.[ 1 ] symbol2Ticker
                            else
                                Map.find l.[ 0 ] symbol2Ticker
                )




let calcYields rate2PayFreq rate2Tenor input rateNames =

    let forwards = rateNames |> List.filter ( fun ( r : string ) -> r.Contains "|" )
    let spots = rateNames |> List.filter ( fun ( r : string ) -> r.Contains "|" |> not )
       
    let mutable ret = Map.empty

    if spots.Length <> 0 then

        ret <- getDbFunction input.rateField input.rateTable spots |> ( if Map.isEmpty rate2PayFreq |> not then annualizeRate rate2PayFreq else ( fun m -> m  ) )

    if forwards.Length <> 0 then

        let fwd2Longs , fwd2Shorts = 
        
            forwards

                |> List.map ( fun s -> 

                                let a = s.Split '|' 
                                ( ( s , a.[ 0 ] ) , ( s , a.[ 1 ] ) )
                            )
                                
                |> List.unzip

                ||> ( fun l s -> ( Map.ofList l , Map.ofList s ) )

        let longData = fwd2Longs |> valueSet |> Set.toList |> ( getDbFunction input.rateField input.rateTable ) 
        let shortData = fwd2Shorts |> valueSet |> Set.toList |> ( getDbFunction input.rateField input.rateTable )
         
        let fwdData =

            forwards

                |> List.map ( fun f ->

                            
                                    let longName = Map.find f fwd2Longs
                                    let shortName = Map.find f fwd2Shorts

                                    if not rate2PayFreq.IsEmpty && rate2Tenor |> ( Map.isEmpty >> not ) then

                                        let payFreqLong = Map.find longName rate2PayFreq
                                        let payFreqShort = Map.find shortName rate2PayFreq
                                        let matLongInst = Map.find longName rate2Tenor
                                        let matShortInst = Map.find shortName rate2Tenor
                                        if matLongInst <= matShortInst then raise( Exception "the longer maturity instrument has a shorter maturity" )

                                        ( f , forwardRates payFreqLong payFreqShort matLongInst matShortInst ( Map.find longName longData ) ( Map.find shortName shortData ) ) 

                                    else

                                        let alignedRates = Map.add "long" ( Map.find longName longData ) Map.empty |> Map.add "short" ( Map.find shortName shortData ) |> alignOnSortedDates

                                        ( f , ( Map.find "long" alignedRates ,  Map.find "short" alignedRates ) ||> List.map2 ( fun ( d , l ) ( _ , s ) -> ( d , l - s ) ) ) )
                                    
                |> Map.ofList
                                

        ret <- Map.fold ( fun s k l -> Map.add k l s ) ret fwdData

    ret
    




let ratesData ( input : RatesPortfolioInput ) = 

    let rateNames , proxyNames , growthNames , conversionNames , gdpRateNames , cpiRateNames  = 
    
        input.rateTradingPackets 
        
            |> List.fold ( fun ( r , p , g , c , gd , cpi ) grd -> ( grd.rateToTrade :: r , grd.proxyRate :: p , grd.growthDifferentialRate :: g , grd.conversionRate :: c , grd.outputGapRate :: gd , grd.cpiRate :: cpi ) )
        
                ( List.empty , List.empty , List.empty , List.empty , List.empty , List.empty )

    let rateNames =
        if input.rateTable = "tbl_market_data" && input.useSa2SymbolForRate then
            convertSymbols2Tickers rateNames
        else
            rateNames

    let proxyNames =
        if input.proxyRateTable = "tbl_market_data" && input.useSa2SymbolForProxy then
            convertSymbols2Tickers proxyNames
        else
            proxyNames

    let proxySet , growthSet , conversionSet , gdpRateSet , cpiRateSet = set proxyNames , set growthNames , set conversionNames , set gdpRateNames , set cpiRateNames // get unique names; rates to trade are not expected to be duplicated

    let foldThem newKey map0 map1 =

//        let m1 = Map.find newKey map1

        if Map.containsKey newKey map0 then

            let m0 = Map.find newKey map0
            let combined = Map.fold ( fun s k v -> Map.add k v s ) m0 map1 
            Map.add newKey combined map0

        else

            Map.add newKey map1 map0

    let rate2PayFreq =
        if input.rateTable = "tbl_market_data" then
            rateNames |> removePipes |> List.concat |> get_static_data DatabaseFields.fixedPayFreq |> Map.find DatabaseFields.fixedPayFreq |> List.map ( fun ( n , freq ) -> ( n , convertStringFrequency freq ) ) |> Map.ofList
        else
            Map.empty

    let rate2Tenor =
        if input.rateTable = "tbl_market_data" then
            rateNames |> removePipes |> List.concat |> get_static_data DatabaseFields.tenor |> Map.find DatabaseFields.tenor |> List.map ( fun ( n , tenor ) -> ( n , convertStringTenor tenor ) ) |> Map.ofList
        else
            Map.empty

    let mutable data = Map.empty

    // NOTE: the code below expects specific field names. we have the freedom to choose any data field in the input file but here we rename them to what is expected in the called code (for example, module GrowthModels )

    data <- [ input.numeraire ] |> get_rate_data |> ( fun m -> Map.add DatabaseFields.yld m Map.empty )
    data <- rateNames |> calcYields rate2PayFreq rate2Tenor input |> foldThem DatabaseFields.yld data
    data <- proxySet |> Set.toList |> ( getDbFunction input.proxyRateField input.proxyRateTable ) |> foldThem DatabaseFields.yld data
    data <- growthSet |> Set.toList |> ( getDbFunction input.growthDifferentialField input.growthDifferentialTable ) |> foldThem DatabaseFields.nominalGrowthRate data
    data <- conversionSet |> Set.toList |> ( getDbFunction input.conversionRateField input.conversionRateTable ) |> foldThem DatabaseFields.yld data
    data <- gdpRateSet |> Set.toList |> ( getDbFunction input.gdpField input.gdpTable ) |> foldThem DatabaseFields.realGrowthRate data 
    data <- cpiRateSet |> Set.toList |> ( getDbFunction input.cpiField input.cpiTable ) |> foldThem DatabaseFields.cpiRate data 
    
    let rate2Proxy , rate2Growth , rate2Conversion , rate2Gdp , proxyRate2Cpi = 
        
            ( rateNames , proxyNames ) ||> List.zip |> Map.ofList ,

                ( rateNames , growthNames ) ||> List.zip |> Map.ofList ,

                    ( rateNames , conversionNames ) ||> List.zip |> Map.ofList ,

                        ( rateNames , gdpRateNames ) ||> List.zip |> Map.ofList ,

                            ( proxyNames , cpiRateNames ) ||> List.zip |> Map.ofList 
    
    ( rateNames , rate2Proxy , rate2Growth , rate2Conversion , rate2Gdp , proxyRate2Cpi , data )


    

let minMaxRankPortfolio longPositionsFraction scores =
    
    let ranks = scores |> keySet

    let minRank , maxRank = ranks |> Set.toList |> List.sort |> ( fun l -> ( l.[ 0 ] , l.[ l.Length - 1 ] ) )

    let longs = Map.find maxRank scores

                    |> ( fun l ->
                            let length = List.length l |> float
                            l |> List.map ( fun n -> ( n , longPositionsFraction / length ) )
                        )
                    |> Map.ofList

    let shorts = Map.find minRank scores

                    |> ( fun l ->
                            let length = List.length l |> float
                            l |> List.map ( fun n -> ( n , - ( 1.0 - longPositionsFraction ) / length ) )
                        )

                    |> Map.ofList
           
    ( longs , shorts ) ||> Map.fold ( fun s k v -> Map.add k v s )   




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




let buildRatesPortfolioForDates ( input : RatesPortfolioInput ) dates =

    let ratesToTrade , rate2Proxy , rate2Growth , rate2Conversion , rate2RealGdpRate , proxyRate2Cpi , data = ratesData input

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
            
            let scores = modelGrowthDifferential input.useReal input.annualizeGrowthRate input.lambda input.rateDays input.proxyDays input.growthQuarters input.gapQuarters input.numeraire date rate2Proxy rate2Growth rate2Conversion rate2RealGdpRate proxyRate2Cpi trimmedData ratesToTrade

            if scores <> Map.empty then
                
                (  date  , scores |> equalWeightsPortfolio input.longPositionsFraction ) /// equalWeightsPortfolio ) 

            else
                
                ( date , createZeroWeights () )
                
    |]

    |> Array.rev

    |> outputPortfolioFile ratesToTrade input.outputFile




let buildRatesPortfolioHistorical ( input : RatesPortfolioHistoricalInput ) =  

    datesAtFrequency input.frequency input.startDate input.endDate |> buildRatesPortfolioForDates input.ratesPortfolioInput




let buildRatesPortfolio ( input : RatesPortfolioInput ) = 

    [| obj2Date DateTime.Today |] |> buildRatesPortfolioForDates input
