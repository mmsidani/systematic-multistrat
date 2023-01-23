module SA2.Equity.IndexMembers

open System

open SA2.Common.Dates
open SA2.Common.DbSchema
open SA2.Common.Utils
open SA2.Common.Io
open SA2.Common.Table_equity_universe
open SA2.Common.Table_static_data
open SA2.Common.Table_instruments
open SA2.Common.Table_index_members
open SA2.Common.ForEx
open SA2.Common.FxTypes
open SA2.Common.Dictionaries

open SA2.MarketData.BloombergReference

open EquityUniverse


let private maxNamesInBloombergRefRequest = 1000


let lookForNoCurrency names =

    try
    
        let currencies = get_static_data DatabaseFields.currency names |> Map.find DatabaseFields.currency |> Map.ofList
        
        names

            |> Seq.filter ( fun n -> currencies.ContainsKey n |> not )

    with

        | KeysNotFound( newNames ) -> newNames |> Set.toSeq

        


let lookForNoGics names =

    try

        let gics = get_static_data DatabaseFields.gicsIndustry names |> Map.find DatabaseFields.gicsIndustry |> Map.ofList

        names

            |> Seq.filter ( fun n -> gics.ContainsKey n |> not )

    with

        | KeysNotFound( newNames ) -> newNames |> Set.toSeq




let partitionForNoFundTicker names =

    try

        let fundTickers = getFundamentalTicker_static_data names

        (

        names

            |> Seq.filter ( fun n -> fundTickers.ContainsKey n |> not ) // return will have both notKnown and known without fundamental tickers

        , fundTickers
        
        )

    with

        | e -> raise( e )


        
              
let lookForNoDescription names =

    try

        names |> set |> getDescription_instruments |> ignore

        Seq.empty // if any name was not known already, an exception would have been thrown

    with

        | KeysNotFound ( newNames ) -> newNames |> Set.toSeq




let loadCurrencyPairs currencyPairs =
    
    if Array.length currencyPairs <> 0 then

        currencyPairs

            |> Array.map ( fun cc -> ( cc , cc ) )
            |> Map.ofArray
            |> update_instruments 
            |> ignore



let loadIndexMembers input indexMembers newTicker2ShortName =
    
    if List.length indexMembers <> 0 then
        
        indexMembers 
        |> set
        |> Set.toList
        |> List.map ( fun ( d , i , u , w ) -> ( d , i , u , w ) ) |> update_index_members newTicker2ShortName  // Note: this call also makes sure all instruments (stocks and indices are known to tbl_instruments)



    
let loadStaticData ( input : EquityUniverseInput ) ( fieldDictionary : StaticDataFieldDictionary ) sourceName nameToCurrency nameToGics nameToFundTicker nameToDescription =
      
    if not ( Map.isEmpty nameToCurrency ) then 
    
        nameToCurrency |> Map.toList |> List.unzip ||> update_static_data sourceName input.currencyField ( fieldDictionary.mapIt( input.currencyField ) ) nameToDescription
        
    if not ( Map.isEmpty nameToGics ) then

        nameToGics |> Map.toList |> List.unzip ||> update_static_data sourceName input.gicsField ( fieldDictionary.mapIt( input.gicsField ) ) nameToDescription

    if  not ( Map.isEmpty nameToFundTicker ) then

        let fundTickerIds = nameToFundTicker |> valueSet |> getKey_instruments // Note: recall we load to tbl_instruments before we get here

        nameToFundTicker

            |> Map.map ( fun _ v -> ( Map.find v fundTickerIds ).ToString() )
            |> Map.toList 
            |> List.unzip 
            ||> update_static_data sourceName input.fundamentalTickerField ( fieldDictionary.mapIt( input.fundamentalTickerField ) ) nameToDescription




let convertIcb2Gics ( icbGicsDictionary : Icb2GicsConverter ) name2Icb  =

    let icb2Gics = icbGicsDictionary.getMapIcb2Gics()

    let name2Gics =
    
        name2Icb

            |> Map.map ( fun _ i -> 

                            if Map.containsKey i icb2Gics then
                        
                                Some ( Map.find i icb2Gics )

                            else

                                None

                        )

    let withGics , noGics = name2Gics |> Map.partition ( fun _ v -> v.IsSome )

    if noGics.Count <> 0 then

        raise( Exception ( sprintf "Some ICB numbers are not known: \n%s" ( noGics |> keySet |> Set.toList |> List.map ( fun n -> ( n , Map.find n name2Icb ) ) |> paste ) ) )

    withGics

        |> Map.map ( fun _ v -> v.Value )

        


let private loopOverBloombergReferenceRequest names field date = 

    let mutable counter = 0

    let namesArray = names |> List.toArray // unfortunately bloombergReferenceRequest takes List's

    let mutable ret = Array.empty

    while counter < names.Length do

        let tempNames = namesArray.[ counter .. min ( counter + maxNamesInBloombergRefRequest - 1 ) ( names.Length - 1 ) ] |> Array.toList

        let tempRet = bloombergReferenceRequest tempNames field date 
        
        counter <- counter + maxNamesInBloombergRefRequest

        ret <- Array.append ret tempRet

    ret




let private updateIndexes dates ( input : IndexMembersInput ) ( forex : ForexDataFields ) ( icbGicsDictionary : Icb2GicsConverter ) =

    let today = [ DateTime.Today ] |> shiftWeekends |> List.head |> obj2Date

    let swLog = new System.IO.StreamWriter ( input.logFilePath + today.ToString() + ".indexData.csv" )

    let logIt x  = logBBOutput swLog x

    let indexes = input.indexes

    let indexMembers =   

        [|
       
            for date in dates ->
                bloombergIndexMembersReferenceRequest indexes date
                |> logBBIndexMembersOutput swLog
        |]

        |> Array.concat

        |> Array.map ( fun ( d , i , s , w ) -> ( d , i , s + " " + input.equityUniverseInput.equityClassIndicator , w ) )

    let stocks = indexMembers |> Array.map ( fun ( _ , _ , n , _ ) -> n  ) |> Array.toList

    let unknownStocks = lookForNoDescription stocks |> set
    let knownStocks = stocks |> List.filter ( fun s -> Set.contains s unknownStocks |> not ) |> set
    let unknownIndexes = lookForNoDescription indexes |> set
    let knownIndexes = indexes |> List.filter ( fun i -> Set.contains i unknownIndexes |> not )

    // fund ticker

    let namesWithNoFundTicker , names2FundTicker = partitionForNoFundTicker knownStocks

    let allNamesWithNoFundTicker = [ namesWithNoFundTicker |> set ; unknownStocks ] |> Set.unionMany |> Set.toList

    let stock2FundTicker =

        loopOverBloombergReferenceRequest allNamesWithNoFundTicker input.equityUniverseInput.fundamentalTickerField today
            |> logIt
            |> Array.map ( fun ( _ , n , v ) -> ( n , v + " " + input.equityUniverseInput.equityClassIndicator ) )
            |> Map.ofArray 
            |> List.foldBack ( fun n s -> if Map.containsKey n s |> not then Map.add n n s else s ) allNamesWithNoFundTicker // names for which we did not obtain fund tickers are their own fund tickers
            |> Map.fold ( fun s k v -> Map.add k v s ) names2FundTicker // add to the known stocks with known fund tickers

    let fundTickersValues = stock2FundTicker |> valueSet
    let unknownFundTickers = lookForNoDescription fundTickersValues |> set

    // short name

    let ticker2ShortName = 
        loopOverBloombergReferenceRequest ( [ unknownStocks ; unknownIndexes ; unknownFundTickers ] |> Set.unionMany |> Set.toList ) input.equityUniverseInput.shortNameField today 
            |> logIt
            |> Array.map ( fun ( _ , n , v ) -> ( n , v ) ) |> Map.ofArray

    let stock2FundTicker =
    
        // why this next map? for a few hungarian securities the fundamental ticker was misspelled with no space between security name and exchange code and bloomberg returned no information about the (misspelled) fundamental ticker

        stock2FundTicker
            |> Map.map ( fun n f -> 
                            if Set.contains f unknownFundTickers then
                                if Map.containsKey f ticker2ShortName then f else n 
                            else f
                        )
    
    // now reset fundTickersValues since stock2FundTicker might have been modified

    let fundTickersValues = stock2FundTicker |> valueSet 
    let unknownFundTickers = lookForNoDescription fundTickersValues |> set
             
    let knownFundTickers = fundTickersValues |> Set.filter ( fun t -> Set.contains t unknownFundTickers |> not )

    // Ccy

    let instrumentsWithNoCcy = [ Seq.append knownFundTickers knownIndexes |> lookForNoCurrency |> Set.ofSeq ; unknownIndexes ; unknownFundTickers ] |> Set.unionMany |> Seq.toList
    let ticker2Currency = loopOverBloombergReferenceRequest  instrumentsWithNoCcy input.equityUniverseInput.currencyField today |> logIt |> Array.map ( fun ( _ , n , v ) -> ( n , v ) ) |> Map.ofArray

    // Gics

    let namesWithNoGics = [ knownFundTickers |> lookForNoGics |> Set.ofSeq ; unknownFundTickers ] |> Set.unionMany |> Set.toList
    let ticker2Gics = loopOverBloombergReferenceRequest  namesWithNoGics input.equityUniverseInput.gicsField today |> logIt |> Array.map ( fun ( _ , n , v ) -> ( n , v ) )  |> Map.ofArray

    let namesStillWithNoGics =
        namesWithNoGics
            |> List.filter ( fun n -> Map.containsKey n ticker2Gics |> not )
    let ticker2Icb = 
        loopOverBloombergReferenceRequest  namesStillWithNoGics input.equityUniverseInput.icbField today 
            |> logIt 
            |> Array.map ( fun ( _ , n , v ) -> ( n , v ) ) 
            |> Map.ofArray
            |> convertIcb2Gics icbGicsDictionary

    swLog.Close()

    let processedIndexMembers = // map index members to fundamental tickers and eliminate duplicates
        indexMembers 
            |> Array.map ( fun ( d , i , n , w ) -> ( ( d , i , Map.find n stock2FundTicker ) , w ) ) 
            |> Map.ofArray // so which weight is picked when there are duplicates is indeterminate
            |> Map.toArray
            |> Array.map ( fun ( ( d , i , n ) , w ) -> ( d , i , n , w  ) ) 
        
    ( processedIndexMembers ,
        ticker2Currency , 
            Map.fold ( fun s k v -> Map.add k v s ) ticker2Icb ticker2Gics , 
                stock2FundTicker , 
                    ticker2ShortName )




let updateIndexMembers ( input : IndexMembersInput ) ( forex : ForexDataFields ) ( fieldDictionary : StaticDataFieldDictionary ) ( icbGicsDictionary : Icb2GicsConverter ) = 
 
    let dates = [ DateTime.Today |> obj2Date ]

                    |> List.map ( fun d -> date2Obj d )
                    |> shiftWeekends
                    |> Seq.map ( fun d -> obj2Date d )

    let indexMembers , newNamesToCurrencies , newNamesToGics , newNamesToFundTickers , newNamesToDescription = updateIndexes dates input forex icbGicsDictionary
    
    loadIndexMembers input.equityUniverseInput ( indexMembers |> Array.toList ) newNamesToDescription

    loadCurrencyPairs ( newNamesToCurrencies |> valueSet |> Set.toList |> formCurrencyPairs forex.forexIndicator input.equityUniverseInput.baseCurrency |> List.toArray )

    loadStaticData input.equityUniverseInput fieldDictionary input.equityUniverseInput.sourceName newNamesToCurrencies newNamesToGics newNamesToFundTickers newNamesToDescription
 



let updateIndexMembersHistorical ( input : IndexMembersHistoricalInput ) ( forex : ForexDataFields ) ( fieldDictionary : StaticDataFieldDictionary ) ( icbGicsDictionary : Icb2GicsConverter ) = 

    let dates = datesAtFrequency input.frequency input.startDate input.endDate

                    |> Array.map ( fun d -> date2Obj d )
                    |> Array.toList
                    |> shiftWeekends
                    |> Seq.map ( fun d -> obj2Date d )

    let indexMembers , newNamesToCurrencies , newNamesToGics , newNamesToFundTickers , newNamesToDescription = updateIndexes dates input.indexMembersInput forex icbGicsDictionary
    
    loadIndexMembers input.indexMembersInput.equityUniverseInput ( indexMembers |> Array.toList ) newNamesToDescription

    loadCurrencyPairs ( newNamesToCurrencies |> valueSet |> Set.toList |> formCurrencyPairs forex.forexIndicator input.indexMembersInput.equityUniverseInput.baseCurrency |> List.toArray )

    loadStaticData input.indexMembersInput.equityUniverseInput fieldDictionary input.indexMembersInput.equityUniverseInput.sourceName newNamesToCurrencies newNamesToGics newNamesToFundTickers newNamesToDescription
