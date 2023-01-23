module SA2.MarketData.BbUpdate

open System

open SA2.Common.Utils
open SA2.Common.DbSchema
open SA2.Common.Io
open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.ForEx
open SA2.Common.Table_equity_universe
open SA2.Common.Table_index_members
open SA2.Common.Table_static_data
open SA2.Common.Table_market_data
open SA2.Common.Table_instruments
open SA2.Common.Dictionaries
open SA2.Common.FxTypes

open BloombergHistorical


let private maxNamesInBloombergHistRequest = 2000



type BbDataTypes =

    | equity = 0
    | equityIndexes = 1
    | rates = 3




type EquitySingleNameData = 

    {

    singleNameMarketDataFields : string list

    forExDataField : string list

    conversionRateDataField : string list

    baseCurrencyFxPairs : string

    forexIndicator : string

    }




type GlobalData = 

    {

    globalDataNamesGics : ( string * string ) list

    globalDataFields : string list

    }




type EquityIndexData =

    {

    indexUniverse : string list

    indexMarketDataFields : string list

    underlyingDataFields : EquitySingleNameData

    }




type BbDataInput =

    {

    sourceName : string ;

    endDate : int ;

    numberOfDays : int ;

    frequency : DateFrequency ;

    logFilePath : string

    }




let private checkFields ( errorSw : IO.StreamWriter ) requestedTickers returnedTickers field =

    let retTickerSet = set returnedTickers

    let notFound = 

        requestedTickers

            |> List.filter ( fun t -> retTickerSet.Contains t |> not )

    if notFound.Length <> 0 then

        errorSw.WriteLine( notFound |> paste |> sprintf "\n\n\nNo %s data was returned for: \n%s" field )




let private verifyGlobalInput ( globalInput : GlobalData ) = 

    // we specify a mapping in the input here and we already assume we have the same mapping in the DB. Error-prone, therefore we do this check here

    let names , gics = globalInput.globalDataNamesGics |> List.unzip

    let names2GicsInput = globalInput.globalDataNamesGics |> Map.ofList

    // TODO fix this 
    let names2Gics = gics |> set |> getDescription_instruments |> Map.toList |> List.map ( fun ( g , d ) -> ( d , g) ) |> Map.ofList

    let mismatches = List.filter ( fun n -> Map.find n names2GicsInput <> Map.find n names2Gics ) names

    if mismatches.Length <> 0  then

        let errorText = List.reduce ( fun n1 n2 -> n1 + "," + n2 ) mismatches

        raise( Exception ( "these names do not match their GICS in tbl_instruments: " + errorText ) )
    



let private updateGlobalMarketData ( errorSw : IO.StreamWriter ) ( logFile : string ) sourceName startDate endDate frequency names2Aliases fields ( dataFieldDictionary : DataFieldDictionary ) =

    let logSw = new IO.StreamWriter( logFile )

    let logIt x = logBBOutput logSw x 

    // what's the alias? in the market data db table, we substitute the Gics for the name of the global index we're using, for example

    let names = names2Aliases |> Map.fold ( fun s k _ -> k :: s ) List.empty
 
    for f in fields do
        
        let bbField = dataFieldDictionary.mapIt( f )

        let dateTickerValues  = 
        
            bloombergHistoricalRequest names bbField ( frequency.ToString() ) false ( startDate.ToString() ) ( endDate.ToString() )

                |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
                |> selectUniqueDataValues 
                |> logIt      

        checkFields errorSw names ( dateTickerValues |> Array.map ( fun ( _ , t , _ ) -> t ) ) bbField

        let aliases = dateTickerValues |> Array.map ( fun ( d , t , v ) -> ( d , Map.find t names2Aliases , v ) )
        updateExisting_market_data sourceName f ( dataFieldDictionary.divisor( f ) ) aliases

    logSw.Close()
    



let private updateMarketData ( errorSw : IO.StreamWriter ) ( logFile : string ) ( dataFieldDictionary : DataFieldDictionary ) sourceName startDate endDate frequency names fields =

    let logSw = new IO.StreamWriter( logFile )

    let logIt x = logBBOutput logSw x 

    let mutable counter = 0

    let namesArray = names |> List.toArray // unfortunately bloombergHistoricalRequest takes List's
    
    let field2ExceptionNames , exceptionNames2Divisor = dataFieldDictionary.allDivisorExceptions ( )

    while counter < names.Length do

        let tempNames = namesArray.[ counter .. min ( counter + maxNamesInBloombergHistRequest - 1 ) ( names.Length - 1 ) ] |> Array.toList

        for f in fields do

                let bbField = dataFieldDictionary.mapIt( f )

                bloombergHistoricalRequest tempNames bbField ( frequency.ToString() ) false ( startDate.ToString() ) ( endDate.ToString() )
                
                    |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
                    |> selectUniqueDataValues 
                    |> logIt
                    |> ( fun dateTickerValues -> checkFields errorSw names ( dateTickerValues |> Array.map ( fun ( _ , t , _ ) -> t ) ) bbField ; dateTickerValues )
                    |> ( fun a -> 
                            if field2ExceptionNames.ContainsKey f |> not then
                                a |> updateExisting_market_data sourceName f ( dataFieldDictionary.divisor ( f ) )
                            else
                                let aExNames , aNoExNames = a |> Array.partition ( fun ( _ , n , _ ) -> exceptionNames2Divisor.ContainsKey n  )
                                aNoExNames |> updateExisting_market_data sourceName f ( dataFieldDictionary.divisor ( f ) )
                                let exceptionNames = exceptionNames2Divisor |> keySet
                                for n in exceptionNames do
                                    aExNames |> Array.filter ( fun ( _ , nn , _ ) -> nn = n ) |> updateExisting_market_data sourceName f ( exceptionNames2Divisor.Item n )
                            )



        counter <- counter + maxNamesInBloombergHistRequest

    logSw.Close()




let updateEquitiesCore ( errorSw : IO.StreamWriter )

    ( dataFieldDictionary : DataFieldDictionary ) startDateInt endDateInt ( input : BbDataInput ) ( equityInput : EquitySingleNameData ) ( globalInput : GlobalData ) ( forExConversions : YieldConversionRates ) equityUniverse =

    verifyGlobalInput globalInput
    
    let ccyPairs = 
    
        get_static_data DatabaseFields.currency equityUniverse 

            |> Map.find DatabaseFields.currency 
            |> List.map ( fun ( _ , c ) -> c ) 
            |> formCurrencyPairs equityInput.forexIndicator equityInput.baseCurrencyFxPairs 
       
    let todayInt = DateTime.Today |> obj2Date // stamp files with the date of the run

    updateMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".marketData.csv" ) dataFieldDictionary input.sourceName startDateInt endDateInt input.frequency equityUniverse equityInput.singleNameMarketDataFields 
        
    updateMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".fxData.csv" ) dataFieldDictionary input.sourceName startDateInt endDateInt input.frequency ccyPairs equityInput.forExDataField 

    let conversionRates = forExConversions.conversionRate |> List.unzip |> snd

    updateMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".conversionData.csv" ) dataFieldDictionary input.sourceName startDateInt endDateInt input.frequency conversionRates equityInput.conversionRateDataField

    let names2Aliases = globalInput.globalDataNamesGics |> Map.ofList

    updateGlobalMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".globalData.csv" ) input.sourceName startDateInt endDateInt input.frequency names2Aliases globalInput.globalDataFields dataFieldDictionary

    ()




let updateEquities ( dataFieldDictionary : DataFieldDictionary ) ( input : BbDataInput ) ( equityInput : EquitySingleNameData ) ( globalInput : GlobalData ) ( forExConversions : YieldConversionRates ) =

    let endDate = DateTime.Today
    let endDateInt = endDate |> obj2Date
    let startDateInt = endDate.AddDays( - float input.numberOfDays ) |> obj2Date

    let todayInt = DateTime.Today |> obj2Date

    let errorSw = new IO.StreamWriter ( input.logFilePath + todayInt.ToString() + ".errorLog.csv" )

    let equityUniverse = get_equity_universe endDate

    updateEquitiesCore errorSw dataFieldDictionary startDateInt endDateInt input equityInput globalInput forExConversions equityUniverse

    errorSw.Close()




let updateEquitiesAndIndexes ( dataFieldDictionary : DataFieldDictionary ) ( input : BbDataInput ) ( indexInput : EquityIndexData ) ( globalInput : GlobalData ) ( forExConversions : YieldConversionRates ) =

    let endDate = DateTime.Today
    let endDateInt = endDate |> obj2Date
    let startDateInt = endDate.AddDays( - float input.numberOfDays ) |> obj2Date

    let todayInt = DateTime.Today |> obj2Date

    let errorSw = new IO.StreamWriter ( input.logFilePath + todayInt.ToString() + ".errorLog.csv" )

    let equityUniverse = 
    
        get_index_members ( indexInput.indexUniverse |> set ) endDate

            |> Map.fold ( fun s _ v -> List.append v s ) List.empty

            |> set

            |> Set.toList

    updateEquitiesCore errorSw dataFieldDictionary startDateInt endDateInt input indexInput.underlyingDataFields globalInput forExConversions equityUniverse

    if indexInput.indexMarketDataFields.IsEmpty |> not then

        updateMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".indexData.csv" ) dataFieldDictionary input.sourceName startDateInt endDateInt input.frequency indexInput.indexUniverse indexInput.indexMarketDataFields 
                
    errorSw.Close()




let updateEquitiesHistorical ( dataFieldDictionary : DataFieldDictionary ) ( input : BbDataInput ) ( equityInput : EquitySingleNameData ) ( globalInput : GlobalData ) ( forExConversions : YieldConversionRates ) =

    let endDateInt = input.endDate
    let endDate = endDateInt |> date2Obj
    let startDateInt = endDate.AddDays( - float input.numberOfDays ) |> obj2Date

    let todayInt = DateTime.Today |> obj2Date

    let errorSw = new IO.StreamWriter ( input.logFilePath + todayInt.ToString() + ".errorLog.csv" )

    let equityUniverse = getUnion_equity_universe ()
    
    updateEquitiesCore errorSw dataFieldDictionary startDateInt endDateInt input equityInput globalInput forExConversions equityUniverse
    
    errorSw.Close()




let updateEquitiesAndIndexesHistorical ( dataFieldDictionary : DataFieldDictionary ) ( input : BbDataInput ) ( indexInput : EquityIndexData ) ( globalInput : GlobalData ) ( forExConversions : YieldConversionRates ) =

    let endDateInt = input.endDate
    let endDate = endDateInt |> date2Obj
    let startDateInt = endDate.AddDays( - float input.numberOfDays ) |> obj2Date

    let todayInt = DateTime.Today |> obj2Date

    let errorSw = new IO.StreamWriter ( input.logFilePath + todayInt.ToString() + ".errorLog.csv" )

    let equityUniverse = indexInput.indexUniverse |> set |> getForRange_index_members startDateInt endDateInt

    updateEquitiesCore errorSw dataFieldDictionary startDateInt endDateInt input indexInput.underlyingDataFields globalInput forExConversions equityUniverse

    if indexInput.indexMarketDataFields.IsEmpty |> not then

        updateMarketData errorSw ( input.logFilePath + todayInt.ToString() + ".indexData.csv" ) dataFieldDictionary input.sourceName startDateInt endDateInt input.frequency indexInput.indexUniverse indexInput.indexMarketDataFields 
    
    errorSw.Close()
