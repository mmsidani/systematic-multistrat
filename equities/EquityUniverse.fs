module SA2.Equity.EquityUniverse

open System

open SA2.Common.Dates
open SA2.Common.DbSchema
open SA2.Common.Utils
open SA2.Common.Table_equity_universe
open SA2.Common.Table_static_data
open SA2.Common.Table_market_data
open SA2.Common.Table_index_members
open SA2.Common.ForEx
open SA2.Common.FxTypes
open SA2.Common.Dictionaries

open SA2.MarketData.BloombergHistorical



type EquityUniverseInput = {

    baseCurrency : string ;

    // which indexes?
    indexes : string list ;

    equityClassIndicator : string ;

    marketCapField : string ;

    gicsField : string ;

    currencyField : string ;

    shortNameField : string ;

    fundamentalTickerField : string ;

    icbField : string ;
    
    numberToKeep : int ;

    sourceName : string

}



type IndexMembersInput = {

    equityUniverseInput : EquityUniverseInput ;

    indexes : string list ;
    
    indexMembersField : string ; // which bloomberg field

    logFilePath : string

}


type EquityUniverseHistoricalInput = {

    equityUniverseInput : EquityUniverseInput

    startDate : int

    endDate : int 

    frequency : DateFrequency 

} 



type IndexMembersHistoricalInput = {

    indexMembersInput : IndexMembersInput ;

    startDate : int

    endDate : int 

    frequency : DateFrequency 

}




type UniverseSelection =

    | byMarketCap = 0




let loadEquityUniverse ( eqU : EquityUniverseInput ) ( fieldDictionary : StaticDataFieldDictionary ) criterion universe =

    let dates , stocks  = universe |> Array.unzip
        
    let makeUnique x =

        // Note: make the second component unique. in case one name belongs to more than one index

        x |> Seq.groupBy ( fun ( _ , s ) -> s ) |> Seq.map ( fun ( s , sOs ) -> ( Seq.nth 0 sOs |> fst , s ) ) |> List.ofSeq 

    ( dates , stocks ) ||> Seq.zip |> makeUnique |> updateExisting_equity_universe ( criterion.ToString() )




let private updateEquities dates ( eqU : EquityUniverseInput ) ( forex : ForexDataFields ) =

    let baseCurrency = eqU.baseCurrency

    let indexes = eqU.indexes
    
    [| 
       
        for date in dates ->
            printfn "now doing %d" date
            let name2Index = 
                get_index_members ( indexes |> set ) ( date |> date2Obj )
                    |> Map.toArray |> Array.unzip |> ( fun ( l , r ) -> ( r , l ) ) ||> Array.map2 ( fun l i -> l |> List.map ( fun n -> ( n , i ) ) ) |> List.concat |> Map.ofList

            let singleNames = name2Index |> keySet |> Set.toList
            
            let allInstruments = List.append singleNames indexes
            
            let singleNameMarketCap = getForDate_market_data 1 ( date |> date2Obj) DatabaseFields.marketCap singleNames // 1 record, the latest data available before that date: TODO a very old data point keeps being picked
                                                |> Map.find DatabaseFields.marketCap
                                                |> Map.map ( fun i l -> List.head l |> snd )
            
            let currencies = get_static_data DatabaseFields.currency allInstruments |> Map.find DatabaseFields.currency |> Map.ofList
            
            let singleNameGics = get_static_data DatabaseFields.gicsIndustry allInstruments |> Map.find DatabaseFields.gicsIndustry |> Map.ofList
            
            let uniqueCurrencies = currencies |> Map.fold ( fun s _ c -> Set.add c s ) Set.empty  |> Set.toList
            let currencyPairs , currencyDivisors = formCurrencyPairsDivisors baseCurrency uniqueCurrencies // something like "USDUSD" will not be returned here
            
            let bbCurrencyPairs = currencyPairs |> valueSet |> Set.toList |> List.map ( fun p -> p + " " + forex.forexIndicator ) 
            
            let forexRates = getForDate_market_data 1 ( date |> date2Obj) DatabaseFields.price bbCurrencyPairs
                                        |> Map.find DatabaseFields.price
                                        |> Map.map ( fun _ l -> List.head l |> snd )
            
            let marketCaps = 
            
                singleNameMarketCap 

                    |> Map.filter ( fun n _ -> 

                                        let index = Map.find n name2Index

                                        Map.containsKey n currencies && Map.containsKey n singleNameGics  && Map.containsKey index currencies
                                    )

                    |> Map.map (
                                                         
                                fun k v -> 

                                    let ccy = Map.find k currencies
                                    let ccyPair = Map.find ccy currencyPairs + " " + forex.forexIndicator                                                                  
                                    float ( Map.find ccyPair forexRates ) / ( Map.find ccy currencyDivisors ) * float ( v )
                                )

                    |> Map.toArray
                    |> Array.sortBy ( fun ( _ , v ) -> v )
                    |> Array.rev
               
            marketCaps.[ 0 .. min ( marketCaps.Length - 1) ( eqU.numberToKeep - 1 ) ]

                |> Array.map 

                        ( 
                
                        fun ( n , cap ) -> 
                            
                            let index = Map.find n name2Index 
                                
                            ( date , n ) 
                        
                        )


    |]

    |> Array.concat




let updateEquityUniverse ( input : EquityUniverseInput ) ( forex : ForexDataFields ) ( fieldDictionary : StaticDataFieldDictionary ) = 
 
    let dates = [ DateTime.Today |> obj2Date ]

                    |> List.map ( fun d -> date2Obj d )
                    |> shiftWeekends
                    |> Seq.map ( fun d -> obj2Date d )

    updateEquities dates input forex

        |> loadEquityUniverse input fieldDictionary UniverseSelection.byMarketCap
 



let updateEquityUniverseHistorical ( input : EquityUniverseHistoricalInput ) ( forex : ForexDataFields ) ( fieldDictionary : StaticDataFieldDictionary ) = 

    let dates = datesAtFrequency input.frequency input.startDate input.endDate

                    |> Array.map ( fun d -> date2Obj d )
                    |> Array.toList
                    |> shiftWeekends
                    |> Seq.map ( fun d -> obj2Date d )

    updateEquities dates input.equityUniverseInput forex

        |> loadEquityUniverse input.equityUniverseInput fieldDictionary UniverseSelection.byMarketCap
