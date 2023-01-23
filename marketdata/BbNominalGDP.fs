module SA2.MarketData.BbNominalGdp



open SA2.Common.DbSchema
open SA2.Common.Dictionaries
open SA2.Common.Utils
open SA2.Common.Table_market_data

open BloombergHistorical



type NominalGdpTickerType =
    | qoq = 0
    | level = 1
    | ytd = 2




let getBBData ( dataFieldDictionary : DataFieldDictionary ) tickers sa2Field freq startDate endDate =

    let fillNonTradingDays = false

    bloombergHistoricalRequest tickers ( dataFieldDictionary.mapIt( sa2Field ) ) ( freq.ToString() ) fillNonTradingDays ( startDate.ToString() ) ( endDate.ToString() )

            |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
    



let updateNominalGdp ( dataFieldDictionary : DataFieldDictionary ) ticker2Type freq sa2Field startDate endDate =
    
    let tickers = ticker2Type |> keySet |> Set.toList

    let ytdTickers =

        ticker2Type

            |> Map.toList
            |> Seq.groupBy ( fun ( _ , tp ) -> tp ) 
            |> Seq.filter ( fun ( tp , _ ) -> tp = NominalGdpTickerType.ytd )
            |> Seq.map ( fun ( _ , s ) -> s |> Seq.map ( fun ( t , _ ) -> t ) )
            |> Seq.concat

    if Seq.isEmpty ytdTickers |> not then

        printfn "WARNING in updateNominalGdp: YTD levels not supported. these tickers will be ignored:\n%s" ( paste ytdTickers )


    let gdpData = getBBData dataFieldDictionary tickers sa2Field freq startDate endDate       
    
    let qoqFromGdpData = gdpData |> Array.filter ( fun ( _ , t , _ ) -> Map.find t ticker2Type = NominalGdpTickerType.qoq )

    let qoqFromLevels =

        gdpData
        
            |> Seq.groupBy ( fun ( _ , t , _ ) -> t )
            |> Seq.toArray
            |> Array.filter ( fun ( t , _ ) -> Map.find t ticker2Type = NominalGdpTickerType.level )
            |> Array.map ( fun ( t , s ) -> 
                                s 
                                    |> Seq.sortBy ( fun ( d , _ , _ ) -> d )
                                    |> ( Seq.pairwise >> Seq.toArray ) 
                                    |> Array.map ( fun ( ( _ , _ , v0 ) , ( d , _ , v1 ) ) -> ( d , t , v1 / v0 - 1.0 ) )
                                )
            |> Array.concat

    
    // why 100 and the other one 1? why not just do the divisions here and make it all one? just trying the minimize the chance of a screw-up if an update happens outside the code

    qoqFromGdpData |> updateExisting_market_data "BB" DatabaseFields.price 100.0
    qoqFromLevels |> updateExisting_market_data "BB" DatabaseFields.price 1.0
