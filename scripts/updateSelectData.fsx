#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"




#load@".\settingsDataFieldDictionary.fsx"




open System
open SA2.MarketData.BloombergHistorical
open SA2.Common.Io
open SA2.Common.Dates
open SA2.Common.Data
open SA2.Common.Table_market_data
open SA2.Common.DbSchema
open SA2.Common.Dictionaries
open SA2.Common.Utils



let namesToUpdate =

    [

        "AUDUSD CURNCY" ;
        "CHFUSD CURNCY" ;
        "CZKUSD CURNCY" ;
        "DKKUSD CURNCY" ;
        "EURUSD CURNCY" ;
        "GBPUSD CURNCY" ;
        "HKDUSD CURNCY" ;
        "JPYUSD CURNCY" ;
        "NOKUSD CURNCY" ;
        "SEKUSD CURNCY" ;
        "SGDUSD CURNCY" ;
        "USD CURNCY" ;
        "CADUSD CURNCY" ;
        "TWDUSD CURNCY" ;
        "ISKUSD CURNCY" ;
        "CNYUSD CURNCY" ;
        "KZTUSD CURNCY" ;
        "NZDUSD CURNCY" ;
        "HUFUSD CURNCY" ;
        "INRUSD CURNCY" ;
        "MYRUSD CURNCY" ;
        "PLNUSD CURNCY" ;
        "THBUSD CURNCY" ;
        "ZARUSD CURNCY" ;
        "BRLUSD CURNCY" ;
        "ILSUSD CURNCY" ;
        "MXNUSD CURNCY" ;
        "RUBUSD CURNCY" ;
        "TRYUSD CURNCY" ;
        "AEDUSD CURNCY" ;
        "BHDUSD CURNCY" ;
        "CLPUSD CURNCY" ;
        "COPUSD CURNCY" ;
        "KRWUSD CURNCY" ;
        "KWDUSD CURNCY" ;
        "OMRUSD CURNCY" ;
        "QARUSD CURNCY" ;
        "SARUSD CURNCY" ;
        "IDRUSD CURNCY" ;

    ]



let getAndUploadMarketData ( logFile : string ) sourceName numberOfDates endDateInt frequency sa2Field ( dataFieldDictionary : DataFieldDictionary ) names = 

    let endDate = endDateInt |> date2Obj
    let startDateInt = endDate.AddDays( - float numberOfDates ) |> obj2Date

    printfn "getting data for range: %d %d" startDateInt endDateInt



    let checkFields requestedTickers returnedTickers field =

        let retTickerSet = set returnedTickers

        let notFound = 

            requestedTickers

                |> List.filter ( fun t -> retTickerSet.Contains t |> not )

        if notFound.Length <> 0 then

            notFound |> paste |> printfn  "\n\n\nNo %s data was returned for: \n%s" field


   

    let sw = new IO.StreamWriter ( logFile  )
    let logIt = logBBOutput sw

    let bbField = dataFieldDictionary.mapIt( sa2Field ) 

    let dateTickerValues  = 

            bloombergHistoricalRequest names bbField ( frequency.ToString() ) false ( startDateInt.ToString() ) ( endDateInt.ToString() )

                |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
                |> selectUniqueDataValues 
                |> logIt

    sw.Close()

    checkFields  names ( dateTickerValues |> Array.map ( fun ( _ , t , _ ) -> t ) ) bbField

    updateExisting_market_data sourceName sa2Field ( dataFieldDictionary.divisor ( sa2Field ) ) dateTickerValues




// execute

getAndUploadMarketData "../output/currencies.csv" "BB" 3000 20080101 DateFrequency.daily DatabaseFields.price SettingsDataFieldDictionary.dataFieldDictionary namesToUpdate
