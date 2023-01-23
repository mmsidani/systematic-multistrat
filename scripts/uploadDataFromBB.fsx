#r@"..\sa2-dlls\common.dll"
#r@"..\sa2-dlls\marketData.dll"


#load@"./settingsDataFieldDictionary.fsx"


open System


open SA2.Common.DbSchema
open SA2.Common.Table_market_data
open SA2.Common.Table_static_data
open SA2.Common.Table_instruments
open SA2.Common.Table_data_fields
open SA2.Common.Io
open SA2.Common.Data
open SA2.Common.Utils

open SA2.MarketData.BloombergHistorical
open SA2.MarketData.BloombergReference





let getTickersFromList ( ) =

    // use this if we don't want to specify the tickers in a separate file


    [
        // put tickers here

    ]




let getTickersFromFile tickersFileName =

    // file should have one ticker per line

    readFile tickersFileName

        |> Seq.toList






let uploadDataFromBB bbFieldName sa2FieldName divisor frequency startDate endDate insts =

    bloombergHistoricalRequest insts bbFieldName frequency false ( startDate.ToString() ) ( endDate.ToString() )

        |> Array.map ( fun ( d , t , v ) -> ( d , t , float v ) )
        |> selectUniqueDataValues // Note: the effect of setting fillNonTradingDays to true will be reversed here. 
        |> updateExisting_market_data "BB" sa2FieldName divisor




// execute but set input fields first




printfn "%s" ( getMapping_data_fields () |> paste ) // run this to get the mapping of sa2 fields to bb fields 




let sa2FieldName = DatabaseFields.ebit
let bbField = "EBIT"
let divisor = 1.0

let frequency = "daily"
let startDate = 19800101 
let endDate = 20140301


//"//Terra/Users/Majed/devel/InOut/Rate-Names-Bloomberg.csv" |> getTickersFromFile 
//
///// getTickersFromList () 
//
//    |> uploadDataFromBB bbField sa2FieldName divisor frequency startDate endDate

